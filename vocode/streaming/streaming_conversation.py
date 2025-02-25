from __future__ import annotations

import asyncio
import queue
import random
import threading
from typing import Any, Awaitable, Callable, Generic, Optional, Tuple, TypeVar, cast
import logging
import time
import typing

from vocode.streaming.action.worker import ActionsWorker

from vocode.streaming.agent.bot_sentiment_analyser import (
    BotSentimentAnalyser,
)
from vocode.streaming.agent.chat_gpt_agent import ChatGPTAgent
from vocode.streaming.models.actions import ActionInput
from vocode.streaming.models.events import Sender
from vocode.streaming.models.transcript import (
    Message,
    Transcript,
    TranscriptCompleteEvent,
)
from vocode.streaming.models.message import BaseMessage
from vocode.streaming.models.transcriber import EndpointingConfig, TranscriberConfig
from vocode.streaming.output_device.base_output_device import BaseOutputDevice
from vocode.streaming.utils.conversation_logger_adapter import wrap_logger
from vocode.streaming.utils.events_manager import EventsManager
from vocode.streaming.utils.goodbye_model import GoodbyeModel

from vocode.streaming.models.agent import ChatGPTAgentConfig, FillerAudioConfig
from vocode.streaming.models.audio_encoding import AudioEncoding
from vocode.streaming.models.synthesizer import (
    SentimentConfig,
    ElevenLabsSynthesizerConfig,
)
from vocode.streaming.constants import (
    TEXT_TO_SPEECH_CHUNK_SIZE_SECONDS,
    PER_CHUNK_ALLOWANCE_SECONDS,
    ALLOWED_IDLE_TIME,
)
from vocode.streaming.agent.base_agent import (
    AgentInput,
    AgentResponse,
    AgentResponseFillerAudio,
    AgentResponseFollowUpAudio,
    AgentResponseBacktrackAudio,
    AgentResponseMessage,
    AgentResponseStop,
    AgentResponseType,
    BaseAgent,
    TranscriptionAgentInput,
)
from vocode.streaming.synthesizer.base_synthesizer import (
    BaseSynthesizer,
    SynthesisResult,
    FillerAudio,
)
from vocode.streaming.utils import (
    create_conversation_id, 
    get_chunk_size_per_second,
    convert_wav
)
from vocode.streaming.transcriber.base_transcriber import (
    Transcription,
    BaseTranscriber,
    HUMAN_ACTIVITY_DETECTED
)
from vocode.streaming.utils.state_manager import ConversationStateManager
from vocode.streaming.utils.worker import (
    AsyncQueueWorker,
    InterruptibleAgentResponseWorker,
    InterruptibleEvent,
    InterruptibleEventFactory,
    InterruptibleAgentResponseEvent,
    InterruptibleWorker,
)
from vocode.streaming.utils.duration_from_message import should_finish_sentence
from vocode.streaming.response_worker.random_response import RandomAudioManager

from opentelemetry import trace, metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider, Span
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)

OutputDeviceType = TypeVar("OutputDeviceType", bound=BaseOutputDevice)


class StreamingConversation(Generic[OutputDeviceType]):
    class QueueingInterruptibleEventFactory(InterruptibleEventFactory):
        def __init__(self, conversation: "StreamingConversation"):
            self.conversation = conversation

        def create_interruptible_event(
            self, payload: Any, is_interruptible: bool = True
        ) -> InterruptibleEvent[Any]:
            interruptible_event: InterruptibleEvent = (
                super().create_interruptible_event(payload, is_interruptible)
            )
            self.conversation.interruptible_events.put_nowait(interruptible_event)
            return interruptible_event

        def create_interruptible_agent_response_event(
            self,
            payload: Any,
            is_interruptible: bool = True,
            agent_response_tracker: Optional[asyncio.Event] = None,
        ) -> InterruptibleAgentResponseEvent:
            interruptible_event = super().create_interruptible_agent_response_event(
                payload,
                is_interruptible=is_interruptible,
                agent_response_tracker=agent_response_tracker,
            )
            self.conversation.interruptible_events.put_nowait(interruptible_event)
            return interruptible_event

    class TranscriptionsWorker(AsyncQueueWorker):
        """Processes all transcriptions: sends an interrupt if needed
        and sends final transcriptions to the output queue (which is the input for the agent)"""

        def __init__(
            self,
            input_queue: asyncio.Queue[Transcription],
            output_queue: asyncio.Queue[InterruptibleEvent[AgentInput]],
            conversation: "StreamingConversation",
            interruptible_event_factory: InterruptibleEventFactory,
        ):
            super().__init__(input_queue, output_queue)
            self.input_queue = input_queue
            self.output_queue = output_queue
            self.conversation = conversation
            self.interruptible_event_factory = interruptible_event_factory

        async def process(self, transcription: Transcription):
            self.conversation.mark_last_action_timestamp()
            self.conversation.current_transcription_is_interrupt = transcription.is_interrupt
            if transcription.message.strip() == "":
                self.conversation.logger.info("Ignoring empty transcription")
                return
            
            self.conversation.random_audio_manager.sync_stop_follow_up_audio()
            if transcription.message == HUMAN_ACTIVITY_DETECTED:
                self.conversation.logger.info("Got transcription: Human activity detected")
            if not transcription.is_final:
                self.conversation.logger.debug("Got partial transcription: {}".format(transcription.message))
            if transcription.is_final:
                if not self.conversation.human_has_spoken:
                    self.conversation.human_has_spoken = True
                self.conversation.logger.debug(
                    "Got transcription: {}, confidence: {}".format(
                        transcription.message, transcription.confidence
                    )
                )

            # check interruption, if not checking interruption, transcription will always be considered 
            should_check_interrupt = (
                self.conversation.is_bot_speaking 
                # or not self.conversation.is_human_speaking # commented out because bot keeps ignoring small utterances even when there is silence
                or self.conversation.is_synthesizing
                or not self.conversation.sent_initial_message
            )
            # self.conversation.logger.debug(f"is_bot_speaking: {self.conversation.is_bot_speaking}")
            # self.conversation.logger.debug(f"is_synthesizing: {self.conversation.is_synthesizing}")
            if should_check_interrupt:
                self.conversation.logger.debug(f"Checking interrupt for {transcription.message}")
                if self.conversation.is_interrupt(transcription):
                    self.conversation.logger.debug("Conversation interrupted")
                    self.conversation.current_transcription_is_interrupt = (
                        self.conversation.broadcast_interrupt()
                    )
                    # if self.conversation.current_transcription_is_interrupt:
                    #     self.conversation.logger.debug("Sending interrupt...")
                    self.conversation.logger.debug("Human started speaking")
                    self.conversation.logger.debug("Sending Backtrack audio to AgentResponseWorker.")
                    self.conversation.agent.produce_interruptible_agent_response_event_nonblocking(
                        AgentResponseBacktrackAudio()
                    )
                else:    
                    self.conversation.logger.debug(f"Ignoring human utterance - text didn't trigger interruption: {transcription.message}")
                    return
            # ignore low confidence transcriptions if bot is speaking
            if transcription.confidence < self.conversation.transcriber.get_transcriber_config().min_interrupt_confidence:
                if should_check_interrupt:
                    self.conversation.logger.info(
                        f"Ignoring low confidence transcription: {transcription.message}, {transcription.confidence}"
                    )
                    return

            transcription.is_interrupt = (
                self.conversation.current_transcription_is_interrupt
            )
            self.conversation.is_human_speaking = not transcription.is_final
            if transcription.is_final:
                
                detected_human_voice = (
                    transcription.message == HUMAN_ACTIVITY_DETECTED
                )
                if detected_human_voice:
                    return
                # we use getattr here to avoid the dependency cycle between VonageCall and StreamingConversation
                event = self.interruptible_event_factory.create_interruptible_event(
                    TranscriptionAgentInput(
                        transcription=transcription,
                        conversation_id=self.conversation.id,
                        vonage_uuid=getattr(self.conversation, "vonage_uuid", None),
                        twilio_sid=getattr(self.conversation, "twilio_sid", None),
                    )
                )
                self.output_queue.put_nowait(event)
    class AgentResponsesWorker(InterruptibleAgentResponseWorker):
        """Runs Synthesizer.create_speech and sends the SynthesisResult to the output queue"""

        def __init__(
            self,
            input_queue: asyncio.Queue[InterruptibleAgentResponseEvent[AgentResponse]],
            output_queue: asyncio.Queue[
                InterruptibleAgentResponseEvent[Tuple[BaseMessage, SynthesisResult]]
            ],
            conversation: "StreamingConversation",
            interruptible_event_factory: InterruptibleEventFactory,
        ):
            super().__init__(
                input_queue=input_queue,
                output_queue=output_queue,
            )
            self.input_queue: asyncio.Queue[InterruptibleAgentResponseEvent[AgentResponse]]  = input_queue
            self.output_queue = output_queue
            self.conversation = conversation
            self.interruptible_event_factory = interruptible_event_factory
            self.chunk_size = (
                get_chunk_size_per_second(
                    self.conversation.synthesizer.get_synthesizer_config().audio_encoding,
                    self.conversation.synthesizer.get_synthesizer_config().sampling_rate,
                )
                * TEXT_TO_SPEECH_CHUNK_SIZE_SECONDS
            )
            self.use_index: bool = bool(getattr(self.conversation.synthesizer.get_synthesizer_config(),
                                     'index_config',
                                      None)
                                     )
        def input_queue_has_agent_response_message(self):
            for item in self.input_queue._queue:
                if isinstance(item.payload, AgentResponseMessage):
                    return True
            return False
            
        async def process(self, item: InterruptibleAgentResponseEvent[AgentResponse]):
            if not self.conversation.synthesis_enabled:
                self.conversation.logger.debug(
                    "Synthesis disabled, not synthesizing speech"
                )
                return
            try:
                agent_response = item.payload
                self.conversation.logger.debug("Got agent response: {}".format(agent_response))
                if isinstance(agent_response, AgentResponseFillerAudio):
                    if not (self.conversation.human_messages_in_transcript > self.conversation.min_human_messages_in_transcript):
                        self.conversation.human_messages_in_transcript = self.conversation.transcript.count_human_messages()
                    should_send_filler_audio = (  
                        self.conversation.bot_has_spoken
                        and (self.conversation.human_messages_in_transcript > self.conversation.min_human_messages_in_transcript)                
                        and not self.conversation.is_bot_speaking
                        and self.conversation.synthesis_results_queue.empty()
                    )
                    if should_send_filler_audio:
                        self.conversation.logger.debug("Sending filler audio in AgentResponsesWorker")
                        self.conversation.random_audio_manager.sync_send_filler_audio(item.agent_response_tracker)
                    return
                
                if isinstance(agent_response, AgentResponseFollowUpAudio):
                    self.conversation.random_audio_manager.sync_send_follow_up_audio(item.agent_response_tracker)
                    return
                if isinstance(agent_response, AgentResponseBacktrackAudio):
                    self.conversation.logger.debug("Waiting for bot to stop speaking after interruption")
                    bot_finished_speaking_event: InterruptibleAgentResponseEvent = self.conversation.synthesis_results_worker.interruptible_event
                    await bot_finished_speaking_event.agent_response_tracker.wait()
                    should_send_backtrack_audio = (
                        (self.conversation.human_messages_in_transcript > self.conversation.min_human_messages_in_transcript)
                        and self.conversation.bot_has_spoken
                        and self.conversation.is_human_speaking
                    )                    
                                        
                    if should_send_backtrack_audio:
                        self.conversation.logger.debug("Sending backtrack audio in AgentResponsesWorker")
                        self.conversation.random_audio_manager.sync_send_backtrack_audio(asyncio.Event())
                    return

                if isinstance(agent_response, AgentResponseStop):
                    self.conversation.logger.debug("Agent requested to stop")
                    item.agent_response_tracker.set()
                    await self.conversation.terminate()
                    return
                               
                agent_response_message = typing.cast(
                    AgentResponseMessage, agent_response
                )

                self.conversation.first_synthesis_span = tracer.start_span(
                    "conversation.synthesizer.create_first_speech"
                )
                self.conversation.first_chunk_flag = True

                self.conversation.logger.debug("Synthesizing speech for message")
                self.conversation.is_synthesizing = True
                synthesis_results = await self.conversation.synthesizer.create_speech(
                    agent_response_message.message,
                    self.chunk_size,
                    bot_sentiment=self.conversation.bot_sentiment,
                    return_tuple=self.use_index
                )
                if self.use_index:
                    synthesis_result, message = synthesis_results
                else:
                    synthesis_result = synthesis_results
                    message = agent_response_message.message
                # check if there is more to synthesize 
                self.conversation.is_synthesizing = self.input_queue_has_agent_response_message()
                self.produce_interruptible_agent_response_event_nonblocking(
                    (message, synthesis_result),
                    is_interruptible=item.is_interruptible,
                    agent_response_tracker=item.agent_response_tracker,
                )
            except asyncio.CancelledError:
                pass

    class SynthesisResultsWorker(InterruptibleAgentResponseWorker):
        """Plays SynthesisResults from the output queue on the output device"""

        def __init__(
            self,
            input_queue: asyncio.Queue[
                InterruptibleAgentResponseEvent[Tuple[BaseMessage, SynthesisResult]]
            ],
            conversation: "StreamingConversation",
        ):
            super().__init__(input_queue=input_queue)
            self.input_queue = input_queue
            self.conversation = conversation

        async def process(
            self,
            item: InterruptibleAgentResponseEvent[Tuple[BaseMessage, SynthesisResult]],
        ):
            try:
                self.conversation.random_audio_manager.stop_all_audios()
                message, synthesis_result = item.payload
                # create an empty transcript message and attach it to the transcript
                transcript_message = Message(
                    text="",
                    sender=Sender.BOT,
                )
                self.conversation.transcript.add_message(
                    message=transcript_message,
                    conversation_id=self.conversation.id,
                    publish_to_events_manager=False,
                )

                # add a customizable delay to the initial message
                if (
                    not self.conversation.sent_initial_message
                    and not self.conversation.human_has_spoken
                ):
                    initial_delay = self.conversation.agent.get_agent_config().initial_message_delay_seconds
                    if initial_delay:
                        elapsed_time = time.time() - self.conversation.call_start_time
                        remaining_time = initial_delay - elapsed_time
                        # if the remaining time is positive, delay the initial message by that amount of time
                        if remaining_time > 0:
                            self.conversation.logger.debug(f"Delaying initial message by {remaining_time:.2f} seconds")
                            await asyncio.sleep(remaining_time)

                # set flag for bot interruption
                self.conversation.is_interrupted = False
                
                # set flag for bot speaking
                self.conversation.is_bot_speaking = True

                message_sent, cut_off, seconds_spoken = await self.conversation.send_speech_to_output(
                    message.text,
                    synthesis_result,
                    item.interruption_event,
                    TEXT_TO_SPEECH_CHUNK_SIZE_SECONDS,
                    transcript_message=transcript_message,
                )
                # set flag to indicate whether bot was interrupted
                self.conversation.is_interrupted = cut_off
                # set flag to check if there is more to say
                self.conversation.is_bot_speaking = (
                    not self.input_queue.empty()
                    and not self.conversation.is_interrupted
                )
                if not self.conversation.bot_has_spoken:
                    if (not cut_off) or (len(message_sent) > 5):
                        self.conversation.bot_has_spoken = True
                # publish the transcript message now that it includes what was said during send_speech_to_output
                self.conversation.transcript.maybe_publish_transcript_event_from_message(
                    message=transcript_message,
                    conversation_id=self.conversation.id,
                )
                item.agent_response_tracker.set()
                self.conversation.logger.debug("Message sent: {}".format(message_sent))
                if cut_off:
                    self.conversation.agent.update_last_bot_message_on_cut_off(
                        message_sent
                    )
                if self.conversation.agent.agent_config.end_conversation_on_goodbye:
                    goodbye_detected_task = (
                        self.conversation.agent.create_goodbye_detection_task(
                            message_sent
                        )
                    )
                    try:
                        if await asyncio.wait_for(goodbye_detected_task, 0.1):
                            self.conversation.logger.debug(
                                "Agent said goodbye, ending call"
                            )
                            await asyncio.sleep(2)  # Wait for 2 seconds
                            await self.conversation.terminate()
                    except asyncio.TimeoutError:
                        pass
                
                should_send_follow_up = (
                    self.conversation.human_has_spoken and
                    not self.conversation.is_bot_speaking and 
                    not self.conversation.is_synthesizing and
                    self.conversation.agent.get_agent_config().send_follow_up_audio
                )
                if should_send_follow_up:
                    self.conversation.logger.debug("Sending Follow Up to AgentResponseWorker.")
                    self.conversation.agent.produce_interruptible_agent_response_event_nonblocking(
                        AgentResponseFollowUpAudio())
            except asyncio.CancelledError:
                pass

    def __init__(
        self,
        output_device: OutputDeviceType,
        transcriber: BaseTranscriber[TranscriberConfig],
        agent: BaseAgent,
        synthesizer: BaseSynthesizer,
        conversation_id: Optional[str] = None,
        per_chunk_allowance_seconds: float = PER_CHUNK_ALLOWANCE_SECONDS,
        events_manager: Optional[EventsManager] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.last_action_timestamp = None
        self.id = conversation_id or create_conversation_id()
        self.logger = wrap_logger(
            logger or logging.getLogger(__name__),
            conversation_id=self.id,
        )
        self.output_device = output_device
        self.transcriber = transcriber
        self.agent = agent
        self.synthesizer = synthesizer
        self.synthesis_enabled = True

        self.interruptible_events: queue.Queue[InterruptibleEvent] = queue.Queue()
        self.interruptible_event_factory = self.QueueingInterruptibleEventFactory(
            conversation=self
        )
        self.agent.set_interruptible_event_factory(self.interruptible_event_factory)
        self.synthesis_results_queue: asyncio.Queue[
            InterruptibleAgentResponseEvent[Tuple[BaseMessage, SynthesisResult]]
        ] = asyncio.Queue()

        self.state_manager = self.create_state_manager()
        self.transcriptions_worker = self.TranscriptionsWorker(
            input_queue=self.transcriber.output_queue,
            output_queue=self.agent.get_input_queue(),
            conversation=self,
            interruptible_event_factory=self.interruptible_event_factory,
        )
        self.agent.attach_conversation_state_manager(self.state_manager)
        self.agent_responses_worker = self.AgentResponsesWorker(
            input_queue=self.agent.get_output_queue(),
            output_queue=self.synthesis_results_queue,
            conversation=self,
            interruptible_event_factory=self.interruptible_event_factory,
        )
        self.random_audio_manager: RandomAudioManager = RandomAudioManager(conversation=self)
        self.actions_worker = None
        if self.agent.get_agent_config().actions:
            self.actions_worker = ActionsWorker(
                input_queue=self.agent.actions_queue,
                output_queue=self.agent.get_input_queue(),
                interruptible_event_factory=self.interruptible_event_factory,
                action_factory=self.agent.action_factory,
            )
            self.actions_worker.attach_conversation_state_manager(self.state_manager)
        self.synthesis_results_worker = self.SynthesisResultsWorker(
            input_queue=self.synthesis_results_queue, conversation=self
        )
        
        self.events_manager = events_manager or EventsManager()
        self.events_task: Optional[asyncio.Task] = None
        self.per_chunk_allowance_seconds = per_chunk_allowance_seconds
        self.transcript = Transcript()
        self.transcript.attach_events_manager(self.events_manager)
        self.bot_sentiment = self.synthesizer.get_synthesizer_config().initial_bot_sentiment
        if self.agent.get_agent_config().track_bot_sentiment:
            self.sentiment_config = (
                self.synthesizer.get_synthesizer_config().sentiment_config
            )
            if not self.sentiment_config:
                self.sentiment_config = SentimentConfig()
            self.bot_sentiment_analyser = BotSentimentAnalyser(
                emotions=self.sentiment_config.emotions
            )

        # set up flags to better handle interruptions
        self.is_human_speaking = False
        self.human_has_spoken = False
        self.human_messages_in_transcript = 0
        self.min_human_messages_in_transcript = 2
        self.is_bot_speaking = False
        self.bot_has_spoken = False
        self.sent_initial_message = False
        self.is_interrupted = False
        self.is_synthesizing = False
        self.active = False
        self.call_start_time: Optional[float] = None
        self.mark_last_action_timestamp()

        self.check_for_idle_task: Optional[asyncio.Task] = None
        self.track_bot_sentiment_task: Optional[asyncio.Task] = None
        self.initial_message_task: Optional[asyncio.Task] = None

        self.current_transcription_is_interrupt: bool = False

        # tracing
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.first_synthesis_span: Optional[Span] = None

    def create_state_manager(self) -> ConversationStateManager:
        return ConversationStateManager(conversation=self)

    async def start(
            self, 
            started_event: Optional[asyncio.Event] = None,
            mark_ready: Optional[Callable[[], Awaitable[None]]] = None
        ):
        self.call_start_time = time.time()
        self.transcriber.start()
        self.transcriptions_worker.start()
        self.agent_responses_worker.start()
        self.synthesis_results_worker.start()
        self.output_device.start()
        await self.random_audio_manager.start()
        if self.actions_worker is not None:
            self.actions_worker.start()
        is_ready = await self.transcriber.ready()
        if not is_ready:
            raise Exception("Transcriber startup failed")


        self.agent.start()
        self.agent.attach_transcript(self.transcript)
        initial_message = self.agent.get_agent_config().initial_message
        if initial_message:
            self.initial_message_task = asyncio.create_task(self.send_initial_message(initial_message))
        self.active = True
        if started_event:
            started_event.set()
        if mark_ready:
            await mark_ready()
        # bluberry modification: added self.agent.get_agent_config().track_bot_sentiment 
        # to the following two if statements
        if (
            self.synthesizer.get_synthesizer_config().sentiment_config 
            and self.agent.get_agent_config().track_bot_sentiment
        ):
            await self.update_bot_sentiment()
       
        if (self.synthesizer.get_synthesizer_config().sentiment_config 
                and self.agent.get_agent_config().track_bot_sentiment):
            self.track_bot_sentiment_task = asyncio.create_task(
                self.track_bot_sentiment()
            )
        self.check_for_idle_task = asyncio.create_task(self.check_for_idle())
        if len(self.events_manager.subscriptions) > 0:
            self.events_task = asyncio.create_task(self.events_manager.start())

    async def send_initial_message(self, initial_message: BaseMessage):
        try:
            if not self.human_has_spoken:
                initial_message_tracker = asyncio.Event()
                agent_response_event = (
                    self.interruptible_event_factory.create_interruptible_agent_response_event(
                        AgentResponseMessage(message=initial_message),
                        is_interruptible=self.agent.get_agent_config().interrupt_initial_message,
                        agent_response_tracker=initial_message_tracker,
                    )
                )
                self.agent_responses_worker.consume_nonblocking(agent_response_event)
                self.sent_initial_message = await initial_message_tracker.wait()
        except asyncio.CancelledError:
            self.logger.debug("Initial message task cancelled")
            self.sent_initial_message = True
            return

    async def check_for_idle(self):
        """Terminates the conversation after allowed_idle_time_seconds seconds if no activity is detected"""
        while self.is_active():
            if time.time() - self.last_action_timestamp > (
                self.agent.get_agent_config().allowed_idle_time_seconds
                or ALLOWED_IDLE_TIME
            ):
                self.logger.debug("Conversation idle for too long, terminating")
                await self.terminate()
                return
            await asyncio.sleep(15)

    async def track_bot_sentiment(self):
        """Updates self.bot_sentiment every second based on the current transcript"""
        prev_transcript = None
        while self.is_active():
            await asyncio.sleep(1)
            if self.transcript.to_string() != prev_transcript:
                await self.update_bot_sentiment()
                prev_transcript = self.transcript.to_string()

    async def update_bot_sentiment(self):
        new_bot_sentiment = await self.bot_sentiment_analyser.analyse(
            self.transcript.to_string()
        )
        if new_bot_sentiment.emotion:
            self.logger.debug("Bot sentiment: %s", new_bot_sentiment)
            self.bot_sentiment = new_bot_sentiment

    def receive_message(self, message: str):
        transcription = Transcription(
            message=message,
            confidence=1.0,
            is_final=True,
        )
        self.transcriptions_worker.consume_nonblocking(transcription)

    def receive_audio(self, chunk: bytes):
        self.transcriber.send_audio(chunk)

    def warmup_synthesizer(self):
        self.synthesizer.ready_synthesizer()

    def mark_last_action_timestamp(self):
        self.last_action_timestamp = time.time()

    def broadcast_interrupt(self):
        """Stops all inflight events and cancels all workers that are sending output

        Returns true if any events were interrupted - which is used as a flag for the agent (is_interrupt)
        """
        num_interrupts = 0
        while True:
            try:
                interruptible_event = self.interruptible_events.get_nowait()
                if not interruptible_event.is_interrupted():
                    if interruptible_event.interrupt():
                        self.logger.debug("Interrupting event")
                        num_interrupts += 1
            except queue.Empty:
                break
        self.agent.cancel_current_task()
        self.agent_responses_worker.cancel_current_task()
        self.random_audio_manager.stop_all_audios()
        # self.is_bot_speaking = False
        self.is_synthesizing = False

        self.logger.info(f"Broadcasting interrupt. Cancelled {num_interrupts} interruptible events.")
        # Clearing these queues cuts time from finishing interruption talking to bot talking cut by 1 second
        self.clear_queue(self.agent.output_queue, 'agent.output_queue')
        self.clear_queue(self.agent_responses_worker.output_queue, 'agent_responses_worker.output_queue')
        self.clear_queue(self.agent_responses_worker.input_queue, 'agent_responses_worker.input_queue')
        self.clear_queue(self.output_device.queue, 'output_device.queue')
        
        
        return num_interrupts > 0

    def is_interrupt(self, transcription: Transcription) -> bool:  
        detected_human_voice = (
            transcription.message == HUMAN_ACTIVITY_DETECTED
        )   
        if detected_human_voice:
            return True   
        # guarantee minimum confidence in transcription
        if transcription.confidence >= (
            self.transcriber.get_transcriber_config().min_interrupt_confidence or 0
        ):
            message = transcription.message.lower().strip()
            words = message.split()
            interruption_threshold = self.transcriber.get_transcriber_config().interruption_word_threshold
            # Verbal cues that indicate no interruption
            verbal_cues = ["uh", "um", "mhm", 
                           "yes", "yeah", "okay", 
                           "i see", "i understand", "go on", "go ahead"]

            if len(words)==0 or len(words)==1:
                # No interruption for no words or one word uttered
                return False
              
            if any(cue in message for cue in verbal_cues) and (len(words) <= interruption_threshold):
                # No interruption for positive verbal cues in short utterances
                return False

            # Check for interruptions with more than two words
            if len(words) > interruption_threshold:
                return True
            # TODO: implement logic for active listening cues
            return True
        else:
            return False

    @staticmethod
    def clear_queue(q: asyncio.Queue, queue_name: str):
        while not q.empty():
            logging.debug(f'Clearing queue {queue_name} with size {q.qsize()}')
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                continue

    async def send_speech_to_output(
        self,
        message: str,
        synthesis_result: SynthesisResult,
        stop_event: threading.Event,
        seconds_per_chunk: int,
        transcript_message: Optional[Message] = None,
        started_event: Optional[threading.Event] = None,
    ):
        """
        - Sends the speech chunk by chunk to the output device
          - update the transcript message as chunks come in (transcript_message is always provided for non filler audio utterances)
        - If the stop_event is set, the output is stopped
        - Sets started_event when the first chunk is sent

        Importantly, we rate limit the chunks sent to the output. For interrupts to work properly,
        the next chunk of audio can only be sent after the last chunk is played, so we send
        a chunk of x seconds only after x seconds have passed since the last chunk was sent.

        Returns the message that was sent up to, and a flag if the message was cut off
        """
        if self.transcriber.get_transcriber_config().mute_during_speech:
            self.logger.debug("Muting transcriber")
            self.transcriber.mute()
        message_sent = message
        cut_off = False
        chunk_size = seconds_per_chunk * get_chunk_size_per_second(
            self.synthesizer.get_synthesizer_config().audio_encoding,
            self.synthesizer.get_synthesizer_config().sampling_rate,
        )

        chunk_idx = 0
        seconds_spoken = 0
        async for chunk_result in synthesis_result.chunk_generator:
            if self.first_chunk_flag:
                self.first_chunk_flag = False
                self.first_synthesis_span.end()
            start_time = time.time()
            speech_length_seconds = seconds_per_chunk * (
                len(chunk_result.chunk) / chunk_size
            )
            seconds_spoken = chunk_idx * seconds_per_chunk
            if stop_event.is_set() and (not cut_off):
                self.logger.debug("Stop event triggered, checking if bot should finish sentence.")
                if should_finish_sentence(message, seconds_spoken):
                    self.logger.debug("Bot should finish sentence.")
                    cut_off = True
                else:
                    self.logger.debug(
                        "Interrupted, stopping text to speech after {} chunks".format(
                            chunk_idx
                        )
                    )
                    message_sent = f"{synthesis_result.get_message_up_to(seconds_spoken)}-"
                    cut_off = True
                    break

            if chunk_idx == 0:
                if started_event:
                    started_event.set()
            self.logger.debug(f"Sending chunk {chunk_idx} to output device...")
            self.output_device.consume_nonblocking(chunk_result.chunk)
            end_time = time.time()
            await asyncio.sleep(
                max(
                    speech_length_seconds
                    - (end_time - start_time)
                    - self.per_chunk_allowance_seconds,
                    0,
                )
            )
            self.logger.debug(
                "Sent chunk {} with size {}".format(chunk_idx, len(chunk_result.chunk))
            )
            self.mark_last_action_timestamp()
            chunk_idx += 1
            seconds_spoken += seconds_per_chunk
            if transcript_message:
                transcript_message.text = synthesis_result.get_message_up_to(
                    seconds_spoken
                )
        if self.transcriber.get_transcriber_config().mute_during_speech:
            self.logger.debug("Unmuting transcriber")
            self.transcriber.unmute()
        if transcript_message:
            transcript_message.text = message_sent
        return message_sent, cut_off, seconds_spoken

    def mark_terminated(self):
        self.active = False

    async def terminate(self):
        self.broadcast_interrupt()
        if self.is_active():
            self.events_manager.publish_event(
                TranscriptCompleteEvent(conversation_id=self.id, transcript=self.transcript)
            )
        self.mark_terminated()
        if not self.initial_message_task.done():
            self.logger.debug("Terminating initial_message Task")
            self.initial_message_task.cancel()
        if self.check_for_idle_task:
            self.logger.debug("Terminating check_for_idle Task")
            self.check_for_idle_task.cancel()
        if self.track_bot_sentiment_task:
            self.logger.debug("Terminating track_bot_sentiment Task")
            self.track_bot_sentiment_task.cancel()
        if self.events_manager and self.events_task:
            self.logger.debug("Terminating events Task")
            self.events_task.cancel()
            await self.events_manager.flush()
        self.logger.debug("Tearing down synthesizer")
        tear_down_synthesizer_task = asyncio.create_task(self.synthesizer.tear_down())
        self.logger.debug("Terminating agent")
        if (
            isinstance(self.agent, ChatGPTAgent)
            and self.agent.agent_config.vector_db_config
        ):
            # Shutting down the vector db should be done in the agent's terminate method,
            # but it is done here because `vector_db.tear_down()` is async and
            # `agent.terminate()` is not async.
            self.logger.debug("Terminating vector db")
            await self.agent.vector_db.tear_down()
        self.agent.terminate()
        self.logger.debug("Terminating output device")
        self.output_device.terminate()
        self.logger.debug("Terminating speech transcriber")
        terminate_transcriber_task = asyncio.create_task(self.transcriber.terminate())
        self.logger.debug("Terminating transcriptions worker")
        self.transcriptions_worker.terminate()
        self.logger.debug("Terminating final transcriptions worker")
        self.agent_responses_worker.terminate()
        self.logger.debug("Terminating synthesis results worker")
        self.synthesis_results_worker.terminate()
        self.logger.debug("Terminating random audio manager")
        self.random_audio_manager.terminate()
        if self.actions_worker is not None:
            self.logger.debug("Terminating actions worker")
            self.actions_worker.terminate()
        await terminate_transcriber_task
        self.logger.debug("Terminated speech transcriber")
        await tear_down_synthesizer_task
        self.logger.debug("Terminated synthesizer")
        self.logger.debug("Successfully terminated")

    def is_active(self):
        return self.active
