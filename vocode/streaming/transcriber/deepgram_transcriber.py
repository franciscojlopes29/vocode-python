import asyncio
import json
import logging
import time
from typing import Optional
import websockets
from websockets.client import WebSocketClientProtocol
import audioop
from urllib.parse import urlencode, quote
from vocode import getenv

from vocode.streaming.transcriber.base_transcriber import (
    BaseAsyncTranscriber,
    Transcription,
    tracer,
    meter,
    HUMAN_ACTIVITY_DETECTED
)
from vocode.streaming.models.transcriber import (
    DeepgramTranscriberConfig,
    EndpointingConfig,
    EndpointingType,
    PunctuationEndpointingConfig,
    TimeEndpointingConfig,
)
from vocode.streaming.models.audio_encoding import AudioEncoding


PUNCTUATION_TERMINATORS = [".", "!", "?"]
NUM_RESTARTS = 5


avg_latency_hist = meter.create_histogram(
    name="transcriber.deepgram.avg_latency",
    unit="seconds",
)
max_latency_hist = meter.create_histogram(
    name="transcriber.deepgram.max_latency",
    unit="seconds",
)
min_latency_hist = meter.create_histogram(
    name="transcriber.deepgram.min_latency",
    unit="seconds",
)
duration_hist = meter.create_histogram(
    name="transcriber.deepgram.duration",
    unit="seconds",
)


class DeepgramTranscriber(BaseAsyncTranscriber[DeepgramTranscriberConfig]):
    def __init__(
        self,
        transcriber_config: DeepgramTranscriberConfig,
        api_key: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(transcriber_config)
        self.api_key = api_key or getenv("DEEPGRAM_API_KEY")
        if not self.api_key:
            raise Exception(
                "Please set DEEPGRAM_API_KEY environment variable or pass it as a parameter"
            )
        self._ended = False
        self._task = None
        self.received_first_audio = False
        self.is_ready = False
        self.logger = logger or logging.getLogger(__name__)
        self.audio_cursor = 0.0

    async def _run_loop(self):
        try:
            restarts = 0
            while not self._ended and restarts < NUM_RESTARTS:
                await self.process()
                if self._ended:
                    break
                restarts += 1
                self.logger.debug(
                    "Deepgram connection died, restarting, num_restarts: %s", restarts
                )
        except asyncio.CancelledError:
            return

    def send_audio(self, chunk):
        if (
            self.transcriber_config.downsampling
            and self.transcriber_config.audio_encoding == AudioEncoding.LINEAR16
        ):
            chunk, _ = audioop.ratecv(
                chunk,
                2,
                1,
                self.transcriber_config.sampling_rate
                * self.transcriber_config.downsampling,
                self.transcriber_config.sampling_rate,
                None,
            )
        super().send_audio(chunk)

    async def terminate(self):
        terminate_msg = json.dumps({"type": "CloseStream"})
        self.input_queue.put_nowait(terminate_msg)
        self._ended = True
        self._task.cancel()
        super().terminate()

    def get_deepgram_url(self):
        if self.transcriber_config.audio_encoding == AudioEncoding.LINEAR16:
            encoding = "linear16"
        elif self.transcriber_config.audio_encoding == AudioEncoding.MULAW:
            encoding = "mulaw"
        url_params = {
            "encoding": encoding,
            "sample_rate": self.transcriber_config.sampling_rate,
            "channels": 1,
            "interim_results": "true",
        }
        extra_params = {}
        if self.transcriber_config.language:
            extra_params["language"] = self.transcriber_config.language
        if self.transcriber_config.model:
            extra_params["model"] = self.transcriber_config.model
        if self.transcriber_config.tier:
            extra_params["tier"] = self.transcriber_config.tier
        if self.transcriber_config.version:
            extra_params["version"] = self.transcriber_config.version
        if self.transcriber_config.filler_words:
            extra_params["filler_words"] = self.transcriber_config.filler_words
        if self.transcriber_config.deepgram_endpointing:
            extra_params["endpointing"] = self.transcriber_config.deepgram_endpointing
        if self.transcriber_config.keywords:
            extra_params["keywords"] = self.transcriber_config.keywords
        if (
            self.transcriber_config.endpointing_config
            and self.transcriber_config.endpointing_config.type
            == EndpointingType.PUNCTUATION_BASED
        ):
            extra_params["punctuate"] = "true"
        url_params.update(extra_params)

        # Encode the "keywords" field first
        encoded_keywords = "&".join([f"keywords={quote(kw)}" for kw in url_params["keywords"]])
        del url_params["keywords"]
        final_url = f"wss://api.deepgram.com/v1/listen?{urlencode(url_params)}&{encoded_keywords}"
        return final_url

    def is_speech_final(
        self, current_buffer: str, deepgram_response: dict, time_silent: float
    ):
        transcript = deepgram_response["channel"]["alternatives"][0]["transcript"]

        # if it is not time based, then return true if speech is final and there is a transcript
        if not self.transcriber_config.endpointing_config:
            return transcript and deepgram_response["speech_final"]
        elif isinstance(
            self.transcriber_config.endpointing_config, TimeEndpointingConfig
        ):
            # if it is time based, then return true if there is no transcript
            # and there is some speech to send
            # and the time_silent is greater than the cutoff
            return (
                not transcript
                and current_buffer
                and (time_silent + deepgram_response["duration"])
                > self.transcriber_config.endpointing_config.time_cutoff_seconds
            )
        elif isinstance(
            self.transcriber_config.endpointing_config, PunctuationEndpointingConfig
        ):
            return (
                transcript
                and deepgram_response["speech_final"]
                and transcript.strip()[-1] in PUNCTUATION_TERMINATORS
            ) or (
                not transcript
                and current_buffer
                and (time_silent + deepgram_response["duration"])
                > self.transcriber_config.endpointing_config.time_cutoff_seconds
            )
        raise Exception("Endpointing config not supported")

    def calculate_time_silent(self, data: dict):
        end = data["start"] + data["duration"]
        words = data["channel"]["alternatives"][0]["words"]
        if words:
            return end - words[-1]["end"]
        return data["duration"]
    
    async def sender(self, ws: WebSocketClientProtocol):  # sends audio to websocket
        while not self._ended:
            try:
                if not self.received_first_audio:
                    self.logger.debug("Deepgram sender: waiting for audio")
                data = await asyncio.wait_for(self.input_queue.get(), 5)
                if not self.received_first_audio:
                    self.logger.debug("Deepgram sender: sent first audio")
                self.received_first_audio = True
                if self.transcriber_config.voice_activity_detector_config:
                    # when using WebRTC VAD, there are too many false positive that break the conversation flow
                    self.logger.debug(f"Using voice activity detector.")
                    try:
                        # TODO: make this async
                        start_time = time.time()
                        if self.voice_activity_detector.should_interrupt(data):
                            self.logger.debug(f"VAD detected - took {time.time() - start_time:.3f} seconds")
                            self.output_queue.put_nowait(
                                Transcription(
                                    message=HUMAN_ACTIVITY_DETECTED,
                                    confidence=1,
                                    is_final=True,
                                )
                            )
                        
                    except Exception as e:
                        self.logger.debug(f"Error in voice activity detector: {repr(e)}")
            except asyncio.exceptions.TimeoutError:
                if not self.received_first_audio:
                    self.logger.debug("Deepgram sender: sending KeepAlive")
                    await ws.send(json.dumps({"type": "KeepAlive"}))
                    continue
                if self._ended:
                    self.logger.debug("Deepgram sender: sending CloseStream")
                    terminate_msg = json.dumps({"type": "CloseStream"})
                    await ws.send(terminate_msg)
                    continue
                break
            num_channels = 1
            sample_width = 2
            self.audio_cursor += len(data) / (
                self.transcriber_config.sampling_rate
                * num_channels
                * sample_width
            )
            await ws.send(data)
        self.logger.debug("Terminating Deepgram transcriber sender")
        return

    async def receiver(self, ws: WebSocketClientProtocol):
        buffer = ""
        buffer_avg_confidence = 0
        num_buffer_utterances = 1
        time_silent = 0
        transcript_cursor = 0.0
        sent_vad_transcription = False
        while not self._ended:
            try:
                msg = await ws.recv()
            except websockets.exceptions.ConnectionClosed:
                self.logger.debug("Deepgram websocket connection closed")
                break
            except asyncio.TimeoutError:
                if self._ended:
                    self.logger.debug("Deepgram receiver timeout error")                    
                    break
            except Exception as e:
                self.logger.debug(f"Got error {e} in Deepgram receiver")
                break
            data = json.loads(msg)
            if not self.received_first_audio:
                self.logger.debug(f"Deepgram receiver: got message {data}")
            if (
                not "is_final" in data
            ):  # means we've finished receiving transcriptions
                self.logger.debug(f"Deepgram: received final transcription - _ended:{self._ended}")
                self._ended = True
                break
            cur_max_latency = self.audio_cursor - transcript_cursor
            transcript_cursor = data["start"] + data["duration"]
            cur_min_latency = self.audio_cursor - transcript_cursor

            avg_latency_hist.record(
                (cur_min_latency + cur_max_latency) / 2 * data["duration"]
            )
            duration_hist.record(data["duration"])

            # Log max and min latencies
            max_latency_hist.record(cur_max_latency)
            min_latency_hist.record(max(cur_min_latency, 0))

            is_final = data["is_final"]
            speech_final = self.is_speech_final(buffer, data, time_silent)
            top_choice = data["channel"]["alternatives"][0]
            confidence = top_choice["confidence"]

            if top_choice["transcript"] and confidence > 0.0 and is_final:
                buffer = f"{buffer} {top_choice['transcript']}"
                if buffer_avg_confidence == 0:
                    buffer_avg_confidence = confidence
                else:
                    buffer_avg_confidence = (
                        buffer_avg_confidence
                        + confidence / (num_buffer_utterances)
                    ) * (num_buffer_utterances / (num_buffer_utterances + 1))
                num_buffer_utterances += 1

            if speech_final:
                self.output_queue.put_nowait(
                    Transcription(
                        message=buffer,
                        confidence=buffer_avg_confidence,
                        is_final=True,
                    )
                )
                buffer = ""
                buffer_avg_confidence = 0
                num_buffer_utterances = 1
                time_silent = 0
                sent_vad_transcription = False
            elif (
                data["duration"] > self.transcriber_config.minimum_speaking_duration_to_interrupt
                and len(top_choice["words"])> self.transcriber_config.interruption_word_threshold
                and not sent_vad_transcription
            ):
                self.logger.debug("Sending VAD transcription")
                self.output_queue.put_nowait(
                    Transcription(
                        message=HUMAN_ACTIVITY_DETECTED,
                        confidence=1,
                        is_final=False,
                    )
                )
                sent_vad_transcription = True
                time_silent = self.calculate_time_silent(data)
            elif top_choice["transcript"] and confidence > 0.0:
                self.output_queue.put_nowait(
                    Transcription(
                        message=buffer,
                        confidence=confidence,
                        is_final=False,
                    )
                )
                time_silent = self.calculate_time_silent(data)
            else:
                time_silent += data["duration"]
        self.logger.debug("Terminating Deepgram transcriber receiver")
        return

    async def process(self):
        try:
            self.audio_cursor = 0.0
            extra_headers = {"Authorization": f"Token {self.api_key}"}
            self.logger.debug(f"Connecting to Deepgram...")
            start_time = time.time()
            async with websockets.connect(
                self.get_deepgram_url(), extra_headers=extra_headers
            ) as ws:
                self.logger.debug(f"Connected to Deepgram! Connection took {time.time()-start_time:.2f} sec.")
                self._task= asyncio.gather(self.sender(ws), self.receiver(ws))
                await self._task
        except asyncio.CancelledError:
            return
