from typing import Optional

from vocode.streaming.models.vector_db import PineconeConfig
from vocode.streaming.utils.aws_s3 import S3Wrapper

from .model import BaseModel

class IndexConfig(BaseModel):
    pinecone_config: Optional[PineconeConfig] = None
    s3_wrapper: Optional[S3Wrapper] = None