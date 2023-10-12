from vocode.streaming.models.vector_db import PineconeConfig
from vocode.streaming.utils.aws_s3 import S3Wrapper

from .model import BaseModel

class IndexConfig(BaseModel):
    pinecone_config: PineconeConfig
    s3_wrapper: S3Wrapper