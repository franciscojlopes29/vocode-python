from vocode.streaming.models.vector_db import PineconeConfig
from vocode.streaming.utils.aws_s3 import S3Wrapper

class IndexConfig():
    def __init__(self, pinecone_config: PineconeConfig, s3_wrapper: S3Wrapper):
        self.pinecone_config: PineconeConfig = pinecone_config
        self.s3_wrapper: S3Wrapper = s3_wrapper