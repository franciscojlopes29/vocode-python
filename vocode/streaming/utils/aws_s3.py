import boto3

class S3Wrapper:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3')

    def load_from_s3(self, object_key):
        try:
            response = self.s3.get_object(
                Bucket=self.bucket_name,
                Key=object_key,
            )
            return response["Body"].read()
        except Exception as e:
            print(f"Error loading object from S3: {str(e)}")
            return None

    def upload_to_s3(self, object_key, data, content_type='application/octet-stream'):
        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=data,
                ContentType=content_type
            )
        except Exception as e:
            print(f"Error uploading object to S3: {str(e)}")

    def delete_from_s3(self, object_key):
        try:
            self.s3.delete_object(
                Bucket=self.bucket_name,
                Key=object_key
            )
        except Exception as e:
            print(f"Error deleting object from S3: {str(e)}")

# Example usage:
if __name__ == "__main__":
    s3_wrapper = S3Wrapper("your-bucket-name")
    
    # Load an object from S3
    data = s3_wrapper.load_from_s3("example-object-key")
    if data:
        print("Loaded data from S3:", data.decode())
    
    # Upload an object to S3
    data_to_upload = b"Hello, S3!"
    s3_wrapper.upload_to_s3("new-object-key", data_to_upload)
    
    # Delete an object from S3
    s3_wrapper.delete_from_s3("example-object-key")
