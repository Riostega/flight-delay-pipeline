import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3", 
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name = os.getenv("AWS_REGION")
)

test_content = "hello from Flight Delay Pipeline"

s3.put_object(
    Bucket=os.getenv("S3_BUCKET_NAME"),
    Key="test/test_file.txt",
    Body=test_content
)

print("Upload successful")