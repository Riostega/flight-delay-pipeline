import requests
import boto3
import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def fetch_flights():
    load_dotenv()

    api_key = os.getenv("FLIGHT_API_KEY")

    url = "http://api.aviationstack.com/v1/flights"

    params = {
        "access_key" : api_key,
        "limit" : 5,
        "flight_status" : "landed"
    }

    response = requests.get(url, params = params)

    if response.status_code != 200:
        print(f"Request failed: {response.status_code} {response.text}")
        return None 
    else:
        data = response.json()
        print(data)
        return data

    

def upload_to_s3(data):
    load_dotenv()

    s3 = boto3.client(
        "s3", 
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name = os.getenv("AWS_REGION")
    )
    now = datetime.now()

    s3.put_object(
        Bucket=os.getenv("S3_BUCKET_NAME"),
        Key=f"raw/flights/{now.strftime('%Y-%m-%d')}/{now.strftime('%H%M%S')}.json",
        Body=json.dumps(data)
    )

    print("Upload successful")

if __name__ =="__main__":
    flights = fetch_flights()
    if flights:
        upload_to_s3(flights)
