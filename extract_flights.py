import requests
import os 
from dotenv import load_dotenv

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
else:
    data = response.json()
    print(data)

