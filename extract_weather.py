import requests
import os 
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("WEATHER_API_KEY")

url = "https://api.openweathermap.org/data/2.5/weather"

params = {
    "lat": 41.9742,
    "lon": -87.9073,
    "appid": api_key,
    "units": "imperial"
}

response = requests.get(url, params = params)

if response.status_code != 200: 
    print(f"Request failed: {response.status_code} {response.text}")
else:
    data = response.json()
    print(data)
