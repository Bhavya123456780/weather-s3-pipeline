import requests
import json
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
BUCKET  = os.getenv("AWS_BUCKET_NAME")

CITIES = [
    "Seattle", "New York", "Chicago",
    "Houston", "Los Angeles"
]

def get_weather(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        print(f"✓ Got weather for {city}")
        return response.json()
    else:
        print(f"✗ Failed for {city}: {response.status_code}")
        return None

def save_to_s3(data, city):
    s3 = boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    now  = datetime.now()
    key  = f"weather/{now.year}/{now.month:02d}/{now.day:02d}/{city}_{now.hour:02d}.json"
    s3.put_object(
        Bucket = BUCKET,
        Key    = key,
        Body   = json.dumps(data)
    )
    print(f"✓ Saved {city} to S3: {key}")

def extract():
    results = []
    for city in CITIES:
        data = get_weather(city)
        if data:
            results.append(data)
            save_to_s3(data, city)
    print(f"\n✓ Extracted {len(results)}/{len(CITIES)} cities")
    return results

if __name__ == "__main__":
    extract()