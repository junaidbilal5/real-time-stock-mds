#Import requirements
import time
import json
import requests
from kafka import KafkaProducer

#Define variables for API
API_KEY="d6912kpr01qs7u9kbft0d6912kpr01qs7u9kbftg"
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

#Initial Producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
#Retrive Data
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int (time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

#Looping and Pushing to Stream
while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing: {quote}")
            producer.send("stock-qoutes", value=quote)
    time.sleep(6)