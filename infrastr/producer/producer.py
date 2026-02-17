#Import requirements
import time
import json
import requests
from kafka import KafkaProducer

#Define variables for API
API_KEY="d6a8qppr01qqjvbphbn0d6a8qppr01qqjvbphbng"
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]




#--------
#producer = KafkaProducer(
#    bootstrap_servers=['localhost:29092'],  # match PLAINTEXT_HOST
#    value_serializer=lambda v: json.dumps(v).encode('utf-8')
#)
#
#topic_name = "stock-data"
#
#for i in range(5):
#    message = {"number": i, "text": f"Hello {i}"}
#    print("Producing:", message)
#    producer.send(topic_name, value=message)
#    producer.flush()
#    time.sleep(1)
#



#--------
#Initial Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],  # match PLAINTEXT_HOST
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
            producer.send("stock-data", value=quote)
    time.sleep(6)