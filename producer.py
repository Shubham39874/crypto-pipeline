import json
import time
from kafka import KafkaProducer
import yfinance as yf

# 1. Initialize the Kafka Producer
# We connect to localhost:9092 because our Python script runs on your Windows machine 
# (outside Docker), talking to the port we exposed in docker-compose.yml.
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

symbols = ["BTC-USD", "ETH-USD", "USDT-USD"]
topic_name = "crypto-prices"

print("🚀 Crypto Kafka Producer started. Streaming data to topic 'crypto-prices'...")

while True:
    try:
        for symbol in symbols:
            # Fetch live ticker data using yfinance
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d", interval="1m")
            
            if not data.empty:
                latest_price = float(data["Close"].iloc[-1])
                timestamp = str(data.index[-1])
                
                # Construct our message payload as a Python dictionary
                message = {
                    "symbol": symbol,
                    "price": latest_price,
                    "timestamp": timestamp
                }
                
                # Send the message to our Kafka topic
                producer.send(topic_name, value=message)
                print(f"Produced -> {message}")
                
        # Force send any buffered messages
        producer.flush()
        
        # Wait 5 seconds before the next polling cycle
        time.sleep(5)
        
    except Exception as e:
        print(f"⚠️ Error fetching or producing data: {e}")
        time.sleep(5)