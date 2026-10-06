import json
from kafka import KafkaConsumer

# We use a new group_id to start fresh from the beginning of the topic
consumer = KafkaConsumer(
    'crypto-prices',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crypto-consumer-group-v2'
)

print("👂 Kafka Consumer started with robust error handling...\n")

try:
    for message in consumer:
        # Decode the raw bytes into a string first
        raw_value = message.value.decode('utf-8')
        
        try:
            # Try parsing as JSON (handles our Python producer data)
            crypto_data = json.loads(raw_value)
            symbol = crypto_data.get("symbol")
            price = crypto_data.get("price")
            timestamp = crypto_data.get("timestamp")
            
            print(f"📥 Consumed JSON -> Symbol: {symbol} | Price: ${price:.2f} | Time: {timestamp}")
            
        except json.JSONDecodeError:
            # Gracefully handle legacy plain-text messages from Phase 2 testing
            print(f"📥 Consumed Legacy Text (Ignored) -> {raw_value}")

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped gracefully by user.")
    consumer.close()