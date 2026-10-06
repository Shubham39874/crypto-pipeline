import json
import sqlite3
import time
import logging
from kafka import KafkaConsumer

# 1. Configure Structured Logging
# This logs messages to both the console AND a file named 'pipeline.log'
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

DB_NAME = "crypto.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices_monitored (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            timestamp TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# 2. Initialize Kafka Consumer
consumer = KafkaConsumer(
    'crypto-prices',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crypto-obs-consumer-group'
)

logging.info("🔭 Observable Kafka Consumer started. Tracking telemetry and metrics...")

# 3. Initialize metrics counters
metrics = {
    "messages_processed": 0,
    "messages_failed": 0,
    "start_time": time.time()
}

try:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for message in consumer:
        start_processing_time = time.time()
        raw_value = message.value.decode('utf-8')
        
        try:
            data_dict = json.loads(raw_value)
            symbol = data_dict.get("symbol")
            price = data_dict.get("price")
            timestamp = data_dict.get("timestamp")
            
            # Write to database
            cursor.execute('''
                INSERT INTO crypto_prices_monitored (symbol, price, timestamp)
                VALUES (?, ?, ?)
            ''', (symbol, price, timestamp))
            conn.commit()
            
            # Update metrics
            metrics["messages_processed"] += 1
            duration = (time.time() - start_processing_time) * 1000 # in milliseconds
            
            # Log success telemetry every 5 messages
            if metrics["messages_processed"] % 5 == 0:
                uptime_mins = (time.time() - metrics["start_time"]) / 60
                logging.info(
                    f"METRIC | Processed: {metrics['messages_processed']} | "
                    f"Failed: {metrics['messages_failed']} | "
                    f"Last Latency: {duration:.2f}ms | "
                    f"Uptime: {uptime_mins:.1f}m"
                )
            else:
                logging.debug(f"Successfully processed record for {symbol}")
                
        except Exception as e:
            metrics["messages_failed"] += 1
            logging.error(f"Failed to process message. Error: {e} | Payload: {raw_value}")

except KeyboardInterrupt:
    uptime_total = (time.time() - metrics["start_time"]) / 60
    logging.info(
        f"🛑 Consumer stopped. Final Stats -> "
        f"Total Processed: {metrics['messages_processed']} | "
        f"Total Failed: {metrics['messages_failed']} | "
        f"Total Runtime: {uptime_total:.1f} mins"
    )
    if 'conn' in locals():
        conn.close()
    consumer.close()