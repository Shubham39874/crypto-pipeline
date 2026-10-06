import json
import sqlite3
from kafka import KafkaConsumer

# 1. Setup SQLite Database Connection
# This will create a file named 'crypto.db' in your folder automatically
DB_NAME = "crypto.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # Create our target table if it doesn't already exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            timestamp TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()
    print("📁 SQLite database initialized and table 'crypto_prices' is ready.")

# Initialize the database table
init_db()

# 2. Initialize the Kafka Consumer
consumer = KafkaConsumer(
    'crypto-prices',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crypto-db-consumer-group'
)

print("👂 Database Consumer started. Listening and writing to SQLite...\n")

# 3. Consume messages and write to database
try:
    # Connect to SQLite for the streaming loop
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for message in consumer:
        raw_value = message.value.decode('utf-8')
        
        try:
            crypto_data = json.loads(raw_value)
            symbol = crypto_data.get("symbol")
            price = crypto_data.get("price")
            timestamp = crypto_data.get("timestamp")
            
            # Insert record into SQLite table
            cursor.execute('''
                INSERT INTO crypto_prices (symbol, price, timestamp)
                VALUES (?, ?, ?)
            ''', (symbol, price, timestamp))
            
            # Commit the transaction to save it permanently to disk
            conn.commit()
            
            print(f"💾 Saved to DB -> Symbol: {symbol} | Price: ${price:.2f} | Time: {timestamp}")
            
        except json.JSONDecodeError:
            # Ignore old legacy messages gracefully
            pass

except KeyboardInterrupt:
    print("\n🛑 Database Consumer stopped gracefully.")
    if 'conn' in locals():
        conn.close()
    consumer.close()