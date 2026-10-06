import json
import sqlite3
from kafka import KafkaConsumer
from pydantic import BaseModel, Field, ValidationError

# 1. Define our strict Data Quality Contract using Pydantic
class CryptoRecord(BaseModel):
    symbol: str = Field(..., min_length=3)
    price: float = Field(..., gt=0.0)      # Business rule: Price MUST be greater than 0
    timestamp: str = Field(..., min_length=10)

DB_NAME = "crypto.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices_validated (
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
    group_id='crypto-dq-consumer-group'
)

print("🛡️ Data Quality Consumer started. Enforcing data contracts...\n")

try:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for message in consumer:
        raw_value = message.value.decode('utf-8')
        
        try:
            # Parse raw JSON string into a dictionary first
            data_dict = json.loads(raw_value)
            
            # 3. Apply Pydantic Data Quality Validation
            # This validates types, missing fields, and business rules (e.g., price > 0)
            validated_record = CryptoRecord(**data_dict)
            
            # If validation passes, insert into our validated table
            cursor.execute('''
                INSERT INTO crypto_prices_validated (symbol, price, timestamp)
                VALUES (?, ?, ?)
            ''', (validated_record.symbol, validated_record.price, validated_record.timestamp))
            conn.commit()
            
            print(f"✅ PASSED DQ -> Symbol: {validated_record.symbol} | Price: ${validated_record.price:.2f}")
            
        except json.JSONDecodeError:
            print("⚠️ WARNING [DLQ]: Received invalid JSON format. Skipped.")
        except ValidationError as e:
            # Data Quality Failure! Caught by Pydantic
            print(f"❌ FAILED DQ [Quarantined]: Invalid data record rejected. Reason: {e.errors()}")

except KeyboardInterrupt:
    print("\n🛑 Data Quality Consumer stopped gracefully.")
    if 'conn' in locals():
        conn.close()
    consumer.close()