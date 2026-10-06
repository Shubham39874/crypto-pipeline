import json
import sqlite3
from datetime import datetime
from kafka import KafkaConsumer

DB_NAME = "crypto.db"

# 1. Initialize Medallion Lakehouse Tables
def init_lakehouse():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # BRONZE LAYER: Raw, immutable log storage
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bronze_crypto (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_payload TEXT,
            ingested_at TEXT
        )
    ''')
    
    # SILVER LAYER: Cleaned, validated, typed records
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS silver_crypto (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            timestamp TEXT NOT NULL,
            processed_at TEXT
        )
    ''')
    
    # GOLD LAYER: Pre-aggregated business analytics per symbol
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS gold_crypto_aggregates (
            symbol TEXT PRIMARY KEY,
            avg_price REAL,
            min_price REAL,
            max_price REAL,
            total_records INTEGER,
            last_updated TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    print("🏛️ Medallion Lakehouse (Bronze, Silver, Gold) tables initialized.")

init_lakehouse()

# 2. Initialize Kafka Consumer
consumer = KafkaConsumer(
    'crypto-prices',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crypto-lakehouse-group'
)

print("👂 Lakehouse Pipeline Consumer started. Streaming data across Medallion layers...\n")

try:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for message in consumer:
        raw_value = message.value.decode('utf-8')
        current_time = datetime.now().isoformat()
        
        # --- LAYER 1: BRONZE (Ingest Raw Data) ---
        cursor.execute('''
            INSERT INTO bronze_crypto (raw_payload, ingested_at)
            VALUES (?, ?)
        ''', (raw_value, current_time))
        
        try:
            data_dict = json.loads(raw_value)
            symbol = data_dict.get("symbol")
            price = float(data_dict.get("price"))
            timestamp = data_dict.get("timestamp")
            
            # --- LAYER 2: SILVER (Clean & Validate) ---
            cursor.execute('''
                INSERT INTO silver_crypto (symbol, price, timestamp, processed_at)
                VALUES (?, ?, ?, ?)
            ''', (symbol, price, timestamp, current_time))
            
            # --- LAYER 3: GOLD (Aggregate Business Metrics) ---
            # Fetch existing aggregate stats for this symbol
            cursor.execute('SELECT avg_price, min_price, max_price, total_records FROM gold_crypto_aggregates WHERE symbol = ?', (symbol,))
            row = cursor.fetchone()
            
            if row is None:
                # First record for this symbol
                new_avg = price
                new_min = price
                new_max = price
                total_recs = 1
            else:
                old_avg, old_min, old_max, total_recs = row
                total_recs += 1
                new_avg = ((old_avg * (total_recs - 1)) + price) / total_recs
                new_min = min(old_min, price)
                new_max = max(old_max, price)
            
            # Upsert into Gold table
            cursor.execute('''
                INSERT OR REPLACE INTO gold_crypto_aggregates (symbol, avg_price, min_price, max_price, total_records, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (symbol, new_avg, new_min, new_max, total_recs, current_time))
            
            conn.commit()
            print(f"🏛️ LAKEHOUSE WRITE -> [Bronze: Raw Log] | [Silver: Cleaned {symbol} @ ${price:.2f}] | [Gold: Updated Aggregates]")
            
        except Exception as e:
            conn.rollback()
            print(f"⚠️ Error processing record in Silver/Gold layers: {e}")

except KeyboardInterrupt:
    print("\n🛑 Lakehouse Consumer stopped gracefully.")
    if 'conn' in locals():
        conn.close()
    consumer.close()
    