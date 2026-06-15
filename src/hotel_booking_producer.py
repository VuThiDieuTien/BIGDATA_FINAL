import json
import time
import random
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer

# ── Cấu hình ─────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME              = 'hotel-bookings'
CSV_PATH                = 'hotel_bookings.csv'
SEND_INTERVAL_SECONDS   = 0.1   # 10 records/giây = ~600 booking/phút
LOOP_DATASET            = True  # True = phát lại mãi


# ── Producer Factory ──────────────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    """
    Tạo KafkaProducer với cấu hình sản xuất.
    
    acks='all'           → tất cả replicas xác nhận trước khi trả về
    batch_size=16384     → gom 16KB/batch → giảm network round-trips
    linger_ms=10         → đợi tối đa 10ms để fill batch
    compression_type     → nén gzip giảm ~70% bandwidth
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k,
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10,
        compression_type='gzip',  
    )


# ── Record Enrichment ─────────────────────────────────────────────────────────
def enrich_record(record: dict) -> dict:
    """
    Thêm metadata vào mỗi booking record trước khi gửi Kafka.
    
    event_time : timestamp thực của lúc gửi → Spark dùng làm event time
    event_id   : ID duy nhất cho deduplication
    """
    record['event_time'] = datetime.now().isoformat()
    record['event_id']   = f"BKG-{random.randint(1_000_000, 9_999_999)}"
    return record


# ── Partition Key ─────────────────────────────────────────────────────────────
def get_key(record: dict) -> str:
    """
    Dùng hotel type làm partition key.
    → Booking cùng khách sạn vào cùng partition → ordering được đảm bảo.
    
    Kafka hash(key) % num_partitions:
      'Resort Hotel' → partition 0
      'City Hotel'   → partition 1
    """
    return record.get('hotel', 'Unknown')


# ── Main Loop ─────────────────────────────────────────────────────────────────
def run_producer():
    print("╔══════════════════════════════════════════════════╗")
    print("║      HOTEL BOOKING KAFKA PRODUCER               ║")
    print("╚══════════════════════════════════════════════════╝")
    print(f"  Topic   : {TOPIC_NAME}")
    print(f"  Brokers : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Speed   : {1/SEND_INTERVAL_SECONDS:.0f} records/giây")
    print(f"  Loop    : {LOOP_DATASET}")

    # Load dataset
    df = pd.read_csv(CSV_PATH).fillna(0)
    total = len(df)
    print(f"\n  Dataset : {total:,} bản ghi\n")

    producer   = create_producer()
    sent       = 0
    loop_no    = 0
    errors     = 0

    try:
        while True:
            loop_no += 1
            print(f"[Loop {loop_no}] Bắt đầu gửi {total:,} bản ghi...")

            for _, row in df.iterrows():
                record = enrich_record(row.to_dict())

                try:
                    future = producer.send(
                        topic=TOPIC_NAME,
                        key=get_key(record),
                        value=record,
                    )
                    sent += 1

                    # Log tiến độ mỗi 500 records
                    if sent % 500 == 0:
                        meta = future.get(timeout=5)
                        print(
                            f"  [{sent:>7,}] ✓ "
                            f"hotel={record.get('hotel','?')[:6]:6s} | "
                            f"country={record.get('country','?'):3s} | "
                            f"canceled={record.get('is_canceled',0)} | "
                            f"adr={record.get('adr',0):6.1f} | "
                            f"partition={meta.partition} offset={meta.offset}"
                        )

                except Exception as e:
                    errors += 1
                    print(f"  [WARN] Gửi thất bại #{errors}: {e}")

                time.sleep(SEND_INTERVAL_SECONDS)

            producer.flush()
            print(f"\n[Loop {loop_no} DONE] Tổng gửi: {sent:,} | Lỗi: {errors}")

            if not LOOP_DATASET:
                break

    except KeyboardInterrupt:
        print(f"\n[STOP] Dừng bởi user. Đã gửi: {sent:,} messages, Lỗi: {errors}")
    finally:
        producer.close()
        print("[DONE] Producer đóng kết nối Kafka.")


if __name__ == "__main__":
    run_producer()
