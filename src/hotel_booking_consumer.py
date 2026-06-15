import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, avg, sum as spark_sum,
    when, round as spark_round, desc, rank
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.sql.window import Window as WindowSpec


# ══════════════════════════════════════════════════════════════════
# 9.5.1 SPARKSESSION
# ══════════════════════════════════════════════════════════════════
def create_spark_session() -> SparkSession:
    """
    SparkSession với Kafka connector.
    
    Cấu hình quan trọng:
      spark.sql.shuffle.partitions=4   → giảm số shuffle partitions
                                          (mặc định 200 quá nhiều cho dev)
      checkpointLocation               → lưu offset để recovery sau crash
    """
def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("HotelBookingRealTimeMonitor")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
        )
        .config("spark.driver.memory", "2g")
        .config(
            "spark.sql.streaming.checkpointLocation", 
            "/tmp/hotel-streaming-checkpoint"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ══════════════════════════════════════════════════════════════════
# 9.5.2 SCHEMA – Cấu trúc JSON từ Kafka
# ══════════════════════════════════════════════════════════════════
BOOKING_SCHEMA = StructType([
    StructField("hotel",                     StringType(),  True),
    StructField("is_canceled",               IntegerType(), True),
    StructField("lead_time",                 IntegerType(), True),
    StructField("arrival_date_year",         IntegerType(), True),
    StructField("arrival_date_month",        StringType(),  True),
    StructField("arrival_date_day_of_month", IntegerType(), True),
    StructField("stays_in_weekend_nights",   IntegerType(), True),
    StructField("stays_in_week_nights",      IntegerType(), True),
    StructField("adults",                    IntegerType(), True),
    StructField("children",                  IntegerType(), True),
    StructField("babies",                    IntegerType(), True),
    StructField("meal",                      StringType(),  True),
    StructField("country",                   StringType(),  True),
    StructField("market_segment",            StringType(),  True),
    StructField("distribution_channel",      StringType(),  True),
    StructField("is_repeated_guest",         IntegerType(), True),
    StructField("previous_cancellations",    IntegerType(), True),
    StructField("reserved_room_type",        StringType(),  True),
    StructField("assigned_room_type",        StringType(),  True),
    StructField("booking_changes",           IntegerType(), True),
    StructField("deposit_type",              StringType(),  True),
    StructField("customer_type",             StringType(),  True),
    StructField("adr",                       DoubleType(),  True),
    StructField("days_in_waiting_list",      IntegerType(), True),
    StructField("total_of_special_requests", IntegerType(), True),
    StructField("reservation_status",        StringType(),  True),
    # Metadata thêm bởi Producer
    StructField("event_time",                StringType(),  True),
    StructField("event_id",                  StringType(),  True),
])


# ══════════════════════════════════════════════════════════════════
# 9.5.3 ĐỌC VÀ PARSE KAFKA STREAM
# ══════════════════════════════════════════════════════════════════
def read_kafka_stream(spark: SparkSession):
    """
    Đọc raw bytes từ Kafka.
    
    startingOffsets="latest"     → chỉ nhận message MỚI
    maxOffsetsPerTrigger=1000    → tối đa 1000 records/batch
                                   (tránh quá tải khi backlog lớn)
    failOnDataLoss=false         → tiếp tục dù Kafka xóa log cũ
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "hotel-bookings")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "1000")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_booking_stream(raw_stream):
    """
    Chuyển Kafka bytes → structured DataFrame với event timestamp.
    
    Pipeline:
      raw bytes → string → JSON parse → cast timestamp → filter valid
    """
    return (
        raw_stream
        .select(col("value").cast("string").alias("json_str"))
        .select(from_json(col("json_str"), BOOKING_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_timestamp", col("event_time").cast(TimestampType()))
        # Lọc dữ liệu không hợp lệ
        .filter(col("event_timestamp").isNotNull())
        .filter(col("adr").isNotNull() & (col("adr") >= 0))
        .filter(col("adults").isNotNull() & (col("adults") > 0))
        .filter(col("hotel").isNotNull())
    )


# ══════════════════════════════════════════════════════════════════
# 9.6 STREAMING QUERY 1: Số booking mỗi phút
# ══════════════════════════════════════════════════════════════════
def query1_bookings_per_minute(booking_df):
    """
    Tumbling Window 1 phút – đếm tổng booking theo loại khách sạn.
    
    Tumbling window (không overlap):
      [00:00─01:00)  [01:00─02:00)  [02:00─03:00)
      
    outputMode="complete" → in lại toàn bộ bảng mỗi trigger
    (phù hợp khi dùng aggregation không watermark)
    """
    q = (
        booking_df
        .groupBy(
            window("event_timestamp", "1 minute"),
            "hotel"
        )
        .agg(
            count("*").alias("total_bookings"),
            count(when(col("is_canceled") == 0, 1)).alias("confirmed"),
            count(when(col("is_canceled") == 1, 1)).alias("canceled"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("hotel"),
            col("total_bookings"),
            col("confirmed"),
            col("canceled"),
        )
        .orderBy("window_start", "hotel")
    )

    return (
        q.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="1 minute")
        .queryName("Q1_BookingsPerMinute")
        .start()
    )


# ══════════════════════════════════════════════════════════════════
# 9.6 STREAMING QUERY 2: Tỷ lệ hủy phòng mỗi phút
# ══════════════════════════════════════════════════════════════════
def query2_cancellation_rate(booking_df):
    """
    Tumbling Window 1 phút – tỷ lệ hủy phòng theo loại deposit.
    
    Alert levels (dựa trên baseline dataset = 37%):
      ≥ 60% → CRITICAL  (cần can thiệp ngay)
      ≥ 50% → WARNING   (theo dõi chặt)
      ≥ 37% → ELEVATED  (cao hơn bình thường)
      < 37% → NORMAL
    """
    q = (
        booking_df
        .groupBy(
            window("event_timestamp", "1 minute"),
            "hotel",
            "deposit_type",
        )
        .agg(
            count("*").alias("total_bookings"),
            spark_sum("is_canceled").alias("total_canceled"),
            spark_round(
                (spark_sum("is_canceled") / count("*")) * 100, 2
            ).alias("cancel_rate_pct"),
        )
        .withColumn(
            "alert",
            when(col("cancel_rate_pct") >= 60, "CRITICAL")
            .when(col("cancel_rate_pct") >= 50, "WARNING")
            .when(col("cancel_rate_pct") >= 37, "ELEVATED")
            .otherwise("NORMAL"),
        )
        .select(
            col("window.start").alias("window_start"),
            "hotel", "deposit_type", "total_bookings",
            "total_canceled", "cancel_rate_pct", "alert",
        )
        .orderBy(desc("cancel_rate_pct"))
    )

    return (
        q.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 30)
        .trigger(processingTime="1 minute")
        .queryName("Q2_CancellationRate")
        .start()
    )


# ══════════════════════════════════════════════════════════════════
# 9.6 STREAMING QUERY 3: ADR trung bình theo thời gian
# ══════════════════════════════════════════════════════════════════
def query3_adr_over_time(booking_df):
    """
    Sliding Window 5 phút / slide 1 phút – ADR theo loại phòng.
    
    Sliding window (overlap):
      [00:00─05:00)
           [01:00─06:00)
                [02:00─07:00)
    → Smooth hơn tumbling, phát hiện trend tốt hơn.
    
    Chỉ tính booking chưa hủy (is_canceled=0)
    → ADR của booking đã hủy không phản ánh doanh thu thực.
    
    Dataset stats: ADR avg=$101.83, min=-6.38 (lỗi), max=$5400
    """
    q = (
        booking_df
        .filter(col("is_canceled") == 0)
        .groupBy(
            window("event_timestamp", "5 minutes", "1 minute"),
            "hotel",
            "reserved_room_type",
        )
        .agg(
            count("*").alias("booking_count"),
            spark_round(avg("adr"), 2).alias("avg_adr"),
            spark_round(spark_sum("adr"), 2).alias("total_adr"),
            spark_round(
                avg(col("stays_in_week_nights") + col("stays_in_weekend_nights")), 1
            ).alias("avg_stay_nights"),
        )
        .filter(col("booking_count") >= 5)   # Đủ mẫu thống kê
        .select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            "hotel",
            col("reserved_room_type").alias("room_type"),
            "booking_count",
            col("avg_adr").alias("avg_daily_rate"),
            col("total_adr").alias("revenue_proxy"),
            "avg_stay_nights",
        )
        .orderBy("period_start", desc("avg_daily_rate"))
    )

    return (
        q.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 40)
        .trigger(processingTime="1 minute")
        .queryName("Q3_ADROverTime")
        .start()
    )


# ══════════════════════════════════════════════════════════════════
# 9.6 STREAMING QUERY 4: Top quốc gia đặt phòng thời gian thực
# ══════════════════════════════════════════════════════════════════
def query4_top_countries(booking_df):
    q = (
        booking_df
        .withWatermark("event_timestamp", "2 minutes")
        .filter(col("country").isNotNull() & (col("country") != "NULL"))
        .groupBy(
            window("event_timestamp", "2 minutes", "1 minute"),
            "country",
            "hotel",
        )
        .agg(
            count("*").alias("booking_count"),
            spark_round(avg("adr"), 2).alias("avg_spend"),
            spark_round(
                (spark_sum("is_canceled") / count("*")) * 100, 1
            ).alias("cancel_rate_pct"),
        )
        .select(
            col("window.start").alias("period_start"),
            "country", "hotel",
            "booking_count", "avg_spend", "cancel_rate_pct",
        )
        .orderBy(desc("period_start"), desc("booking_count"))
    )

    return (
        q.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 15) 
        .trigger(processingTime="1 minute")
        .queryName("Q4_TopCountries")
        .start()
    )


# ══════════════════════════════════════════════════════════════════
# MAIN – Chạy tất cả queries song song
# ══════════════════════════════════════════════════════════════════
def main():

    spark = create_spark_session()
    print(" SparkSession khởi động")

    raw      = read_kafka_stream(spark)
    bookings = parse_booking_stream(raw)
    print("Kafka stream kết nối: topic=hotel-bookings")
    print("Schema parsing sẵn sàng\n")

    print("Khởi chạy 4 Streaming Queries song song...")
    q1 = query1_bookings_per_minute(bookings)
    q2 = query2_cancellation_rate(bookings)
    q3 = query3_adr_over_time(bookings)
    q4 = query4_top_countries(bookings)

    print("\n [Q1] Số booking mỗi phút          → console")
    print(" [Q2] Tỷ lệ hủy phòng mỗi phút     → console")
    print(" [Q3] ADR trung bình theo thời gian → console")
    print(" [Q4] Top quốc gia thời gian thực   → console")
    print("\n[RUNNING] Đang lắng nghe Kafka stream... Ctrl+C để dừng\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
