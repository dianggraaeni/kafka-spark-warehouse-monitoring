from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Membuat Spark Session
spark = SparkSession.builder \
    .appName("WarehouseMonitoring") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🚀 PySpark Consumer untuk Monitoring Gudang dimulai...")

# Schema untuk data suhu
schema_suhu = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("suhu", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Schema untuk data kelembaban
schema_kelembaban = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("kelembaban", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Baca stream data suhu dari Kafka
df_suhu = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data suhu
df_suhu_parsed = df_suhu.select(
    from_json(col("value").cast("string"), schema_suhu).alias("data")
).select("data.*") \
.withColumn("event_time", to_timestamp(col("timestamp")))

# Baca stream data kelembaban dari Kafka
df_kelembaban = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data kelembaban
df_kelembaban_parsed = df_kelembaban.select(
    from_json(col("value").cast("string"), schema_kelembaban).alias("data")
).select("data.*") \
.withColumn("event_time", to_timestamp(col("timestamp")))

# Filter peringatan suhu tinggi (> 80°C)
df_suhu_warning = df_suhu_parsed.filter(col("suhu") > 80)

# Filter peringatan kelembaban tinggi (> 70%)
df_kelembaban_warning = df_kelembaban_parsed.filter(col("kelembaban") > 70)

# Fungsi untuk menampilkan peringatan suhu
def display_temperature_warning(df, epoch_id):
    print(f"\n🌡️ === PERINGATAN SUHU TINGGI (Batch {epoch_id}) ===")
    for row in df.collect():
        print(f"[Peringatan Suhu Tinggi] Gudang {row.gudang_id}: Suhu {row.suhu}°C")

# Fungsi untuk menampilkan peringatan kelembaban
def display_humidity_warning(df, epoch_id):
    print(f"\n💧 === PERINGATAN KELEMBABAN TINGGI (Batch {epoch_id}) ===")
    for row in df.collect():
        print(f"[Peringatan Kelembaban Tinggi] Gudang {row.gudang_id}: Kelembaban {row.kelembaban}%")

# Join kedua stream berdasarkan gudang_id dan window waktu
df_joined = df_suhu_parsed.alias("suhu") \
    .join(
        df_kelembaban_parsed.alias("kelembaban"),
        expr("""
            suhu.gudang_id = kelembaban.gudang_id AND 
            suhu.event_time >= kelembaban.event_time - interval 10 seconds AND
            suhu.event_time <= kelembaban.event_time + interval 10 seconds
        """),
        "inner"
    ) \
    .select(
        col("suhu.gudang_id").alias("gudang_id"),
        col("suhu.suhu").alias("suhu"),
        col("kelembaban.kelembaban").alias("kelembaban"),
        col("suhu.event_time").alias("event_time")
    )

# Fungsi untuk menampilkan status gabungan
def display_combined_status(df, epoch_id):
    print(f"\n🏭 === STATUS GABUNGAN GUDANG (Batch {epoch_id}) ===")
    
    for row in df.collect():
        gudang_id = row.gudang_id
        suhu = row.suhu
        kelembaban = row.kelembaban
        
        # Tentukan status berdasarkan kondisi
        if suhu > 80 and kelembaban > 70:
            status = "BAHAYA TINGGI! Barang berisiko rusak"
            print(f"[PERINGATAN KRITIS] Gudang {gudang_id}:")
            print(f"  - Suhu: {suhu}°C")
            print(f"  - Kelembaban: {kelembaban}%")
            print(f"  - Status: {status}")
        elif suhu > 80:
            status = "Suhu tinggi, kelembaban normal"
            print(f"Gudang {gudang_id}:")
            print(f"  - Suhu: {suhu}°C")
            print(f"  - Kelembaban: {kelembaban}%")
            print(f"  - Status: {status}")
        elif kelembaban > 70:
            status = "Kelembaban tinggi, suhu aman"
            print(f"Gudang {gudang_id}:")
            print(f"  - Suhu: {suhu}°C")
            print(f"  - Kelembaban: {kelembaban}%")
            print(f"  - Status: {status}")
        else:
            status = "Aman"
            print(f"Gudang {gudang_id}:")
            print(f"  - Suhu: {suhu}°C")
            print(f"  - Kelembaban: {kelembaban}%")
            print(f"  - Status: {status}")

# Mulai streaming queries
query_suhu = df_suhu_warning.writeStream \
    .outputMode("append") \
    .foreachBatch(display_temperature_warning) \
    .trigger(processingTime='5 seconds') \
    .start()

query_kelembaban = df_kelembaban_warning.writeStream \
    .outputMode("append") \
    .foreachBatch(display_humidity_warning) \
    .trigger(processingTime='5 seconds') \
    .start()

query_combined = df_joined.writeStream \
    .outputMode("append") \
    .foreachBatch(display_combined_status) \
    .trigger(processingTime='10 seconds') \
    .start()

print("✅ Streaming queries dimulai...")
print("Tekan Ctrl+C untuk menghentikan...")

try:
    # Tunggu semua query selesai
    query_suhu.awaitTermination()
    query_kelembaban.awaitTermination()
    query_combined.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Menghentikan streaming...")
    query_suhu.stop()
    query_kelembaban.stop()
    query_combined.stop()
    spark.stop()
    print("✅ Streaming dihentikan")