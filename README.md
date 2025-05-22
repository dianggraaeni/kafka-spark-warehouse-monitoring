# 🏭 Kafka-Spark Warehouse Monitoring

Proyek monitoring gudang real-time menggunakan Apache Kafka dan PySpark untuk memantau sensor suhu dan kelembaban.

## 📁 Struktur Proyek

```
kafka-spark-warehouse-monitoring/
├── docker-compose.yml
├── producer/
│   ├── producer_suhu.py
│   └── producer_kelembaban.py
└── consumer/
    └── pyspark_consumer.py
```

## 🚀 Cara Menjalankan

### 1. Setup Kafka dan Zookeeper

```bash
# Clone/buat folder proyek
mkdir kafka-spark-warehouse-monitoring
cd kafka-spark-warehouse-monitoring

# Buat folder struktur
mkdir producer consumer

# Copy semua file ke folder masing-masing

# Jalankan semua container
docker-compose up -d
```

### 2. Buat Topik Kafka

```bash
# Masuk ke container Kafka
docker exec -it kafka bash

# Buat topik sensor-suhu-gudang
kafka-topics --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Buat topik sensor-kelembaban-gudang
kafka-topics --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Verifikasi topik sudah dibuat
kafka-topics --list --bootstrap-server localhost:9092

# Keluar dari container
exit
```

### 3. Simulasikan Data Sensor (Producer Kafka)

**Terminal 1 - Jalankan Producer Suhu:**
```bash
# Masuk ke container producer
docker exec -it producer bash

# Install dependensi
pip install kafka-python

# Jalankan producer suhu
python producer/producer_suhu.py
```

**Terminal 2 - Jalankan Producer Kelembaban:**
```bash
# Masuk ke container producer (terminal baru)
docker exec -it producer bash

# Install dependensi (jika belum)
pip install kafka-python

# Jalankan producer kelembaban
python producer/producer_kelembaban.py
```

### 4. Konsumsi dan Olah Data dengan PySpark

**Terminal 3 - Jalankan PySpark Consumer:**
```bash
# Masuk ke container spark
docker exec -it spark bash

# Install dependensi
pip install kafka-python

# Jalankan PySpark consumer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 consumer/pyspark_consumer.py
```

## 📊 Output yang Diharapkan

### Peringatan Individual:
```
🌡️ === PERINGATAN SUHU TINGGI ===
[Peringatan Suhu Tinggi] Gudang G2: Suhu 85°C

💧 === PERINGATAN KELEMBABAN TINGGI ===
[Peringatan Kelembaban Tinggi] Gudang G3: Kelembaban 74%
```

### Status Gabungan:
```
🏭 === STATUS GABUNGAN GUDANG ===
[PERINGATAN KRITIS] Gudang G1:
  - Suhu: 84°C
  - Kelembaban: 73%
  - Status: BAHAYA TINGGI! Barang berisiko rusak

Gudang G2:
  - Suhu: 78°C
  - Kelembaban: 68%
  - Status: Aman

Gudang G3:
  - Suhu: 85°C
  - Kelembaban: 65%
  - Status: Suhu tinggi, kelembaban normal
```

## 🔧 Konfigurasi

- **Suhu Normal**: 15-25°C
- **Suhu Tinggi**: > 80°C
- **Kelembaban Normal**: 40-60%
- **Kelembaban Tinggi**: > 70%
- **Window Join**: 10 detik
- **Gudang yang Dimonitor**: G1, G2, G3

## 🛑 Menghentikan Aplikasi

```bash
# Hentikan semua producer/consumer dengan Ctrl+C di masing-masing terminal

# Hentikan semua container
docker-compose down
```

## 📝 Catatan

- Data sensor digenerate secara random dengan probabilitas 30% untuk kondisi abnormal
- Stream processing menggunakan micro-batch dengan interval 5-10 detik
- Join antar stream menggunakan time window untuk menghindari data loss
- Semua log dan peringatan ditampilkan di console untuk monitoring real-time
