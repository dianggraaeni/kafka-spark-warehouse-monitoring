import json
import time
import random
from kafka import KafkaProducer

# Konfigurasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    api_version=(0, 11, 5)
)

# Daftar gudang yang akan dimonitor
gudang_list = ['G1', 'G2', 'G3']

def generate_humidity_data():
    """Generate data kelembaban dengan rentang yang realistis"""
    # Kelembaban normal: 40-60%, kelembaban tinggi: 70-85%
    if random.random() < 0.3:  # 30% kemungkinan kelembaban tinggi
        return random.randint(70, 85)
    else:
        return random.randint(40, 60)

try:
    print("💧 Producer Kelembaban dimulai...")
    print("Mengirim data kelembaban setiap detik...")
    
    while True:
        for gudang_id in gudang_list:
            # Generate data kelembaban
            kelembaban_data = {
                "gudang_id": gudang_id,
                "kelembaban": generate_humidity_data(),
                "timestamp": int(time.time())
            }
            
            # Kirim data ke topik Kafka
            producer.send('sensor-kelembaban-gudang', value=kelembaban_data)
            print(f"📤 Sent: {kelembaban_data}")
        
        # Flush untuk memastikan data terkirim
        producer.flush()
        
        # Tunggu 1 detik sebelum mengirim data berikutnya
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Producer Kelembaban dihentikan")
finally:
    producer.close()