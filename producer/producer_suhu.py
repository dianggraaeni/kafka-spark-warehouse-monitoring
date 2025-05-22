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

def generate_temperature_data():
    """Generate data suhu dengan rentang yang realistis"""
    # Suhu normal: 15-25°C, suhu tinggi: 80-90°C
    if random.random() < 0.3:  # 30% kemungkinan suhu tinggi
        return random.randint(80, 90)
    else:
        return random.randint(15, 25)

try:
    print("🌡️  Producer Suhu dimulai...")
    print("Mengirim data suhu setiap detik...")
    
    while True:
        for gudang_id in gudang_list:
            # Generate data suhu
            suhu_data = {
                "gudang_id": gudang_id,
                "suhu": generate_temperature_data(),
                "timestamp": int(time.time())
            }
            
            # Kirim data ke topik Kafka
            producer.send('sensor-suhu-gudang', value=suhu_data)
            print(f"📤 Sent: {suhu_data}")
        
        # Flush untuk memastikan data terkirim
        producer.flush()
        
        # Tunggu 1 detik sebelum mengirim data berikutnya
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Producer Suhu dihentikan")
finally:
    producer.close()