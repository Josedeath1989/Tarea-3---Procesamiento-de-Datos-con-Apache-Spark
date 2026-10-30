import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

# Configuración del productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generar_datos_sensor():
    return {
        "sensor_id": random.randint(1, 5),
        "temperatura": round(random.uniform(15.0, 35.0), 2),
        "humedad": round(random.uniform(40.0, 90.0), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

print("Iniciando transmisión de datos climáticos. Presiona Ctrl+C para detener.")

try:
    while True:
        datos = generar_datos_sensor()
        producer.send('sensor_data', value=datos)
        print(f"Enviado: {datos}")
        time.sleep(1) # Simula la llegada de 1 registro por segundo
except KeyboardInterrupt:
    print("\nTransmisión detenida.")
finally:
    producer.close()
