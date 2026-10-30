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

ciudades = ["Bogota", "Medellin", "Cali", "Barranquilla", "Cartagena"]
tipos_evento = ["Nacimiento", "Defuncion", "Migracion_Entrante", "Migracion_Saliente"]

def generar_datos_poblacionales():
    return {
        "ciudad": random.choice(ciudades),
        "tipo_evento": random.choice(tipos_evento),
        "cantidad": random.randint(1, 5), # Simulando eventos en bloque para la ciudad
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

print("Iniciando transmisión de datos poblacionales. Presiona Ctrl+C para detener.")

try:
    while True:
        datos = generar_datos_poblacionales()
        producer.send('datos_poblacion', value=datos)
        print(f"Enviado: {datos}")
        time.sleep(1.5) # Simula la llegada de eventos poblacionales
except KeyboardInterrupt:
    print("\nTransmisión detenida.")
finally:
    producer.close()
