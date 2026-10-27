
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "sensor"

SENSORES = ["SENSOR-001", "SENSOR-002", "SENSOR-003"]
CIUDADES = ["Bogota", "Medellin", "Cali", "Bucaramanga"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda dato: json.dumps(
        dato, ensure_ascii=False
    ).encode("utf-8"),
    key_serializer=lambda clave: clave.encode("utf-8"),
    acks="all"
)

contador = 1

print(f"Simulador de sensores publicando en '{TOPIC_NAME}'")
print("Presiona Ctrl+C para detenerlo.\n")

try:
    while True:
        sensor = random.choice(SENSORES)

        medicion = {
            "id_medicion": contador,
            "sensor_id": sensor,
            "ciudad": random.choice(CIUDADES),
            "temperatura": round(random.uniform(18.0, 35.0), 2),
            "humedad": round(random.uniform(30.0, 90.0), 2),
            "fecha": datetime.now().isoformat(timespec="seconds")
        }

        resultado = producer.send(
            TOPIC_NAME,
            key=sensor,
            value=medicion
        ).get(timeout=10)

        print(
            f"[{medicion['fecha']}] "
            f"{sensor} | "
            f"Ciudad: {medicion['ciudad']} | "
            f"Temperatura: {medicion['temperatura']} °C | "
            f"Humedad: {medicion['humedad']} % | "
            f"Particion: {resultado.partition} | "
            f"Offset: {resultado.offset}"
        )

        contador += 1

        espera = random.randint(2, 5)
        time.sleep(espera)

except KeyboardInterrupt:
    print("\nSimulador de sensores detenido.")

finally:
    producer.close()
    print("Conexion con Kafka cerrada.")
