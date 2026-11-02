
from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9094",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

sensores = ["sensor-01", "sensor-02", "sensor-03"]

print("Generando temperaturas...")

try:
    while True:
        sensor = random.choice(sensores)

        # Simula temperaturas entre 20 y 40 grados
        temperatura = round(random.uniform(20, 40), 2)

        mensaje = {
            "sensor": sensor,
            "temperatura": temperatura
        }

        producer.send("mediciones", value=mensaje)

        print("Enviado:", mensaje)

        producer.flush()

        time.sleep(1)

except KeyboardInterrupt:
    print("Productor detenido")

finally:
    producer.close()
