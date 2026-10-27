
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configuración
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "ventas"
INTERVALO_SEGUNDOS = 5

CIUDADES = [
    "Bogota",
    "Medellin",
    "Cali",
    "Bucaramanga",
    "Barranquilla",
    "Cartagena",
    "Pereira",
    "Manizales"
]

VALOR_MINIMO = 10000
VALOR_MAXIMO = 500000

# Crear productor Kafka
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda venta: json.dumps(
        venta, ensure_ascii=False
    ).encode("utf-8"),
    acks="all"
)

contador = 1

print(f"Productor iniciado. Enviando ventas a '{TOPIC_NAME}'...")
print("Presiona Ctrl+C para detenerlo.\n")

try:
    while True:
        venta = {
            "id_venta": contador,
            "ciudad": random.choice(CIUDADES),
            "valor": random.randint(VALOR_MINIMO, VALOR_MAXIMO),
            "fecha": datetime.now().isoformat(timespec="seconds")
        }

        # Publicar venta en Kafka
        future = producer.send(TOPIC_NAME, value=venta)
        resultado = future.get(timeout=10)

        print(
            f"Venta enviada: {venta} | "
            f"Particion: {resultado.partition} | "
            f"Offset: {resultado.offset}"
        )

        contador += 1
        time.sleep(INTERVALO_SEGUNDOS)

except KeyboardInterrupt:
    print("\nProductor detenido por el usuario.")

finally:
    producer.flush()
    producer.close()
    print("Conexion con Kafka cerrada.")
