
import json
import random
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "ventas"
ARCHIVO_DATOS = "ventas_experimento.json"
NUM_VENTAS = 100


def cargar_o_generar_ventas():
    try:
        with open(ARCHIVO_DATOS, "r", encoding="utf-8") as archivo:
            ventas = json.load(archivo)
        print(f"Datos cargados desde {ARCHIVO_DATOS}")
        return ventas

    except FileNotFoundError:
        random.seed(42)

        ciudades = [
            "Bogota", "Medellin", "Cali",
            "Bucaramanga", "Cartagena", "Pereira"
        ]

        fecha_base = datetime(2026, 1, 1, 8, 0, 0)
        ventas = []

        for i in range(1, NUM_VENTAS + 1):
            venta = {
                "id_venta": i,
                "ciudad": random.choice(ciudades),
                "valor": random.randint(10000, 500000),
                "fecha": (
                    fecha_base + timedelta(minutes=i * 5)
                ).isoformat(timespec="seconds")
            }
            ventas.append(venta)

        with open(ARCHIVO_DATOS, "w", encoding="utf-8") as archivo:
            json.dump(
                ventas, archivo,
                ensure_ascii=False, indent=2
            )

        print(f"Se generaron {len(ventas)} ventas.")
        print(f"Datos guardados en {ARCHIVO_DATOS}")
        return ventas


ventas = cargar_o_generar_ventas()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    acks="all"
)

total_bytes = 0

try:
    for venta in ventas:
        mensaje = json.dumps(
            venta, ensure_ascii=False
        ).encode("utf-8")

        tamano = len(mensaje)
        total_bytes += tamano

        producer.send(
            TOPIC_NAME,
            value=mensaje,
            key=str(venta["id_venta"]).encode("utf-8"),
            headers=[("formato", b"json")]
        ).get(timeout=10)

        print(
            f"Venta {venta['id_venta']:03d} | "
            f"Tamaño JSON: {tamano} bytes"
        )

    producer.flush()

    print("\n--- Resumen JSON ---")
    print(f"Mensajes enviados: {len(ventas)}")
    print(f"Tamaño total de los valores: {total_bytes} bytes")
    print(f"Promedio por mensaje: {total_bytes / len(ventas):.2f} bytes")

finally:
    producer.close()
