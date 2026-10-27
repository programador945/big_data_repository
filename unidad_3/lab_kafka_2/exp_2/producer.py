
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "ventas"

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

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda venta: json.dumps(
        venta, ensure_ascii=False
    ).encode("utf-8"),
    acks="all"
)

contador = 1

print(f"Productor enviando ventas al tópico '{TOPIC_NAME}'")
print("Intervalo aleatorio: entre 5 y 10 segundos.")
print("Presiona Ctrl+C para detenerlo.\n")

try:
    while True:
        venta = {
            "id_venta": contador,
            "ciudad": random.choice(CIUDADES),
            "valor": random.randint(VALOR_MINIMO, VALOR_MAXIMO),
            "fecha": datetime.now().isoformat(timespec="seconds")
        }

        resultado = producer.send(
            TOPIC_NAME,
            value=venta
        ).get(timeout=10)

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"Venta {contador} enviada | "
            f"Ciudad: {venta['ciudad']} | "
            f"Valor: ${venta['valor']:,} | "
            f"Partición: {resultado.partition} | "
            f"Offset: {resultado.offset}"
        )

        contador += 1

        espera = random.randint(5, 10)
        print(f"Próxima venta en {espera} segundos.\n")
        time.sleep(espera)

except KeyboardInterrupt:
    print("\nProductor detenido.")

finally:
    producer.close()
    print("Conexión con Kafka cerrada.")
