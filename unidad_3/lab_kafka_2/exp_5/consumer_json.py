
import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "ventas"
GROUP_ID = "experimento-json"


def es_json(headers):
    return any(
        nombre == "formato" and valor == b"json"
        for nombre, valor in (headers or [])
    )


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

total_bytes = 0
total_mensajes = 0

print("Consumidor JSON iniciado. Esperando mensajes...\n")

try:
    for mensaje in consumer:
        if not es_json(mensaje.headers):
            continue

        tamano = len(mensaje.value)
        venta = json.loads(mensaje.value.decode("utf-8"))

        total_bytes += tamano
        total_mensajes += 1

        print(
            f"Venta {venta['id_venta']:03d} | "
            f"Ciudad: {venta['ciudad']} | "
            f"Valor: ${venta['valor']:,} | "
            f"Tamaño: {tamano} bytes | "
            f"Partición: {mensaje.partition} | "
            f"Offset: {mensaje.offset}"
        )

        if total_mensajes == 100:
            break

    print("\n--- Resumen consumidor JSON ---")
    print(f"Mensajes recibidos: {total_mensajes}")
    print(f"Tamaño total recibido: {total_bytes} bytes")

    if total_mensajes:
        print(
            f"Promedio por mensaje: "
            f"{total_bytes / total_mensajes:.2f} bytes"
        )

finally:
    consumer.close()
