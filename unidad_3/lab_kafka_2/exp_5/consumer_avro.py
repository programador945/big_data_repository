
from io import BytesIO
from fastavro import parse_schema, schemaless_reader
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "ventas"
GROUP_ID = "experimento-avro"

SCHEMA = parse_schema({
    "type": "record",
    "name": "Venta",
    "fields": [
        {"name": "id_venta", "type": "int"},
        {"name": "ciudad", "type": "string"},
        {"name": "valor", "type": "long"},
        {"name": "fecha", "type": "string"}
    ]
})


def es_avro(headers):
    return any(
        nombre == "formato" and valor == b"avro"
        for nombre, valor in (headers or [])
    )


def deserializar_avro(mensaje):
    buffer = BytesIO(mensaje)
    return schemaless_reader(buffer, SCHEMA)


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

total_bytes = 0
total_mensajes = 0

print("Consumidor Avro iniciado. Esperando mensajes...\n")

try:
    for mensaje in consumer:
        if not es_avro(mensaje.headers):
            continue

        tamano = len(mensaje.value)
        venta = deserializar_avro(mensaje.value)

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

    print("\n--- Resumen consumidor Avro ---")
    print(f"Mensajes recibidos: {total_mensajes}")
    print(f"Tamaño total recibido: {total_bytes} bytes")

    if total_mensajes:
        print(
            f"Promedio por mensaje: "
            f"{total_bytes / total_mensajes:.2f} bytes"
        )

finally:
    consumer.close()
