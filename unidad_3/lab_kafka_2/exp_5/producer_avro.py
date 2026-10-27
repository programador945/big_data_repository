
import json
from io import BytesIO
from fastavro import parse_schema, schemaless_writer
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "ventas"
ARCHIVO_DATOS = "ventas_experimento.json"

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


def serializar_avro(venta):
    buffer = BytesIO()
    schemaless_writer(buffer, SCHEMA, venta)
    return buffer.getvalue()


with open(ARCHIVO_DATOS, "r", encoding="utf-8") as archivo:
    ventas = json.load(archivo)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    acks="all"
)

total_bytes = 0

try:
    for venta in ventas:
        mensaje = serializar_avro(venta)
        tamano = len(mensaje)
        total_bytes += tamano

        producer.send(
            TOPIC_NAME,
            value=mensaje,
            key=str(venta["id_venta"]).encode("utf-8"),
            headers=[("formato", b"avro")]
        ).get(timeout=10)

        print(
            f"Venta {venta['id_venta']:03d} | "
            f"Tamaño Avro: {tamano} bytes"
        )

    producer.flush()

    print("\n--- Resumen Avro ---")
    print(f"Mensajes enviados: {len(ventas)}")
    print(f"Tamaño total de los valores: {total_bytes} bytes")
    print(f"Promedio por mensaje: {total_bytes / len(ventas):.2f} bytes")

finally:
    producer.close()
