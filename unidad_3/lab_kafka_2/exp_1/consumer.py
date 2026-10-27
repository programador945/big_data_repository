
import json
from kafka import KafkaConsumer

# Configuración
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "ventas"
GROUP_ID = "grupo-ventas"

# Crear consumidor Kafka
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda mensaje: json.loads(
        mensaje.decode("utf-8")
    )
)

print(f"Consumidor suscrito al tópico '{TOPIC_NAME}'")
print(f"Grupo de consumidores: {GROUP_ID}")
print("Esperando mensajes... Presiona Ctrl+C para detenerlo.\n")

try:
    for mensaje in consumer:
        venta = mensaje.value

        print(
            f"Venta recibida | "
            f"ID: {venta['id_venta']} | "
            f"Ciudad: {venta['ciudad']} | "
            f"Valor: ${venta['valor']:,} | "
            f"Fecha: {venta['fecha']} | "
            f"Partición: {mensaje.partition} | "
            f"Offset: {mensaje.offset}"
        )

except KeyboardInterrupt:
    print("\nConsumidor detenido por el usuario.")

finally:
    consumer.close()
    print("Conexion con Kafka cerrada.")
