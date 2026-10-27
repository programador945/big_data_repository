
import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "sensor"
GROUP_ID = "grupo-sensor"

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

print(f"Consumidor suscrito a '{TOPIC_NAME}'")
print(f"Grupo: {GROUP_ID}")
print("Esperando mediciones... Presiona Ctrl+C para detenerlo.\n")

try:
    for mensaje in consumer:
        medicion = mensaje.value

        print(
            f"Medicion {medicion['id_medicion']} | "
            f"Sensor: {medicion['sensor_id']} | "
            f"Ciudad: {medicion['ciudad']} | "
            f"Temperatura: {medicion['temperatura']} °C | "
            f"Humedad: {medicion['humedad']} % | "
            f"Particion: {mensaje.partition} | "
            f"Offset: {mensaje.offset}"
        )

except KeyboardInterrupt:
    print("\nConsumidor de mediciones detenido.")

finally:
    consumer.close()
    print("Conexion con Kafka cerrada.")
