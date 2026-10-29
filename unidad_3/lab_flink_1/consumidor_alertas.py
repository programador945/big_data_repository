
from kafka import KafkaConsumer

# --------------------------------------------------
# 1. Configurar consumidor Kafka
# --------------------------------------------------

consumer = KafkaConsumer(
    "alertas",
    bootstrap_servers="localhost:9094",
    group_id="grupo-consumidor-alertas",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda mensaje: mensaje.decode("utf-8")
)

# --------------------------------------------------
# 2. Consumir alertas
# --------------------------------------------------

print("========================================")
print("   CONSUMIDOR DE ALERTAS DE TEMPERATURA")
print("========================================")
print("Esperando alertas de Kafka...\n")

try:

    for mensaje in consumer:

        alerta = mensaje.value

        print("ALERTA RECIBIDA")
        print("------------------------------")
        print(alerta)
        print("------------------------------\n")

except KeyboardInterrupt:

    print("\nConsumidor detenido por el usuario.")

finally:

    consumer.close()
