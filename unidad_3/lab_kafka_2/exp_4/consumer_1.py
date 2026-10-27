
import json
import time
from datetime import datetime
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "ventas"
GROUP_ID = "grupo-1"
TIEMPO_PROCESAMIENTO = 2

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

print(f"Consumidor 2 suscrito a '{TOPIC_NAME}'")
print(f"Grupo: {GROUP_ID}")
print(f"Tiempo de procesamiento: {TIEMPO_PROCESAMIENTO} segundos")
print("Esperando ventas... Presiona Ctrl+C para detenerlo.\n")

try:
    for mensaje in consumer:
        venta = mensaje.value

        inicio = datetime.now().strftime("%H:%M:%S")

        print(
            f"\n[{inicio}] Iniciando procesamiento "
            f"de la venta {venta['id_venta']}"
        )

        print(
            f"Ciudad: {venta['ciudad']} | "
            f"Valor: ${venta['valor']:,} | "
            f"Offset: {mensaje.offset}"
        )

        # Simular procesamiento de la venta
        time.sleep(TIEMPO_PROCESAMIENTO)

        fin = datetime.now().strftime("%H:%M:%S")

        print(
            f"[{fin}] Venta {venta['id_venta']} procesada."
        )

except KeyboardInterrupt:
    print("\nConsumidor 2 detenido.")

finally:
    consumer.close()
    print("Conexión con Kafka cerrada.")
