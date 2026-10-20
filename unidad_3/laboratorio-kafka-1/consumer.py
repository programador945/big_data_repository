
import json

from kafka import KafkaConsumer

BROKER = "localhost:9092"
TOPIC = "ventas"


def main():
    consumidor = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id="laboratorio-ventas",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda mensaje: json.loads(
            mensaje.decode("utf-8")
        ),
        key_deserializer=lambda clave: (
            clave.decode("utf-8") if clave else None
        ),
    )

    print(f"Consumidor conectado a Kafka: {BROKER}")
    print(f"Esperando eventos del topic '{TOPIC}'...\n")

    try:
        for mensaje in consumidor:
            venta = mensaje.value

            print("=" * 45)
            print(f"ID de venta: {venta['id_venta']}")
            print(f"Ciudad:      {venta['ciudad']}")
            print(f"Valor:       {venta['valor']:.2f}")
            print(f"Fecha:       {venta['timestamp']}")
            print(f"Partición:   {mensaje.partition}")
            print(f"Offset:      {mensaje.offset}")

    except KeyboardInterrupt:
        print("\nConsumidor interrumpido.")

    finally:
        consumidor.close()
        print("Consumidor finalizado.")


if __name__ == "__main__":
    main()