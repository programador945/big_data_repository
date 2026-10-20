
import json
from datetime import datetime, timezone

from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "ventas"


def crear_productor():
    return KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda valor: json.dumps(
            valor, ensure_ascii=False
        ).encode("utf-8"),
        key_serializer=lambda clave: str(clave).encode("utf-8"),
        acks="all",
    )


def main():
    productor = crear_productor()
    print(f"Productor conectado a Kafka: {BROKER}")
    print("Escribe una ciudad para publicar una venta.")
    print("Escribe 'salir' para terminar.\n")

    id_venta = 1

    try:
        while True:
            ciudad = input("Ciudad: ").strip()

            if ciudad.lower() == "salir":
                break

            if not ciudad:
                print("La ciudad no puede estar vacía.")
                continue

            try:
                valor = float(input("Valor de la venta: "))
                if valor <= 0:
                    print("El valor debe ser mayor que cero.")
                    continue
            except ValueError:
                print("Introduce un valor numérico válido.")
                continue

            evento = {
                "id_venta": id_venta,
                "ciudad": ciudad,
                "valor": valor,
                "timestamp": datetime.now(
                    timezone.utc
                ).isoformat(),
            }

            try:
                resultado = productor.send(
                    TOPIC,
                    key=id_venta,
                    value=evento,
                ).get(timeout=10)

                print(
                    f"Venta publicada: {evento} | "
                    f"partición={resultado.partition}, "
                    f"offset={resultado.offset}\n"
                )
                id_venta += 1

            except Exception as error:
                print(f"Error publicando el evento: {error}")

    except KeyboardInterrupt:
        print("\nProductor interrumpido.")

    finally:
        productor.flush()
        productor.close()
        print("Productor finalizado.")


if __name__ == "__main__":
    main()