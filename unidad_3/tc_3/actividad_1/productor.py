
import json
import time
import random

from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuración de Kafka
BROKER = "localhost:9092"
TOPIC = "vehiculos"

# Configuración de simulación
NUM_VEHICULOS = 10
INTERVALO_EVENTOS = 1

# Coordenadas iniciales (Bogotá)
LATITUD_BASE = 4.7110
LONGITUD_BASE = -74.0721


def crear_productor():
    return KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda valor: json.dumps(
            valor, ensure_ascii=False
        ).encode("utf-8"),
        key_serializer=lambda clave: str(clave).encode("utf-8"),
        acks="all",
    )


def crear_vehiculos():
    vehiculos = {}

    for i in range(1, NUM_VEHICULOS + 1):

        id_vehiculo = f"VEH{i:03d}"

        vehiculos[id_vehiculo] = {
            "latitud": LATITUD_BASE + random.uniform(-0.01, 0.01),
            "longitud": LONGITUD_BASE + random.uniform(-0.01, 0.01),
            "velocidad": random.randint(20, 60)
        }

    return vehiculos


def actualizar_ubicacion(vehiculo):
    """
    Simula el desplazamiento de un vehículo.
    """

    vehiculo["latitud"] += random.uniform(-0.001, 0.001)
    vehiculo["longitud"] += random.uniform(-0.001, 0.001)
    vehiculo["velocidad"] = random.randint(20, 80)


def main():

    productor = crear_productor()
    vehiculos = crear_vehiculos()

    print(f"Productor conectado a Kafka: {BROKER}")
    print(f"Tópico: {TOPIC}")
    print(f"Vehículos: {NUM_VEHICULOS}")
    print("Publicando un vehículo diferente cada segundo.\n")

    numero_evento = 0

    siguiente_publicacion = time.monotonic()

    try:

        while True:

            # Seleccionar vehículo de manera rotativa
            indice = numero_evento % NUM_VEHICULOS

            id_vehiculo = f"VEH{indice + 1:03d}"

            vehiculo = vehiculos[id_vehiculo]

            # A partir de la segunda publicación de cada vehículo,
            # actualizar su ubicación antes de enviar el evento.
            if numero_evento >= NUM_VEHICULOS:
                actualizar_ubicacion(vehiculo)

            # Construir evento
            evento = {
                "id_vehiculo": id_vehiculo,
                "latitud": round(vehiculo["latitud"], 6),
                "longitud": round(vehiculo["longitud"], 6),
                "velocidad": vehiculo["velocidad"],
                "timestamp": datetime.now(
                    timezone.utc
                ).isoformat(),
                "numero_evento": numero_evento + 1
            }

            # Publicar evento
            resultado = productor.send(
                TOPIC,
                key=id_vehiculo,
                value=evento
            ).get(timeout=10)

            print(
                f"Segundo {numero_evento:03d} | "
                f"Vehículo: {id_vehiculo} | "
                f"Latitud: {evento['latitud']} | "
                f"Longitud: {evento['longitud']} | "
                f"Velocidad: {evento['velocidad']} km/h | "
                f"Partición: {resultado.partition}"
            )

            numero_evento += 1

            # Esperar hasta el siguiente segundo
            siguiente_publicacion += INTERVALO_EVENTOS

            tiempo_espera = (
                siguiente_publicacion - time.monotonic()
            )

            if tiempo_espera > 0:
                time.sleep(tiempo_espera)

    except KeyboardInterrupt:
        print("\nSimulación interrumpida.")

    finally:
        productor.flush()
        productor.close()
        print("Productor finalizado.")


if __name__ == "__main__":
    main()
