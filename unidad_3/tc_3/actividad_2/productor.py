
import json
import time
import random

from datetime import datetime, timezone
from kafka import KafkaProducer

# ==========================================
# CONFIGURACIÓN DE KAFKA
# ==========================================

BROKER = "localhost:9092"
TOPIC = "vehiculos"

# ==========================================
# CONFIGURACIÓN DE SIMULACIÓN
# ==========================================

NUM_VEHICULOS = 10
INTERVALO_EVENTOS = 1

PROBABILIDAD_FUERA_RANGO = 0.10

# ==========================================
# GEOCERCA - BOGOTÁ
# ==========================================

LAT_MIN = 4.7000
LAT_MAX = 4.7300

LON_MIN = -74.0900
LON_MAX = -74.0500


def crear_productor():

    return KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda valor: json.dumps(
            valor, ensure_ascii=False
        ).encode("utf-8"),
        key_serializer=lambda clave: str(clave).encode("utf-8"),
        acks="all",
    )


def generar_ubicacion_fuera_rango():
    """
    Genera una ubicación claramente fuera de la geocerca.
    Selecciona aleatoriamente una de las cuatro direcciones.
    """

    direccion = random.choice(["NORTE", "SUR", "ESTE", "OESTE"])

    if direccion == "NORTE":
        latitud = random.uniform(LAT_MAX + 0.005, LAT_MAX + 0.015)
        longitud = random.uniform(LON_MIN, LON_MAX)

    elif direccion == "SUR":
        latitud = random.uniform(LAT_MIN - 0.015, LAT_MIN - 0.005)
        longitud = random.uniform(LON_MIN, LON_MAX)

    elif direccion == "ESTE":
        latitud = random.uniform(LAT_MIN, LAT_MAX)
        longitud = random.uniform(LON_MAX + 0.005, LON_MAX + 0.015)

    else:  # OESTE
        latitud = random.uniform(LAT_MIN, LAT_MAX)
        longitud = random.uniform(LON_MIN - 0.015, LON_MIN - 0.005)

    return latitud, longitud


def generar_ubicacion_dentro_rango():
    """
    Genera una ubicación dentro de la geocerca.
    """

    latitud = random.uniform(LAT_MIN + 0.001, LAT_MAX - 0.001)
    longitud = random.uniform(LON_MIN + 0.001, LON_MAX - 0.001)

    return latitud, longitud


def crear_vehiculos():

    vehiculos = {}

    for i in range(1, NUM_VEHICULOS + 1):

        id_vehiculo = f"VEH{i:03d}"

        latitud, longitud = generar_ubicacion_dentro_rango()

        vehiculos[id_vehiculo] = {
            "latitud": latitud,
            "longitud": longitud,
            "velocidad": random.randint(20, 80)
        }

    return vehiculos


def actualizar_ubicacion(vehiculo):
    """
    Actualiza la ubicación y velocidad de un vehículo.

    90%: ubicación dentro de la geocerca.
    10%: ubicación fuera de la geocerca.
    """

    fuera_rango = random.random() < PROBABILIDAD_FUERA_RANGO

    if fuera_rango:
        latitud, longitud = generar_ubicacion_fuera_rango()

    else:
        latitud, longitud = generar_ubicacion_dentro_rango()

    vehiculo["latitud"] = latitud
    vehiculo["longitud"] = longitud

    # Velocidad entre 20 y 80 km/h
    vehiculo["velocidad"] = random.randint(20, 80)

    return fuera_rango


def main():

    productor = crear_productor()
    vehiculos = crear_vehiculos()

    print(f"Productor conectado a Kafka: {BROKER}")
    print(f"Tópico: {TOPIC}")
    print(f"Vehículos: {NUM_VEHICULOS}")
    print("Publicando un vehículo diferente cada segundo.")
    print("Probabilidad fuera de geocerca: 10%")
    print("Velocidades: entre 20 y 80 km/h\n")

    numero_evento = 0

    siguiente_publicacion = time.monotonic()

    try:

        while True:

            # Seleccionar vehículo de manera rotativa
            indice = numero_evento % NUM_VEHICULOS

            id_vehiculo = f"VEH{indice + 1:03d}"

            vehiculo = vehiculos[id_vehiculo]

            # Actualizar ubicación y velocidad
            fuera_rango = actualizar_ubicacion(vehiculo)

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

            estado_ubicacion = (
                "FUERA DE RANGO" if fuera_rango else "DENTRO DE RANGO"
            )

            print(
                f"Evento {numero_evento + 1:03d} | "
                f"Vehículo: {id_vehiculo} | "
                f"Latitud: {evento['latitud']} | "
                f"Longitud: {evento['longitud']} | "
                f"Velocidad: {evento['velocidad']} km/h | "
                f"Ubicación: {estado_ubicacion} | "
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
