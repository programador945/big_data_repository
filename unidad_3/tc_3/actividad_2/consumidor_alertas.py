
import json

from kafka import KafkaConsumer


# ==========================================
# CONFIGURACIÓN KAFKA
# ==========================================

BROKER = "localhost:9092"
TOPIC = "alertas"

CONSUMER_GROUP = "monitor-alertas"


# ==========================================
# CREAR CONSUMIDOR
# ==========================================

def crear_consumidor():

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda valor: json.loads(
            valor.decode("utf-8")
        )
    )

    return consumer


# ==========================================
# MOSTRAR ALERTA
# ==========================================

def mostrar_alerta(evento):

    print("\n" + "=" * 55)
    print("              ALERTA DE VEHÍCULO")
    print("=" * 55)

    print(f"Vehículo       : {evento['id_vehiculo']}")

    print(f"Número evento  : {evento['numero_evento']}")

    print(f"Fecha y hora   : {evento['timestamp']}")

    print("-" * 55)

    print("INFORMACIÓN DE UBICACIÓN")

    print(f"Latitud        : {evento['latitud']}")
    print(f"Longitud       : {evento['longitud']}")

    print("-" * 55)

    print("INFORMACIÓN DE VELOCIDAD")

    print(f"Velocidad      : {evento['velocidad']} km/h")

    print("-" * 55)

    print("CLASIFICACIÓN DE ALERTA")

    causas = evento.get("tipo_alerta") or ""

    for causa in causas.split(","):

        if causa == "EXCESO_VELOCIDAD":

            print(" - EXCESO DE VELOCIDAD")

        elif causa == "FUERA_PERIMETRO":

            print(" - VEHÍCULO FUERA DE GEOCERCA")

    print("=" * 55)


# ==========================================
# EJECUTAR CONSUMIDOR
# ==========================================

def main():

    consumer = crear_consumidor()

    print("==========================================")
    print("       MONITOR DE ALERTAS VEHICULARES")
    print("==========================================")

    print(f"Broker : {BROKER}")
    print(f"Tópico : {TOPIC}")

    print("\nEsperando alertas...\n")

    try:

        for mensaje in consumer:

            evento = mensaje.value

            mostrar_alerta(evento)

    except KeyboardInterrupt:

        print("\nMonitor interrumpido.")

    finally:

        consumer.close()

        print("Consumidor finalizado.")


if __name__ == "__main__":

    main()
