import json
import logging
import sys

from pyflink.common import (
    SimpleStringSchema,
    WatermarkStrategy,
    Types
)

from pyflink.datastream import StreamExecutionEnvironment

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    FlinkKafkaProducer
)


# ==================================================
# CONFIGURACIÓN GENERAL
# ==================================================

KAFKA_BROKER = "kafka:9092"

TOPIC_ENTRADA = "vehiculos"
TOPIC_ALERTAS = "alertas"

CONSUMER_GROUP = "flink-monitoreo"


# ==================================================
# CONFIGURACIÓN DE GEOCERCA
# ==================================================

LAT_MIN = 4.7000
LAT_MAX = 4.7300

LON_MIN = -74.0900
LON_MAX = -74.0500

VELOCIDAD_MAXIMA = 60


# ==================================================
# CONFIGURACIÓN DE LOGGING
# ==================================================

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ==================================================
# 1. CREAR FUENTE KAFKA
# ==================================================

def crear_fuente_kafka():

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(TOPIC_ENTRADA)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    return source


# ==================================================
# 2. PROCESAR EVENTOS
# ==================================================

def procesar_evento(mensaje):

    try:

        # Convertir el mensaje JSON a un diccionario
        evento = json.loads(mensaje)

        # ------------------------------------------
        # EXTRAER INFORMACIÓN DEL VEHÍCULO
        # ------------------------------------------

        id_vehiculo = evento["id_vehiculo"]

        latitud = float(evento["latitud"])
        longitud = float(evento["longitud"])

        velocidad = int(evento["velocidad"])

        timestamp = evento["timestamp"]

        numero_evento = int(evento["numero_evento"])

        # Lista donde se almacenarán las causas
        # de las alertas detectadas.
        causas = []

        # ==================================================
        # ACTIVIDAD DEL ESTUDIANTE: PROCESAMIENTO
        # ==================================================

        # ------------------------------------------
        # REGLA 1: EXCESO DE VELOCIDAD
        # ------------------------------------------
        #
        # Verifique si la velocidad del vehículo
        # supera el límite establecido.
        #
        # Si se cumple la condición, agregue a la
        # lista causas el texto:
        #
        # "EXCESO_VELOCIDAD"
        #
        # COMPLETAR:
        # --------------------------------------------------


        # ------------------------------------------
        # REGLA 2: FUERA DE GEOCERCA
        # ------------------------------------------
        #
        # Verifique si la ubicación del vehículo
        # está fuera del perímetro permitido.
        #
        # La ubicación debe estar dentro de estos límites:
        #
        # Latitud:  LAT_MIN <= latitud <= LAT_MAX
        # Longitud: LON_MIN <= longitud <= LON_MAX
        #
        # Si está fuera del perímetro, agregue a la
        # lista causas el texto:
        #
        # "FUERA_PERIMETRO"
        #
        # COMPLETAR:
        # --------------------------------------------------


        # ------------------------------------------
        # CLASIFICACIÓN DEL EVENTO
        # ------------------------------------------
        #
        # Si la lista causas contiene al menos una
        # causa, el estado debe ser "ALERTA".
        #
        # De lo contrario, debe ser "NORMAL".
        #
        # Además, construya tipo_alerta uniendo
        # las causas con comas.
        #
        # Si no existen causas, tipo_alerta debe
        # ser None.
        #
        # COMPLETAR:
        # --------------------------------------------------

        estado = "PENDIENTE"
        tipo_alerta = None


        # ==================================================
        # FIN DE LA ACTIVIDAD DEL ESTUDIANTE
        # ==================================================

        # Construir el evento procesado
        evento_procesado = {
            "id_vehiculo": id_vehiculo,
            "latitud": latitud,
            "longitud": longitud,
            "velocidad": velocidad,
            "estado": estado,
            "tipo_alerta": tipo_alerta,
            "timestamp": timestamp,
            "numero_evento": numero_evento
        }

        logging.info(
            "Vehículo: %s | Velocidad: %s km/h | Estado: %s | Causa: %s",
            id_vehiculo,
            velocidad,
            estado,
            tipo_alerta or "NINGUNA"
        )

        return evento_procesado

    except Exception as error:

        logging.error(
            "Error procesando mensaje: %s | Detalle: %s",
            mensaje,
            error
        )

        return None


# ==================================================
# 3. CREAR KAFKA SINK PARA ALERTAS
# ==================================================

def crear_kafka_sink_alertas():

    producer_config = {
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
        "retries": "3"
    }

    kafka_sink = FlinkKafkaProducer(
        topic=TOPIC_ALERTAS,
        serialization_schema=SimpleStringSchema(),
        producer_config=producer_config
    )

    return kafka_sink


# ==================================================
# 4. CONSTRUIR Y EJECUTAR EL JOB
# ==================================================

def main():

    logging.info("========================================")
    logging.info("INICIANDO MONITOREO DE VEHÍCULOS")
    logging.info("========================================")

    # Crear entorno de ejecución de Flink
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)

    # ------------------------------------------
    # FUENTE KAFKA
    # ------------------------------------------

    source = crear_fuente_kafka()

    eventos = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Vehiculos Source"
    )

    # ------------------------------------------
    # PROCESAMIENTO DE EVENTOS
    # ------------------------------------------

    eventos_procesados = (
        eventos
        .map(
            procesar_evento,
            output_type=Types.PICKLED_BYTE_ARRAY()
        )
        .filter(
            lambda evento: evento is not None
        )
    )

    # ------------------------------------------
    # FILTRAR SOLAMENTE ALERTAS
    # ------------------------------------------

    alertas = (
        eventos_procesados
        .filter(
            lambda evento: evento["estado"] == "ALERTA"
        )
        .map(
            lambda evento: json.dumps(
                evento,
                ensure_ascii=False
            ),
            output_type=Types.STRING()
        )
    )

    # ------------------------------------------
    # PUBLICAR ALERTAS EN KAFKA
    # ------------------------------------------

    alertas.add_sink(
        crear_kafka_sink_alertas()
    ).name(
        "Kafka - Alertas"
    )

    # ------------------------------------------
    # EJECUTAR JOB
    # ------------------------------------------

    logging.info("Kafka entrada: %s", TOPIC_ENTRADA)
    logging.info("Kafka salida: %s", TOPIC_ALERTAS)

    logging.info(
        "Regla 1: velocidad superior a %s km/h",
        VELOCIDAD_MAXIMA
    )

    logging.info("Regla 2: ubicación fuera de geocerca")

    env.execute("Monitoreo de Vehiculos")


if __name__ == "__main__":

    main()
