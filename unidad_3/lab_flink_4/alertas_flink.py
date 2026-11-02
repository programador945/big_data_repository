
import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee
)

# --------------------------------------------------
# 1. Crear entorno de ejecución
# --------------------------------------------------

env = StreamExecutionEnvironment.get_execution_environment()

env.set_parallelism(1)

# --------------------------------------------------
# 2. Configurar Kafka Source
# --------------------------------------------------

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("mediciones")
    .set_group_id("grupo-alertas")
    .set_starting_offsets(
        KafkaOffsetsInitializer.earliest()
    )
    .set_value_only_deserializer(
        SimpleStringSchema()
    )
    .build()
)

# --------------------------------------------------
# 3. Leer mensajes de Kafka
# --------------------------------------------------

mensajes = env.from_source(
    source,
    watermark_strategy=WatermarkStrategy.no_watermarks(),
    source_name="Kafka mediciones"
)

# --------------------------------------------------
# 4. Convertir JSON
# --------------------------------------------------

def convertir_json(mensaje):

    datos = json.loads(mensaje)

    sensor = datos["sensor"]
    temperatura = float(datos["temperatura"])

    return (
        sensor,
        temperatura
    )


temperaturas = mensajes.map(
    convertir_json,
    output_type=Types.TUPLE([
        Types.STRING(),
        Types.FLOAT()
    ])
)

# --------------------------------------------------
# 5. Filtrar temperaturas críticas
# --------------------------------------------------

umbral = 30.0

alertas = temperaturas.filter(
    lambda dato: dato[1] > umbral
)

# --------------------------------------------------
# 6. Generar alerta en formato JSON
# --------------------------------------------------

def generar_alerta(dato):

    sensor, temperatura = dato

    alerta = {
        "sensor": sensor,
        "temperatura": temperatura,
        "umbral": umbral,
        "tipo": "ALERTA",
        "mensaje": (
            f"{sensor} registró "
            f"{temperatura} °C"
        )
    }

    return json.dumps(
        alerta,
        ensure_ascii=False
    )


mensajes_alerta = alertas.map(
    generar_alerta,
    output_type=Types.STRING()
)

# --------------------------------------------------
# 7. Configurar Kafka Sink
# --------------------------------------------------

kafka_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("alertas")
        .set_value_serialization_schema(
            SimpleStringSchema()
        )
        .build()
    )
    .set_delivery_guarantee(
        DeliveryGuarantee.AT_LEAST_ONCE
    )
    .build()
)

# --------------------------------------------------
# 8. Publicar alertas en Kafka
# --------------------------------------------------

mensajes_alerta.sink_to(kafka_sink)

# --------------------------------------------------
# 9. Ejecutar job
# --------------------------------------------------

env.execute("Alertas de temperatura")
