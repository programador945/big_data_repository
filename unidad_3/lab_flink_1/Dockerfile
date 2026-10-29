
FROM flink:1.20.2-scala_2.12

USER root

# --------------------------------------------------
# 1. Instalar Python y herramientas
# --------------------------------------------------

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv curl && \
    rm -rf /var/lib/apt/lists/*

# --------------------------------------------------
# 2. Crear entorno virtual
# --------------------------------------------------

RUN python3 -m venv /opt/pyflink-venv

# --------------------------------------------------
# 3. Instalar PyFlink
# --------------------------------------------------

RUN /opt/pyflink-venv/bin/pip install \
    --default-timeout=300 \
    --retries=10 \
    apache-flink==1.20.2

# --------------------------------------------------
# 4. Instalar conector Kafka
# --------------------------------------------------

RUN mkdir -p /opt/flink/lib && \
    curl -fL \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar \
    -o /opt/flink/lib/flink-connector-kafka-3.3.0-1.20.jar && \
    curl -fL \
    https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar \
    -o /opt/flink/lib/kafka-clients-3.9.0.jar

# --------------------------------------------------
# 5. Configurar Python para Flink
# --------------------------------------------------

ENV PATH="/opt/pyflink-venv/bin:${PATH}"
ENV PYTHON=/opt/pyflink-venv/bin/python
ENV PYFLINK_CLIENT_EXECUTABLE=/opt/pyflink-venv/bin/python

USER flink
