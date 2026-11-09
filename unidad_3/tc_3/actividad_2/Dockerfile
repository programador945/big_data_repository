
FROM flink:1.20.5-scala_2.12

USER root

# ==========================================
# 1. INSTALAR PYTHON Y HERRAMIENTAS
# ==========================================

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv curl && \
    rm -rf /var/lib/apt/lists/*

# ==========================================
# 2. CREAR ENTORNO VIRTUAL
# ==========================================

RUN python3 -m venv /opt/pyflink-venv

# ==========================================
# 3. INSTALAR PYFLINK
# ==========================================

RUN /opt/pyflink-venv/bin/pip install \
    --default-timeout=300 \
    --retries=10 \
    apache-flink==1.20.5

# ==========================================
# 4. INSTALAR CONECTOR KAFKA
# ==========================================

RUN mkdir -p /opt/flink/lib && \
    curl -fL \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar \
    -o /opt/flink/lib/flink-connector-kafka-3.3.0-1.20.jar && \
    curl -fL \
    https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar \
    -o /opt/flink/lib/kafka-clients-3.9.0.jar

# ==========================================
# 5. CONFIGURAR PYTHON PARA FLINK
# ==========================================

ENV PATH="/opt/pyflink-venv/bin:${PATH}"
ENV PYTHON=/opt/pyflink-venv/bin/python
ENV PYFLINK_CLIENT_EXECUTABLE=/opt/pyflink-venv/bin/python

USER flink
