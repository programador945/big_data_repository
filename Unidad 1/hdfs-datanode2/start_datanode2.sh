#!/bin/bash

set -e

# ============================================================
# Iniciar DataNode 2
# Hadoop 3.5.0
# ============================================================

HADOOP_HOME="/opt/hadoop"
DN2_HOME="$HOME/hadoop-datanode2"
DN2_CONF="$DN2_HOME/etc/hadoop"
DN2_PID_DIR="$DN2_HOME/pids"
DN2_LOG_DIR="$DN2_HOME/logs"

export HADOOP_HOME
export HADOOP_CONF_DIR="$DN2_CONF"
export HADOOP_PID_DIR="$DN2_PID_DIR"
export HADOOP_LOG_DIR="$DN2_LOG_DIR"

echo "============================================================"
echo " Iniciando DataNode 2"
echo "============================================================"

# ------------------------------------------------------------
# Comprobar configuración
# ------------------------------------------------------------

if [ ! -d "$DN2_CONF" ]; then
    echo "ERROR: El DataNode 2 no está configurado."
    echo
    echo "Ejecuta primero:"
    echo "  ./create_config_datanode2.sh"
    exit 1
fi

# ------------------------------------------------------------
# Crear directorios necesarios
# ------------------------------------------------------------

mkdir -p "$DN2_PID_DIR"
mkdir -p "$DN2_LOG_DIR"

# ------------------------------------------------------------
# Comprobar puertos
# ------------------------------------------------------------

for PORT in 9869 9871 9872
do
    if ss -lnt 2>/dev/null | grep -q ":$PORT "; then
        echo "ERROR: El puerto $PORT ya está ocupado."
        echo "No se iniciará el DataNode 2."
        exit 1
    fi
done

# ------------------------------------------------------------
# Comprobar si DN2 ya está ejecutándose
# ------------------------------------------------------------

PID_FILE="$DN2_PID_DIR/hadoop-$USER-datanode.pid"

if [ -f "$PID_FILE" ]; then

    PID=$(cat "$PID_FILE")

    if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
        echo "DataNode 2 ya está ejecutándose."
        echo "PID: $PID"
        exit 0
    fi

    echo "El archivo PID existe pero el proceso no está activo."
    echo "Eliminando PID antiguo..."

    rm -f "$PID_FILE"
fi

# ------------------------------------------------------------
# Iniciar DataNode 2
# ------------------------------------------------------------

echo "Iniciando DataNode..."

"$HADOOP_HOME/bin/hdfs" \
    --config "$DN2_CONF" \
    --daemon start datanode

# ------------------------------------------------------------
# Esperar
# ------------------------------------------------------------

echo
echo "Esperando registro del DataNode..."
sleep 5

# ------------------------------------------------------------
# Verificar proceso
# ------------------------------------------------------------

echo
echo "Procesos Java:"
echo "------------------------------------------------------------"

jps

# ------------------------------------------------------------
# Verificar DataNodes en NameNode
# ------------------------------------------------------------

echo
echo "DataNodes registrados:"
echo "------------------------------------------------------------"

hdfs dfsadmin -report

echo
echo "============================================================"
echo " DataNode 2 iniciado."
echo "============================================================"
