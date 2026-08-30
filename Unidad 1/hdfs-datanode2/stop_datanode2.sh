#!/bin/bash

# ============================================================
# Detener DataNode 2
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
echo " Deteniendo DataNode 2"
echo "============================================================"

if [ ! -d "$DN2_CONF" ]; then
    echo "ERROR: No existe la configuración del DataNode 2."
    exit 1
fi

PID_FILE="$DN2_PID_DIR/hadoop-$USER-datanode.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "No se encontró el PID del DataNode 2."
    echo "Puede que ya esté detenido."
    exit 0
fi

PID=$(cat "$PID_FILE")

echo "PID encontrado: $PID"
echo "Deteniendo DataNode 2..."

"$HADOOP_HOME/bin/hdfs" \
    --config "$DN2_CONF" \
    --daemon stop datanode

sleep 3

echo
echo "Procesos Java:"
echo "------------------------------------------------------------"

jps

echo
echo "DataNodes registrados:"
echo "------------------------------------------------------------"

hdfs dfsadmin -report

echo
echo "============================================================"
echo " DataNode 2 detenido."
echo "============================================================"
