#!/bin/bash

HADOOP_HOME="/opt/hadoop"
DN2_HOME="$HOME/hadoop-datanode2"
DN2_CONF="$DN2_HOME/etc/hadoop"
DN2_PID_DIR="$DN2_HOME/pids"

export HADOOP_HOME
export HADOOP_CONF_DIR="$DN2_CONF"
export HADOOP_PID_DIR="$DN2_PID_DIR"

echo "============================================================"
echo " Estado del DataNode 2"
echo "============================================================"

if [ ! -d "$DN2_CONF" ]; then
    echo
    echo "Estado: NO CONFIGURADO"
    exit 0
fi

echo
echo "Configuración:"
echo "------------------------------------------------------------"

echo "Config : $DN2_CONF"
echo "Datos  : /opt/hadoop-data/hdfs/datanode2"
echo "PID    : $DN2_PID_DIR"

echo
echo "Puertos:"
echo "------------------------------------------------------------"

for PORT in 9869 9871 9872
do
    if ss -lnt 2>/dev/null | grep -q ":$PORT "; then
        echo "Puerto $PORT : ACTIVO"
    else
        echo "Puerto $PORT : INACTIVO"
    fi
done

echo
echo "DataNodes registrados:"
echo "------------------------------------------------------------"

hdfs dfsadmin -report
