#!/bin/bash

set -e

# ============================================================
# Configuración del DataNode 2
# Hadoop 3.5.0 - VM pseudo-distribuida
# ============================================================

HADOOP_HOME="/opt/hadoop"

DN2_HOME="$HOME/hadoop-datanode2"
DN2_CONF="$DN2_HOME/etc/hadoop"
DN2_PID_DIR="$DN2_HOME/pids"
DN2_LOG_DIR="$DN2_HOME/logs"

DN2_DATA="/opt/hadoop-data/hdfs/datanode2"

echo "============================================================"
echo " Configuración del DataNode 2"
echo "============================================================"

# ------------------------------------------------------------
# 1. Comprobar Hadoop
# ------------------------------------------------------------

if [ ! -d "$HADOOP_HOME" ]; then
    echo "ERROR: No existe el directorio:"
    echo "       $HADOOP_HOME"
    exit 1
fi

if [ ! -x "$HADOOP_HOME/bin/hdfs" ]; then
    echo "ERROR: No se encuentra el ejecutable HDFS:"
    echo "       $HADOOP_HOME/bin/hdfs"
    exit 1
fi

echo "[OK] Hadoop encontrado en $HADOOP_HOME"

# ------------------------------------------------------------
# 2. Crear estructura de directorios
# ------------------------------------------------------------

echo
echo "[1/6] Creando directorios de configuración..."

mkdir -p "$DN2_CONF"
mkdir -p "$DN2_PID_DIR"
mkdir -p "$DN2_LOG_DIR"

echo "[OK] $DN2_HOME"
echo "[OK] $DN2_CONF"
echo "[OK] $DN2_PID_DIR"
echo "[OK] $DN2_LOG_DIR"

# ------------------------------------------------------------
# 3. Copiar core-site.xml
# ------------------------------------------------------------

echo
echo "[2/6] Copiando core-site.xml..."

cp "$HADOOP_HOME/etc/hadoop/core-site.xml" \
   "$DN2_CONF/core-site.xml"

echo "[OK] core-site.xml"

# ------------------------------------------------------------
# 4. Copiar configuración de logs
# ------------------------------------------------------------

echo
echo "[3/6] Configurando logs..."

if [ -f "$HADOOP_HOME/etc/hadoop/log4j.properties" ]; then

    cp "$HADOOP_HOME/etc/hadoop/log4j.properties" \
       "$DN2_CONF/log4j.properties"

    echo "[OK] log4j.properties copiado"

else

    echo "[AVISO] No se encontró log4j.properties"
    echo "        Hadoop puede utilizar su configuración de logs"
    echo "        predeterminada."

fi

# ------------------------------------------------------------
# 5. Crear hdfs-site.xml específico del DataNode 2
# ------------------------------------------------------------

echo
echo "[4/6] Creando hdfs-site.xml del DataNode 2..."

cat > "$DN2_CONF/hdfs-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>

<configuration>

    <!-- =====================================================
         Directorio exclusivo del DataNode 2
         ===================================================== -->

    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file://${DN2_DATA}</value>
    </property>


    <!-- =====================================================
         Puerto de transferencia de bloques
         DataNode 1 utiliza 9866
         ===================================================== -->

    <property>
        <name>dfs.datanode.address</name>
        <value>0.0.0.0:9869</value>
    </property>


    <!-- =====================================================
         Interfaz web del DataNode 2
         DataNode 1 utiliza 9864
         ===================================================== -->

    <property>
        <name>dfs.datanode.http.address</name>
        <value>0.0.0.0:9872</value>
    </property>


    <!-- =====================================================
         Puerto IPC del DataNode 2
         DataNode 1 utiliza 9867
         ===================================================== -->

    <property>
        <name>dfs.datanode.ipc.address</name>
        <value>0.0.0.0:9871</value>
    </property>

</configuration>
EOF

echo "[OK] hdfs-site.xml creado"

# ------------------------------------------------------------
# 6. Crear directorio físico del DataNode 2
# ------------------------------------------------------------

echo
echo "[5/6] Creando directorio de datos..."

sudo mkdir -p "$DN2_DATA"

sudo chown -R "$(whoami):$(id -gn)" "$DN2_DATA"

echo "[OK] Directorio:"
echo "     $DN2_DATA"

# ------------------------------------------------------------
# 7. Verificar puertos
# ------------------------------------------------------------

echo
echo "[6/6] Verificando puertos..."

PORTS_OK=true

for PORT in 9869 9871 9872
do

    if ss -lnt 2>/dev/null | grep -q ":$PORT "; then

        echo "[ERROR] El puerto $PORT ya está ocupado."
        PORTS_OK=false

    else

        echo "[OK] Puerto $PORT disponible."

    fi

done

if [ "$PORTS_OK" = false ]; then

    echo
    echo "============================================================"
    echo " ERROR"
    echo "============================================================"
    echo
    echo "Uno o más puertos necesarios para el DataNode 2"
    echo "ya están siendo utilizados."
    echo
    echo "No se recomienda iniciar el DataNode 2 hasta"
    echo "resolver este conflicto."
    echo

    exit 1
fi

# ------------------------------------------------------------
# Resumen
# ------------------------------------------------------------

echo
echo "============================================================"
echo " DataNode 2 configurado correctamente"
echo "============================================================"

echo
echo "Hadoop:"
echo "  HADOOP_HOME : $HADOOP_HOME"

echo
echo "Configuración:"
echo "  DN2_HOME    : $DN2_HOME"
echo "  DN2_CONF    : $DN2_CONF"

echo
echo "Datos:"
echo "  DN2_DATA    : $DN2_DATA"

echo
echo "Procesos:"
echo "  PID         : $DN2_PID_DIR"
echo "  Logs        : $DN2_LOG_DIR"

echo
echo "Puertos:"
echo "  Transferencia : 9869"
echo "  HTTP          : 9872"
echo "  IPC           : 9871"

echo
echo "============================================================"
echo " Para iniciar el DataNode 2:"
echo
echo "   ./start_datanode2.sh"
echo
echo "============================================================"
