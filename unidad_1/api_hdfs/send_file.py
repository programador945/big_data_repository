import requests
from urllib.parse import urlparse, urlunparse


# ============================================================
# CONFIGURACIÓN
# ============================================================

ARCHIVO_LOCAL = "ventas_10mb.csv"
ARCHIVO_HDFS = "/datos/bloques/ventas_10mb.csv"

NAMENODE = "http://localhost:9870"
USUARIO_HDFS = "hadoop"


# ============================================================
# REEMPLAZAR EL HOST DEL DATANODE POR LOCALHOST
# ============================================================

def cambiar_host(url, nuevo_host="localhost"):

    partes = urlparse(url)

    return urlunparse((
        partes.scheme,
        f"{nuevo_host}:{partes.port}",
        partes.path,
        partes.params,
        partes.query,
        partes.fragment
    ))


# ============================================================
# ENVIAR ARCHIVO A HDFS
# ============================================================

url = f"{NAMENODE}/webhdfs/v1{ARCHIVO_HDFS}"

params = {
    "op": "CREATE",
    "overwrite": "true",
    "user.name": USUARIO_HDFS
}


# ------------------------------------------------------------
# 1. Solicitar al NameNode la creación del archivo
# ------------------------------------------------------------

respuesta = requests.put(
    url,
    params=params,
    allow_redirects=False
)

print("Respuesta del NameNode:", respuesta.status_code)


if respuesta.status_code == 307:

    # --------------------------------------------------------
    # 2. Obtener dirección del DataNode
    # --------------------------------------------------------

    location = respuesta.headers["Location"]

    print("\nDataNode indicado por Hadoop:")
    print(location)


    # --------------------------------------------------------
    # 3. Cambiar la IP del DataNode por localhost
    # --------------------------------------------------------

    location = cambiar_host(location)

    print("\nDataNode utilizado desde Windows:")
    print(location)


    # --------------------------------------------------------
    # 4. Enviar el archivo al DataNode
    # --------------------------------------------------------

    print("\nEnviando archivo...")

    with open(ARCHIVO_LOCAL, "rb") as archivo:

        respuesta = requests.put(
            location,
            data=archivo,
            headers={
                "Content-Type": "application/octet-stream"
            }
        )


    # --------------------------------------------------------
    # 5. Verificar resultado
    # --------------------------------------------------------

    print("\nRespuesta del DataNode:", respuesta.status_code)

    if respuesta.status_code == 201:

        print("✓ Archivo enviado correctamente a HDFS")
        print(f"✓ Ubicación: {ARCHIVO_HDFS}")

    else:

        print("✗ Error al enviar el archivo")
        print(respuesta.text)


else:

    print("✗ Error al solicitar la creación del archivo")
    print(respuesta.text)