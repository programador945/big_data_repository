import requests
from urllib.parse import urlparse, urlunparse


# ============================================================
# CONFIGURACIÓN
# ============================================================

ARCHIVO_HDFS = "/datos/bloques/ventas_10mb.csv"
ARCHIVO_LOCAL = "ventas_recuperado.csv"

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
# 1. SOLICITAR EL ARCHIVO AL NAMENODE
# ============================================================

url = f"{NAMENODE}/webhdfs/v1{ARCHIVO_HDFS}"

params = {
    "op": "OPEN",
    "user.name": USUARIO_HDFS
}


respuesta = requests.get(
    url,
    params=params,
    allow_redirects=False
)

print("Respuesta del NameNode:", respuesta.status_code)


# ============================================================
# 2. OBTENER LA DIRECCIÓN DEL DATANODE
# ============================================================

if respuesta.status_code == 307:

    location = respuesta.headers["Location"]

    print("\nDataNode indicado por Hadoop:")
    print(location)


    # ========================================================
    # 3. CAMBIAR IP DEL DATANODE POR LOCALHOST
    # ========================================================

    location = cambiar_host(location)

    print("\nDataNode utilizado desde Windows:")
    print(location)


    # ========================================================
    # 4. DESCARGAR EL ARCHIVO
    # ========================================================

    print("\nDescargando archivo...")

    respuesta = requests.get(
        location,
        stream=True
    )


    if respuesta.status_code == 200:

        with open(ARCHIVO_LOCAL, "wb") as archivo:

            for bloque in respuesta.iter_content(
                chunk_size=1024 * 1024
            ):

                if bloque:
                    archivo.write(bloque)


        print("\n✓ Archivo recuperado correctamente")
        print(f"✓ Archivo local: {ARCHIVO_LOCAL}")


    else:

        print("\n✗ Error al descargar el archivo")
        print(respuesta.text)


else:

    print("\n✗ Error al solicitar el archivo al NameNode")
    print(respuesta.text)