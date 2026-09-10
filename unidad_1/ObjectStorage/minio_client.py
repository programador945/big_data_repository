import argparse
from pathlib import Path
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError


class MinioClient:
    """
    Cliente para interactuar con MinIO utilizando la API S3.
    """

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region_name: str = "us-east-1",
    ):
        self.client: BaseClient = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
        )

    def test_connection(self) -> bool:
        """
        Verifica la conexión con MinIO.
        """

        try:
            self.client.list_buckets()

            print("✓ Conexión exitosa con MinIO")

            return True

        except Exception as e:

            print(f"✗ Error conectando con MinIO: {e}")

            return False

    def get_bucket_info(
        self,
        bucket: str,
    ) -> dict[str, Any]:
        """
        Obtiene información de un bucket.
        """

        try:

            self.client.head_bucket(
                Bucket=bucket
            )

            response = self.client.list_objects_v2(
                Bucket=bucket
            )

            objects = []

            for obj in response.get("Contents", []):

                objects.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                })

            result = {
                "bucket": bucket,
                "exists": True,
                "object_count": len(objects),
                "objects": objects,
            }

            print(f"\nBucket: {bucket}")
            print(f"Objetos: {len(objects)}")

            for obj in objects:

                print(
                    f"  {obj['key']} "
                    f"({obj['size']} bytes)"
                )

            return result

        except ClientError as e:

            print(
                f"✗ Error consultando bucket "
                f"'{bucket}': {e}"
            )

            return {
                "bucket": bucket,
                "exists": False,
                "objects": [],
                "object_count": 0,
            }

    def get_file(
        self,
        bucket: str,
        key: str,
        local_path: str,
    ) -> bool:
        """
        Descarga un objeto desde MinIO.

        Si local_path es un directorio, utiliza el nombre
        original del archivo.
        """

        path = Path(local_path)

        # Si se proporciona un directorio, usar el nombre del objeto
        if path.is_dir() or local_path == ".":
            path = path / Path(key).name

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        try:
            self.client.download_file(
                Bucket=bucket,
                Key=key,
                Filename=str(path),
            )

            print(f"✓ Archivo descargado: {path}")

            return True

        except Exception as e:

            print(f"✗ Error descargando archivo: {e}")

            return False

    def set_file(
        self,
        bucket: str,
        key: str,
        local_path: str,
    ) -> bool:
        """
        Sube un archivo a MinIO.
        """

        path = Path(local_path)

        if not path.exists():

            print(
                f"✗ No existe el archivo: "
                f"{local_path}"
            )

            return False

        try:

            self.client.upload_file(
                Filename=str(path),
                Bucket=bucket,
                Key=key,
            )

            print(
                f"✓ Archivo subido correctamente"
            )

            print(f"  Bucket: {bucket}")
            print(f"  Key:    {key}")
            print(f"  File:   {local_path}")

            return True

        except Exception as e:

            print(
                f"✗ Error subiendo archivo: {e}"
            )

            return False


# ==========================================================
# CONFIGURACIÓN
# ==========================================================

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "minioadmin123"


# ==========================================================
# CLI
# ==========================================================

def main():

    parser = argparse.ArgumentParser(
        description="Cliente CLI para MinIO"
    )

    subparsers = parser.add_subparsers(
        dest="command"
    )

    # ------------------------------------------------------
    # test-connection
    # ------------------------------------------------------

    subparsers.add_parser(
        "test-connection",
        help="Prueba la conexión con MinIO",
    )

    # ------------------------------------------------------
    # get-bucket-info
    # ------------------------------------------------------

    bucket_parser = subparsers.add_parser(
        "get-bucket-info",
        help="Obtiene información de un bucket",
    )

    bucket_parser.add_argument(
        "bucket",
        help="Nombre del bucket",
    )

    # ------------------------------------------------------
    # set-file
    # ------------------------------------------------------

    upload_parser = subparsers.add_parser(
        "set-file",
        help="Sube un archivo a MinIO",
    )

    upload_parser.add_argument(
        "bucket",
        help="Nombre del bucket",
    )

    upload_parser.add_argument(
        "key",
        help="Key que tendrá el objeto",
    )

    upload_parser.add_argument(
        "local_path",
        help="Ruta del archivo local",
    )

    # ------------------------------------------------------
    # get-file
    # ------------------------------------------------------

    download_parser = subparsers.add_parser(
        "get-file",
        help="Descarga un archivo desde MinIO",
    )

    download_parser.add_argument(
        "bucket",
        help="Nombre del bucket",
    )

    download_parser.add_argument(
        "key",
        help="Key del objeto",
    )

    download_parser.add_argument(
        "local_path",
        help="Ruta donde guardar el archivo",
    )

    # ------------------------------------------------------
    # Procesar argumentos
    # ------------------------------------------------------

    args = parser.parse_args()

    if not args.command:

        parser.print_help()

        return

    minio = MinioClient(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    )

    # ------------------------------------------------------
    # Ejecutar comando
    # ------------------------------------------------------

    if args.command == "test-connection":

        minio.test_connection()

    elif args.command == "get-bucket-info":

        minio.get_bucket_info(
            args.bucket
        )

    elif args.command == "set-file":

        minio.set_file(
            bucket=args.bucket,
            key=args.key,
            local_path=args.local_path,
        )

    elif args.command == "get-file":

        minio.get_file(
            bucket=args.bucket,
            key=args.key,
            local_path=args.local_path,
        )


if __name__ == "__main__":
    main()