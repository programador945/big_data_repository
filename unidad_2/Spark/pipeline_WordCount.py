from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("PipelineWordCount")
    .getOrCreate()
)

ruta_entrada = "hdfs:///spark_hadoop/palabras.txt"
ruta_salida = "hdfs:///spark_hadoop/salidas/wordcount_parquet"

try:
    # 1. Leer el archivo de texto desde HDFS
    lineas = spark.read.text(ruta_entrada)

    # 2. Separar cada línea en palabras
    palabras = (
        lineas
        .select(
            F.explode(
                F.split(F.col("value"), r"\s+")
            ).alias("palabra")
        )
    )

    # 3. Limpiar y normalizar las palabras
    palabras_limpias = (
        palabras
        .select(
            F.lower(
                F.regexp_replace(
                    F.col("palabra"),
                    r"^[^\w]+|[^\w]+$",
                    ""
                )
            ).alias("palabra")
        )
        .filter(F.col("palabra") != "")
    )

    # 4. Contar las apariciones de cada palabra
    resultado = (
        palabras_limpias
        .groupBy("palabra")
        .agg(F.count("*").alias("frecuencia"))
        .orderBy(F.desc("frecuencia"), F.asc("palabra"))
    )

    # 5. Mostrar una muestra en los logs
    print("Resultado del WordCount:")
    resultado.show(50, truncate=False)

    # 6. Guardar el resultado en Parquet en HDFS
    resultado.write \
        .mode("overwrite") \
        .parquet(ruta_salida)

    print("Pipeline WordCount finalizado correctamente.")
    print("Salida:", ruta_salida)

finally:
    spark.stop()
