from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Crear la sesión de Spark
# El master se especificará con spark-submit
spark = (
    SparkSession.builder
    .appName("PipelineClientes")
    .getOrCreate()
)

# 2. Definir las rutas de HDFS
ruta_entrada = "hdfs:///spark_hadoop/clientes_1000.csv"
ruta_salida = "hdfs:///spark_hadoop/salidas/resumen_clientes_parquet"

try:
    # 3. Leer el CSV desde HDFS
    clientes = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ruta_entrada)
    )

    print("Esquema de los datos:")
    clientes.printSchema()

    print("Cantidad de registros:", clientes.count())

    # 4. Transformar los datos
    clientes_limpios = (
        clientes
        .withColumn(
            "fecha_nacimiento",
            F.to_date("fecha_nacimiento", "yyyy-MM-dd")
        )
        .filter(F.col("fecha_nacimiento").isNotNull())
        .filter(F.col("departamento").isNotNull())
        .withColumn(
            "edad",
            F.floor(
                F.months_between(
                    F.current_date(),
                    F.col("fecha_nacimiento")
                ) / 12
            )
        )
    )

    # 5. Agrupar por departamento
    resumen = (
        clientes_limpios
        .groupBy("departamento")
        .agg(
            F.count("*").alias("cantidad_clientes"),
            F.round(F.avg("edad"), 2).alias("edad_promedio"),
            F.round(F.min("edad"), 2).alias("edad_minima"),
            F.round(F.max("edad"), 2).alias("edad_maxima")
        )
        .orderBy(F.desc("cantidad_clientes"))
    )

    print("Resumen por departamento:")
    resumen.show(truncate=False)

    # 6. Guardar el resultado en Parquet en HDFS
    resumen.write \
        .mode("overwrite") \
        .parquet(ruta_salida)

    print("Pipeline finalizado correctamente.")
    print("Resultado guardado en:", ruta_salida)

finally:
    # 7. Cerrar la sesión
    spark.stop()