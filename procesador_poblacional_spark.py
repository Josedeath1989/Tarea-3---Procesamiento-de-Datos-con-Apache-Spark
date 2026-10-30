from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# 1. Inicializar Spark Session
spark = SparkSession.builder \
    .appName("AnalisisPoblacionalTiempoReal") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema de los datos JSON entrantes
esquema_poblacion = StructType([
    StructField("ciudad", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 3. Leer el stream de datos desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "datos_poblacion") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transformar los datos (Parsear JSON)
df_valores = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), esquema_poblacion).alias("datos")) \
    .select("datos.*")

# 5. Procesamiento: Contar eventos por tipo y ciudad en ventanas de tiempo
df_estadisticas = df_valores \
    .groupBy(
        window(col("timestamp"), "10 seconds", "5 seconds"),
        "ciudad",
        "tipo_evento"
    ) \
    .agg(
        sum("cantidad").alias("total_eventos")
    ) \
    .orderBy("window", "ciudad")

# 6. Visualizar los resultados en tiempo real en la consola (Modo Complete)
query = df_estadisticas.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
