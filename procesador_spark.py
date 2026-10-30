from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

# 1. Inicializar Spark Session
spark = SparkSession.builder \
    .appName("AnalisisClimaticoTiempoReal") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema de los datos JSON entrantes
esquema_sensor = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperatura", DoubleType(), True),
    StructField("humedad", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 3. Leer el stream de datos desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transformar los datos (Parsear JSON)
df_valores = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), esquema_sensor).alias("datos")) \
    .select("datos.*")

# 5. Procesamiento: Calcular promedios por sensor_id 
df_estadisticas = df_valores \
    .groupBy("sensor_id") \
    .agg(
        avg("temperatura").alias("temp_promedio"),
        avg("humedad").alias("humedad_promedio")
    )

# 6. Visualizar los resultados en tiempo real en la consola (Modo Complete)
query = df_estadisticas.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
