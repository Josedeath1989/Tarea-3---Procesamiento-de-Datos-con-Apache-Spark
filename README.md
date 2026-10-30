🚀 Procesamiento de Datos Masivos: Apache Spark & Kafka

Tarea 3 - Curso de Big Data

Implementación de una arquitectura de datos híbrida (Batch & Streaming) para la captura, procesamiento y análisis de eventos en tiempo real.

📖 Tabla de Contenidos

Descripción del Proyecto

Arquitectura del Sistema

Requisitos Previos

Estructura del Repositorio

Ejecución Paso a Paso

Resultados Esperados

Autor

💡 Descripción del Proyecto

Este repositorio contiene la solución práctica a la necesidad de procesar grandes volúmenes de datos. Se aborda la creación de un flujo de Procesamiento en Tiempo Real (Streaming) simulando la llegada continua de datos (climáticos/poblacionales) mediante el desarrollo de aplicaciones que consumen y transforman información al vuelo.

Características principales:

📡 Ingesta de eventos escalable y tolerante a fallos.

⚡ Análisis computacional en memoria con evaluación perezosa.

🪟 Agrupación temporal (Windowing) para analítica en vivo.

🏗 Arquitectura del Sistema

El flujo de datos sigue un modelo de Productor-Consumidor altamente desacoplado:

graph LR
    A[Generador Python\n(Productor)] -->|JSON| B((Apache Kafka\nTopic: sensor_data))
    B -->|Subscribe| C{Spark Structured\nStreaming}
    C -->|Agregación| D[Consola/Output\n(DataFrames)]


(El diagrama utiliza Mermaid, soportado nativamente por GitHub).

⚙ Requisitos Previos

Asegúrate de tener instaladas y configuradas las siguientes dependencias en tu entorno Linux (Ubuntu/Debian):

Java JDK 8/11 (Requerido por Spark y Kafka).

Apache Kafka (Versión 3.x recomendada).

Apache Spark (Versión 3.x con soporte para PySpark).

Python 3.8+ con las siguientes librerías:

pip install kafka-python pyspark


📁 Estructura del Repositorio

📦 Repositorio_BigData
 ┣ 📜 generador_poblacional.py     # Script productor de datos aleatorios
 ┣ 📜 procesador_poblacional_spark.py # Script consumidor de Spark Streaming
 ┣ 📜 kafka_producer.py            # (Alternativa) Productor de clima
 ┣ 📜 Tarea3_grupo.pdf             # Informe académico final
 ┗ 📜 README.md                    # Documentación del proyecto


🚀 Ejecución Paso a Paso

Sigue estos pasos en terminales separadas para levantar todo el ecosistema.

1. Iniciar Zookeeper y Kafka

Abre una terminal y levanta los servicios principales (asegúrate de los paths):

# Iniciar Zookeeper
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties

# Iniciar Broker de Kafka (en una nueva pestaña)
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties


2. Crear el Topic

Crea el canal por donde viajarán los mensajes:

/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic datos_poblacion


3. Ejecutar el Productor (Generador)

Inicia la simulación de eventos en tiempo real:

python3 generador_poblacional.py


4. Ejecutar el Consumidor (Spark)

En una nueva terminal, procesa los datos acoplando el paquete de Kafka para Spark (revisa tu versión de Spark, ej. 3.5.0):

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 procesador_poblacional_spark.py


📈 Resultados Esperados

Al ejecutar ambos scripts, verás en la consola de Spark un DataFrame actualizándose en intervalos regulares (ej. cada 10 segundos) mostrando estadísticas agregadas, como conteos poblacionales o promedios climáticos procesados directamente desde la memoria.

👨‍💻 Autor

Jose Luis Sierra Ramirez

🎓 Estudiante de Ingeniería / Tecnología

🏫 Universidad Nacional Abierta y a Distancia (UNAD)

📊 Curso: Big Data (202016911)

"El éxito en el Big Data no se trata solo de la cantidad de datos, sino de la velocidad y precisión de las decisiones que puedes tomar con ellos."
