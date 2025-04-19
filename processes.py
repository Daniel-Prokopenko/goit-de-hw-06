from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config
import os
from kafka import KafkaProducer
import json
import uuid

# Пакет, необхідний для читання Kafka зі Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Створення SparkSession
spark = SparkSession.builder.appName("KafkaStreaming").master("local[*]").getOrCreate()

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
# maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    )
    .option("subscribe", "danylo_building_sensors")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .load()
)

# Визначення схеми для JSON,
# оскільки Kafka має структуру ключ-значення, а значення має формат JSON.
json_schema = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("sensor_id", IntegerType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
    ]
)

clean_df = (
    df.selectExpr("CAST(value AS STRING) AS value_deserialized")
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
    .withColumn(
        "timestamp",
        from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp"),
    )
    .select(
        col("timestamp"),
        col("value_json.sensor_id").alias("sensor_id"),
        col("value_json.temperature").alias("temperature"),
        col("value_json.humidity").alias("humidity"),
    )
)


conditions = spark.read.csv("alerts_conditions.csv", header=True)
conditions.createTempView("conditions")
spark.sql("SELECT * FROM conditions").show()

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def process_batch(batch_df, batch_id):
    alert_df = (
        batch_df.withColumn(
            "code",
            when(col("avg_humidity") < 40, 101)
            .when(col("avg_humidity") > 60, 102)
            .when(col("avg_temperature") < 30, 103)
            .when(col("avg_temperature") > 40, 104),
        )
        .withColumn(
            "message",
            when(col("code") == 101, "It's too dry")
            .when(col("code") == 102, "It's too wet")
            .when(col("code") == 103, "It's too cold")
            .when(col("code") == 104, "It's too hot"),
        )
        .filter(col("code").isNotNull())
    )

    alert_df.show(truncate=False)

    for row in alert_df.collect():
        warning = {
            "sensor_id": row["sensor_id"],
            "timestamp": str(row["window"].start),
            "temperature": row["avg_temperature"],
            "humidity": row["avg_humidity"],
            "warning": row["message"],
        }

        topic = (
            "danylo_temperature_alerts"
            if row["code"] in (103, 104)
            else "danylo_humidity_alerts"
        )
        producer.send(topic, key=str(uuid.uuid4()), value=warning)
        print(f"✅ Sent to {topic}: {warning}")

    producer.flush()

    alert_df.select(
        "window", "sensor_id", "avg_temperature", "avg_humidity", "code", "message"
    ).show(truncate=False)


windowedAvg = (
    clean_df.withWatermark("timestamp", "10 seconds")
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"), col("sensor_id"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
    )
)

# Виведення даних на екран
displaying_df = (
    windowedAvg.writeStream.trigger(processingTime="30 seconds")
    .outputMode("append")
    .format("console")
    .foreachBatch(process_batch)
    .start()
    .awaitTermination()
)
