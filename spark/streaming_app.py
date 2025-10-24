"""
Spark Streaming Application
อ่านข้อมูล GPS จาก Kafka → ประมวลผล → เขียนเข้า Elasticsearch
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, FloatType

def main():
    # ============================================
    # 1. สร้าง SparkSession
    # ============================================
    spark = SparkSession.builder \
        .appName("HybridTransitStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session created.")

    # ============================================
    # 2. Schema ของ JSON ข้อมูลจาก Kafka
    # ============================================
    schema = StructType([
        StructField("bus_id", StringType(), True),
        StructField("route", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("speed_kmh", FloatType(), True)
    ])

    # ============================================
    # 3. อ่านข้อมูลจาก Kafka
    # ============================================
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "bus-locations") \
        .option("startingOffsets", "latest") \
        .load()

    print("✅ Kafka stream DataFrame created.")
    kafka_df.printSchema()

    # ============================================
    # 4. แปลง JSON เป็น columns
    # ============================================
    processed_df = kafka_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    print("✅ DataFrame with parsed JSON created.")
    processed_df.printSchema()

    # ============================================
    # 5. เตรียมข้อมูลสำหรับ Elasticsearch
    # ============================================
    # สร้าง location field เป็น struct สำหรับ geo_point
    es_df = processed_df.withColumn(
        "location", 
        struct(col("lat"), col("lon"))
    )
    
    print("✅ Elasticsearch DataFrame created.")
    es_df.printSchema()

    # ============================================
    # 6. เขียนไปยัง Elasticsearch
    # ============================================
    query_es = es_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "bus-locations") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "false") \
        .option("es.mapping.id", "bus_id") \
        .option("checkpointLocation", "/tmp/spark-checkpoint-es") \
        .start()

    # ============================================
    # 7. เขียนออก console สำหรับ debug
    # ============================================
    query_console = es_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    print("✅ Streaming queries started.")
    print("📊 รอข้อมูลจาก Kafka...")
    print("💡 เปิด Terminal อื่นรัน: cd producer && python bus_producer.py")

    # ============================================
    # 8. ให้ Spark ทำงานค้างไว้
    # ============================================
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()