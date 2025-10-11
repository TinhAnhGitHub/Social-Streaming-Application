from ultis.spark import get_spark_session
from processing.cleaner import clean_text_udf
from processing.keyword_extractor import keyword_extractor_udf
from processing.embedder import embedder_udf
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, MapType

import json

payload_schema = StructType([
    StructField("id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("author", StringType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True),
    StructField("created_utc", TimestampType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("permalink", StringType(), True),
    StructField("flair", StringType(), True)
])

envelope_schema = StructType([
    StructField("entity_type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("mode", StringType(), True),
    StructField("payload", payload_schema, True),
    StructField("emitted_at", TimestampType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

def main(use_kafka=False, kafka_bootstrap="localhost:9092", kafka_topic="topic_name"):
    spark = get_spark_session("SparkPreprocessing")

    if use_kafka:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap) \
            .option("subscribe", kafka_topic) \
            .load()

        df = df.selectExpr("CAST(value AS STRING) as json_str")

        df = df.withColumn("json_data", from_json(col("json_str"), envelope_schema)) \
               .select("json_data.*")
    else:
        df = spark.read.option("multiLine", True).json(r"/opt/spark-data/raw/sample.json")

    df = df.withColumn("body", col("payload.body"))

    df = df.withColumn("clean_body", clean_text_udf(col("body")))
    df = df.withColumn("keywords", keyword_extractor_udf(col("clean_body")))
    df = df.withColumn("embedding", embedder_udf(col("clean_body")))

    if use_kafka:
        query = df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
        query.awaitTermination()
    else:
        df.write.mode("overwrite").json(r"/opt/spark-data/processed/")

    spark.stop()


if __name__ == "__main__":
    main(use_kafka=True, kafka_bootstrap="localhost:9092", kafka_topic="reddit.submissions")
    # main(use_kafka=True, kafka_bootstrap="localhost:9092", kafka_topic="reddit.comments")
