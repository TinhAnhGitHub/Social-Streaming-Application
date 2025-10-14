import argparse
import json
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, MapType
)
import os

from ultis.spark import get_spark_session
from processing.cleaner import clean_text_udf
from processing.keyword_extractor import keyword_extractor_udf
from processing.embedder import embedder_udf


# ===== PAYLOAD SCHEMAS =====
payload_submission_schema = StructType([
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
    StructField("flair", StringType(), True),
])

payload_comment_schema = StructType([
    StructField("id", StringType(), True),
    StructField("submission_id", StringType(), True),
    StructField("parent_id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("author", StringType(), True),
    StructField("body", StringType(), True),
    StructField("created_utc", TimestampType(), True),
    StructField("score", IntegerType(), True),
    StructField("permalink", StringType(), True),
    StructField("controversiality", IntegerType(), True),
])

# ===== ENVELOPE WRAPPER =====
def build_envelope_schema(payload_schema):
    return StructType([
        StructField("entity_type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("mode", StringType(), True),
        StructField("payload", payload_schema, True),
        StructField("emitted_at", TimestampType(), True),
        StructField("metadata", MapType(StringType(), StringType()), True),
    ])


# ===== PROCESSING FUNCTION =====
def process_stream(df):
    """
    Apply the text cleaning, keyword extraction, and embedding to body.
    """
    df = df.withColumn("body", col("payload.body"))
    df = df.withColumn("clean_body", clean_text_udf(col("body")))
    df = df.withColumn("keywords", keyword_extractor_udf(col("clean_body")))
    df = df.withColumn("embedding", embedder_udf(col("clean_body")))
    return df


# ===== MAIN DRIVER =====
def run_spark_stream(topic: str, kafka_bootstrap: str = "localhost:9092", use_kafka=True):
    spark = get_spark_session(f"SparkPreprocessing-{topic.replace('.', '_')}")

    if topic == "reddit.submissions":
        envelope_schema = build_envelope_schema(payload_submission_schema)
    elif topic == "reddit.comments":
        envelope_schema = build_envelope_schema(payload_comment_schema)
    else:
        raise ValueError(f"Unsupported topic: {topic}")

    if use_kafka:
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("subscribe", topic)
            .load()
            .selectExpr("CAST(value AS STRING) as json_str")
        )
        df = df.withColumn("json_data", from_json(col("json_str"), envelope_schema)).select("json_data.*")
    else:
        df = spark.read.option("multiLine", True).json(r"/opt/spark-data/raw/sample.json")

    df = process_stream(df)

    if use_kafka:
        kafka_save_dir = r"data/kafka/{topic}"
        os.makedirs(kafka_save_dir, exist_ok=True)
        query = (
            df.writeStream
            .format("json")
            .option("path", kafka_save_dir)
            .option("checkpointLocation", "data/kafka_checkpoint")
            .outputMode("append")
            .start()
        )
        query.awaitTermination()
    else:
        df.write.mode("overwrite").json(r"/opt/spark-data/processed/")

    spark.stop()


# ===== ENTRYPOINT =====
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Reddit Stream Processor")
    parser.add_argument(
        "--topic",
        type=str,
        choices=["reddit.submissions", "reddit.comments"],
        required=True,
        help="Kafka topic to read from."
    )
    parser.add_argument("--bootstrap", type=str, default="localhost:9092", help="Kafka bootstrap server.")
    parser.add_argument("--offline", action="store_true", help="Use local JSON instead of Kafka stream.")

    args = parser.parse_args()
    run_spark_stream(
        topic=args.topic,
        kafka_bootstrap=args.bootstrap,
        use_kafka=not args.offline
    )