import argparse
from pyspark.sql.functions import col, from_json
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, MapType
)
import os
import sys
ROOT_DIR = os.path.abspath(os.path.join(__file__, '../../..'))

sys.path.insert(0,ROOT_DIR)

from src.spark.utils.spark import get_spark_session
from src.spark.processing.cleaner import clean_text_udf
from src.spark.processing.keyword_extractor import keyword_extractor_udf
from src.spark.processing.embedder import embedder_udf




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
    df = df.withColumn("id", col("payload.id"))
    df = df.withColumn("body", col("payload.body"))
    df = df.withColumn("clean_body", clean_text_udf(col("body")))
    df = df.withColumn("keywords", keyword_extractor_udf(col("clean_body")))
    df = df.withColumn("embedding", embedder_udf(col("clean_body")))
    return df

# ===== MAIN DRIVER =====
def run_spark_stream(
        topic: str, 
        kafka_bootstrap: str = "localhost:9092",
        save_dir: str | None = None
    ):
    spark = get_spark_session(f"SparkPreprocessing-{topic.replace('.', '_')}")

    if topic == "reddit.submissions":
        envelope_schema = build_envelope_schema(payload_submission_schema)
    elif topic == "reddit.comments":
        envelope_schema = build_envelope_schema(payload_comment_schema)
    else:
        raise ValueError(f"Unsupported topic: {topic}")

    
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
    )
    df = df.withColumn("json_data", from_json(col("json_str"), envelope_schema)).select("json_data.*")
    df = process_stream(df)

    base_dir = (
        save_dir
        if save_dir is not None
        else os.environ.get("SPARK_SAVE_DIR", "/tmp/spark-data")
    )

    kafka_save_dir = f"{base_dir}/kafka/output/{topic}"
    checkpoint_dir = f"{base_dir}/kafka/checkpoints/{topic}"

    os.makedirs(kafka_save_dir, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)

    print(f"Writing stream to {kafka_save_dir} with checkpoint {checkpoint_dir}")
    query = (
        df.writeStream
        .format("json")
        .option("path", kafka_save_dir)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()
    spark.stop()


# ===== ENTRYPOINT =====
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Reddit Stream Processor")
    parser.add_argument(
        "--topic",
        type=str,
        choices=["reddit.submissions", "reddit.comments"],
        default="reddit.submissions",
        help="Kafka topic to read from."
    )
    parser.add_argument("--bootstrap", type=str, default="localhost:9092", help="Kafka bootstrap server.")
    parser.add_argument(
        "--save-dir",
        type=str,
        default=None,
        help=(
            "Base directory for streaming output and checkpoints. "
            "Defaults to $SPARK_SAVE_DIR if set, otherwise /tmp/spark-data."
        ),
    )
    args = parser.parse_args()
    run_spark_stream(
        topic=args.topic,
        kafka_bootstrap=args.bootstrap,
        save_dir=args.save_dir,
    )
