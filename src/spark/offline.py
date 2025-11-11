import os
import sys

ROOT_DIR = os.path.abspath(
    os.path.join(__file__, '../../..')
)
print(ROOT_DIR)
sys.path.insert(0,ROOT_DIR)

from src.spark.utils.spark import get_spark_session
from src.spark.processing.cleaner import clean_text_udf
from src.spark.processing.keyword_extractor import keyword_extractor_udf
from src.spark.processing.embedder import embedder_udf
from pyspark.sql.functions import col

def main():
    spark = get_spark_session("SparkPreprocessing")

    df = spark.read.option("multiLine", True).json(r"/opt/spark-data/raw/sample.json")
    df.printSchema()

    df = df.withColumn("clean_body", clean_text_udf(col("payload.body")))
    df = df.withColumn("keywords", keyword_extractor_udf(col("clean_body")))
    df = df.withColumn("embedding", embedder_udf(col("clean_body")))

    df.write.mode("overwrite").json(r"/opt/spark-data/processed/")
    spark.stop()

if __name__ == "__main__":
    main()
