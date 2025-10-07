from ultis.spark import get_spark_session
from processing.cleaner import clean_text_udf
from processing.keyword_extractor import extract_keywords_udf
from processing.embedder import embed_text_udf
from pyspark.sql.functions import col

def main():
    spark = get_spark_session("SparkPreprocessing")

    df = spark.read.json("data/raw/sample.json")

    df = df.withColumn("clean_text", clean_text_udf(col("text")))
    df = df.withColumn("keywords", extract_keywords_udf(col("clean_text")))
    df = df.withColumn("embedding", embed_text_udf(col("clean_text")))

    df.write.mode("overwrite").json("data/processed/")
    spark.stop()

if __name__ == "__main__":
    main()
    # hello