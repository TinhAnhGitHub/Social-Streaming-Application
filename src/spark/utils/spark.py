from pyspark.sql import SparkSession
import yaml

def get_spark_session(app_name="SparkPreprocessing"):
    master = "local[*]"
    partitions = 4

    spark = (
          SparkSession.builder
          .appName(app_name) #type:ignore
          .master(master)
          .config("spark.sql.shuffle.partitions", partitions)
          .config(
              "spark.jars.packages",
              ",".join([
                  "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
                  "org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1",
                  "org.elasticsearch:elasticsearch-spark-30_2.13:8.13.4"
              ])
          )
          .config("spark.es.nodes", "elasticsearch")
          .config("spark.es.port",  "9200")
          .config("spark.es.nodes.wan.only", "true")
          .config("spark.es.batch.size.entries", 1000)
          .config("spark.es.batch.write.refresh", "false")
          .getOrCreate()
      )
    return spark



