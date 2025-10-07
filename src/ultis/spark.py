from pyspark.sql import SparkSession
import yaml

def get_spark_session(app_name="SparkPreprocessing"):
    with open(r"/opt/spark-config/spark_config.yaml") as f:
        cfg = yaml.safe_load(f)
    master = cfg.get("master", "local[*]")
    partitions = str(cfg.get("partitions", 4))

    spark = (SparkSession.builder
             .appName(app_name)
             .master(master)
             .config("spark.sql.shuffle.partitions", partitions)
             .getOrCreate())
    return spark
