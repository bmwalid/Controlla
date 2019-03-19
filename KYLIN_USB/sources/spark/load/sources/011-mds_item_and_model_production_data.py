# import Libraries
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G")

# Initialize Spark Session
spark = SparkSession.builder.appName("011-mds_item_and_model_production_data").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods/mds_item_and_model_production_data/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c5") \
    .withColumn("iam_and_item_model_code", df["_c0"].cast(IntegerType())) \
    .withColumn("pty_process_type_pty", df["_c5"].cast(StringType())) \
    .select("iam_and_item_model_code", "pty_process_type_pty") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.mds_item_and_model_production_data")

# stopping session
spark.sparkContext.stop()
