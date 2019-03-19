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
spark = SparkSession.builder.appName("023-process_type_label").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-cds/cds_supply/Process_type_label/Process_type_label/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c1", "_c2") \
    .withColumn("pty_label_lang", df["_c0"].cast(StringType())) \
    .withColumn("pty_process_type_pty", df["_c1"].cast(IntegerType())) \
    .withColumn("pty_process_type_label", df["_c2"].cast(StringType())) \
    .select("pty_label_lang", "pty_process_type_pty", "pty_process_type_label") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.process_type_label")

# stopping session
spark.sparkContext.stop()
