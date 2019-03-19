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
spark = SparkSession.builder.appName("026-nwt_itv_model").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods_retail/nwt_itv_model/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c1", "_c2") \
    .withColumn("osr_server_alias_name", df["_c0"].cast(StringType())) \
    .withColumn("imo_internal_code", df["_c2"].cast(StringType())) \
    .withColumn("itv_id", df["_c1"].cast(IntegerType())) \
    .select("osr_server_alias_name", "imo_internal_code", "itv_id") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.nwt_itv_model")

# stopping session
spark.sparkContext.stop()
