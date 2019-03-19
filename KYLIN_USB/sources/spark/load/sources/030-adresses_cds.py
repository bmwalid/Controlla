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
spark = SparkSession.builder.appName("030-adresses_cds").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-cds/cds_supply/adresses_cds/adresses_cds/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c20", "_c0", "_c1") \
    .withColumn("country_code", df["_c20"].cast(StringType())) \
    .withColumn("address_num", df["_c0"].cast(IntegerType())) \
    .withColumn("sapsrc", df["_c1"].cast(StringType())) \
    .select("country_code", "address_num", "sapsrc") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.adresses_cds")

# stopping session
spark.sparkContext.stop()
