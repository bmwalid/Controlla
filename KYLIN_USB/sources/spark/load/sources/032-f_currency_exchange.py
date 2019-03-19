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
spark = SparkSession.builder.appName("032-f_currency_exchange").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://preprod-decathlon-cds/cds/f_currency_exchange/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c3", "_c5", "_c0") \
    .withColumn("hde_effect_date", df["_c3"].cast(TimestampType())) \
    .withColumn("hde_share_price", df["_c5"].cast(FloatType())) \
    .withColumn("cpt_idr_cur_price", df["_c0"].cast(IntegerType())) \
    .select("hde_effect_date", "hde_share_price", "cpt_idr_cur_price") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.f_currency_exchange")

# stopping session
spark.sparkContext.stop()
