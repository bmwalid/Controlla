# import Libraries
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G")

# Initialize Spark Session
spark = SparkSession.builder.appName("005-language_defloc").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods_supply/defloc/labelTextsDEFLOC/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c2", "_c0", "_c1", "_c3") \
    .withColumn("etr_element_id", df["_c2"].cast(IntegerType())) \
    .withColumn("lan_language_code_lan", df["_c0"].cast(StringType())) \
    .withColumn("etr_element_type", df["_c1"].cast(IntegerType())) \
    .withColumn("etr_label", df["_c3"].cast(StringType())) \
    .select("etr_element_id", "lan_language_code_lan", "etr_element_type", "etr_label") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.label_texts_defloc")

# stopping session
spark.sparkContext.stop()
