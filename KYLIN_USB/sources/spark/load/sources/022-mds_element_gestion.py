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
spark = SparkSession.builder.appName("022-mds_element_gestion").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods/mds_element_gestion/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c2", "_c5") \
    .withColumn("elg_num_elt_gestion_elg", df["_c0"].cast(IntegerType())) \
    .withColumn("elg_num_elt_gestion_mod", df["_c2"].cast(IntegerType())) \
    .withColumn("elg_mod_ou_art", df["_c5"].cast(StringType())) \
    .select("elg_num_elt_gestion_elg", "elg_num_elt_gestion_mod", "elg_mod_ou_art") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.mds_element_gestion")

# stopping session
spark.sparkContext.stop()
