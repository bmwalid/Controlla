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
spark = SparkSession.builder.appName("007-sales_organizations_texts").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-cds/cds_supply/sales_organizations_texts_cds/salesOrganizationsTexts_cds/"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c1", "_c3", "_c0") \
    .withColumn("sales_org", df["_c1"].cast(StringType())) \
    .withColumn("sales_org_text", df["_c3"].cast(StringType())) \
    .withColumn("language_id", df["_c0"].cast(StringType())) \
    .select("sales_org", "sales_org_text", "language_id") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.sales_organizations_texts")

# stopping session
spark.sparkContext.stop()
