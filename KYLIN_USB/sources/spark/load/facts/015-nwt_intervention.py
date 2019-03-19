# import Libraries
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G")

# Initialize Spark Session
spark = SparkSession.builder.appName("015-nwt_intervention").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods_retail/nwt_intervention/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c1", "_c7", "_c17", "_c0", "_c15", "_c12") \
    .withColumn("itv_id", df["_c1"].cast(IntegerType())) \
    .withColumn("itv_document_number", df["_c7"].cast(StringType())) \
    .withColumn("currency_code", df["_c17"].cast(StringType())) \
    .withColumn("osr_server_alias_name", df["_c0"].cast(StringType())) \
    .withColumn("itv_creation_site", df["_c15"].cast(StringType())) \
    .withColumn("itv_id_intervention_type", df["_c12"].cast(IntegerType())) \
    .select("itv_id", "itv_document_number", "currency_code", "osr_server_alias_name", "itv_creation_site",
            "itv_id_intervention_type") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.nwt_intervention")

# stopping session
spark.sparkContext.stop()
