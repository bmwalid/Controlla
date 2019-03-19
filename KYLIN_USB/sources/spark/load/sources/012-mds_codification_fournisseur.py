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
spark = SparkSession.builder.appName("012-mds_codification_fournisseur").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods/mds_codification_fournisseur/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c5", "_c0", "_c1", "_c2", "_c3") \
    .withColumn("cof_code_fournisseur", df["_c5"].cast(IntegerType())) \
    .withColumn("elg_num_elt_gestion_elg", df["_c0"].cast(IntegerType())) \
    .withColumn("tti_num_type_tiers_four", df["_c1"].cast(IntegerType())) \
    .withColumn("tir_num_tiers_four", df["_c2"].cast(IntegerType())) \
    .withColumn("tir_sous_num_tiers_four", df["_c3"].cast(IntegerType())) \
    .select("cof_code_fournisseur", "elg_num_elt_gestion_elg", "tti_num_type_tiers_four", "tir_num_tiers_four",
            "tir_sous_num_tiers_four") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.mds_codification_fournisseur")

# stopping session
spark.sparkContext.stop()
