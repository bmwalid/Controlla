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
spark = SparkSession.builder.appName("027-mds_declinaison_article").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods/mds_declinaison_article/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9") \
    .withColumn("elg_num_elt_gestion_elg", df["_c0"].cast(IntegerType())) \
    .withColumn("val_grille_vag", df["_c1"].cast(StringType())) \
    .withColumn("tgr_num_type_grille_vag", df["_c2"].cast(IntegerType())) \
    .withColumn("tti_num_type_tiers_vag", df["_c3"].cast(IntegerType())) \
    .withColumn("tir_num_tiers_vag", df["_c4"].cast(IntegerType())) \
    .withColumn("tir_sous_num_tiers_vag", df["_c5"].cast(IntegerType())) \
    .withColumn("dec_date_creation", df["_c6"].cast(TimestampType())) \
    .withColumn("dec_date_maj", df["_c7"].cast(TimestampType())) \
    .withColumn("dec_date_maj_tec", df["_c8"].cast(TimestampType())) \
    .withColumn("dec_typ_declinaison", df["_c9"].cast(StringType())) \
    .select("elg_num_elt_gestion_elg", "val_grille_vag", "tgr_num_type_grille_vag", "tti_num_type_tiers_vag",
            "tir_num_tiers_vag", "tir_sous_num_tiers_vag", "dec_date_creation", "dec_date_maj", "dec_date_maj_tec",
            "dec_typ_declinaison") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.mds_declinaison_article")

# stopping session
spark.sparkContext.stop()
