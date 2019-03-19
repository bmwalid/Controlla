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
spark = SparkSession.builder.appName("028-mds_val_grille").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods/mds_val_grille/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c6", "_c12", "_c0", "_c2", "_c3", "_c4", "_c1") \
    .withColumn("tlb_typ_libelle_lib", df["_c6"].cast(StringType())) \
    .withColumn("lib_num_libelle_lib", df["_c12"].cast(IntegerType())) \
    .withColumn("val_grille_vag", df["_c0"].cast(StringType())) \
    .withColumn("tti_num_type_tiers_grl", df["_c2"].cast(IntegerType())) \
    .withColumn("tir_num_tiers_grl", df["_c3"].cast(IntegerType())) \
    .withColumn("tir_sous_num_tiers_grl", df["_c4"].cast(IntegerType())) \
    .withColumn("tgr_num_type_grille_grl", df["_c1"].cast(IntegerType())) \
    .select("tlb_typ_libelle_lib", "lib_num_libelle_lib", "val_grille_vag", "tti_num_type_tiers_grl",
            "tir_num_tiers_grl", "tir_sous_num_tiers_grl", "tgr_num_type_grille_grl") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.mds_val_grille")

# stopping session
spark.sparkContext.stop()
