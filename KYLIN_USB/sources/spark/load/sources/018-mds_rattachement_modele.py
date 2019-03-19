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
spark = SparkSession.builder.appName("018-mds_rattachement_modele").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods/mds_rattachement_modele/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c1", "_c2", "_c3") \
    .withColumn("elg_num_elt_gestion_elg", df["_c0"].cast(IntegerType())) \
    .withColumn("org_num_organisation_eln", df["_c1"].cast(IntegerType())) \
    .withColumn("niv_num_niveau_eln", df["_c2"].cast(IntegerType())) \
    .withColumn("eln_num_elt_niveau_eln", df["_c3"].cast(IntegerType())) \
    .select("elg_num_elt_gestion_elg", "org_num_organisation_eln", "niv_num_niveau_eln", "eln_num_elt_niveau_eln") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.mds_rattachement_modele")

# stopping session
spark.sparkContext.stop()
