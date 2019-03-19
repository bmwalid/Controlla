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
spark = SparkSession.builder.appName("019-mds_libelle_modele").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods/mds_libelle_modele/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c2", "_c0", "_c1", "_c3") \
    .withColumn("lib_num_libelle_lib", df["_c2"].cast(IntegerType())) \
    .withColumn("elg_num_elt_gestion_elg", df["_c0"].cast(IntegerType())) \
    .withColumn("tlb_typ_libelle_lib", df["_c1"].cast(StringType())) \
    .select("lib_num_libelle_lib", "elg_num_elt_gestion_elg", "tlb_typ_libelle_lib") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.mds_libelle_modele")

# stopping session
spark.sparkContext.stop()
