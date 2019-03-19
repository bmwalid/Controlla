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
spark = SparkSession.builder.appName("020-mds_elt_niveau").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods/mds_elt_niveau/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c1", "_c2", "_c6", "_c7", "_c3", "_c4", "_c5") \
    .withColumn("org_num_organisation_niv", df["_c0"].cast(IntegerType())) \
    .withColumn("niv_num_niveau_niv", df["_c1"].cast(StringType())) \
    .withColumn("eln_num_elt_niveau", df["_c2"].cast(IntegerType())) \
    .withColumn("tlb_typ_libelle_lib", df["_c6"].cast(StringType())) \
    .withColumn("lib_num_libelle_lib", df["_c7"].cast(IntegerType())) \
    .withColumn("org_num_organisation_sup", df["_c3"].cast(IntegerType())) \
    .withColumn("niv_num_niveau_sup", df["_c4"].cast(IntegerType())) \
    .withColumn("eln_num_elt_niveau_sup", df["_c5"].cast(IntegerType())) \
    .select("org_num_organisation_niv", "niv_num_niveau_niv", "eln_num_elt_niveau", "tlb_typ_libelle_lib",
            "lib_num_libelle_lib", "org_num_organisation_sup", "niv_num_niveau_sup", "eln_num_elt_niveau_sup") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.mds_elt_niveau")

# stopping session
spark.sparkContext.stop()
