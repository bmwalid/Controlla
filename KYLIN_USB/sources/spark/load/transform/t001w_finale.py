# import libraries
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "2") \
    .set("spark.executor.memory", "2G") \
    .set("spark.sql.cbo.enabled", "true")

# Initialize Spark Session
spark = SparkSession.builder.appName("t001w_finale").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Initialize Spark Context
sc = spark.sparkContext

t001w = spark.read.parquet("hdfs:///user/hive/warehouse/kylin_usb_mqb.db/t001w").filter(
    (col("purch_org").isNotNull()) & (col("plant_category") == 'A') & (col("distrib_channel") == '02') & (
        col("cust_num_plant").like('0%')))
adresse_cds = spark.read.parquet("hdfs:///user/hive/warehouse/kylin_usb_mqb.db/adresses_cds/")

t001w_prt = t001w.filter(col("sapsrc") == 'PRT').withColumn("plant", t001w["plant_id"].cast(IntegerType()))
adress_prt = adresse_cds.filter(col("sapsrc") == 'PRT').withColumn("address",
                                                                   adresse_cds["address_num"].cast(IntegerType()))

t001w_pbr = t001w.filter(col("sapsrc") == 'PBR').withColumn("plant", t001w["plant_id"].cast(IntegerType()))
adress_pbr = adresse_cds.filter(col("sapsrc") == 'PBR').withColumn("address",
                                                                   adresse_cds["address_num"].cast(IntegerType()))

part_prt = t001w_prt.join(adress_prt, how='left', on=[t001w_prt.address_num == adress_prt.address])
part_pbr = t001w_pbr.join(adress_pbr, how='left', on=[t001w_pbr.address_num == adress_pbr.address])

df = part_prt.union(part_pbr).filter(~col("plant").isin(402, 407, 415, 417)).select("plant", "purch_org", "sales_org",
                                                                                    "country_code").withColumnRenamed(
    "country_code", "country")

df.repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.t001w2")
