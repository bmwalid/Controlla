# import libraries
import sys
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
# get args
years = sys.argv[1]
months = sys.argv[2]

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.sql.cbo.enabled", "true") \

# Initialize Spark Session
spark = SparkSession.builder.appName("etapes_de_vie_" + years + months).config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Initialize Spark Context
sc = spark.sparkContext
sqlContext = HiveContext(sc)
sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

# transforming args
months = str(int(months))
years = str(int(years))

# getting cash receipts
tdt = spark.table("kylin_usb_mqb.f_transaction_detail")\
    .filter((col("year_transaction") == years) & (col("month_transaction") == months)) \
    .select("the_transaction_id", "tdt_num_line", "sku_idr_sku", "day_transaction")


# getting d_sku_h
sku = spark.table("kylin_usb_mqb.d_sku_h")\
           .filter((col("year_sku") == years) & (col("month_sku") == months)) \
           .select("sku_idr_sku",
                   "sku_num_sku",
                   "sku_num_sku_r3",
                   "mdl_num_model_r3",
                   "day_sku")



# getting mds_elt_gestion_echange
mge = spark.table("kylin_usb_mqb.mds_elt_gestion_echange")\
            .filter((col("year_mge") == years) & (col("month_mge") == months)) \
           .select("elg_num_elt_gestion_elg", "eta_num_etape_eta", "day_mge")


# join cash receipts with skus
cond1 = [tdt.sku_idr_sku == sku.sku_idr_sku, sku.sku_date_begin <= tdt.tdt_date_transaction,
         sku.sku_date_end >= tdt.tdt_date_transaction]
tdt = tdt.join(sku, how='left', on=cond1).select("the_transaction_id", "tdt_num_line", "sku_num_sku_r3", "sku_num_sku",
                                                 "tdt_date_transaction")

cond2 = [tdt.sku_num_sku == mge.elg_num_elt_gestion_elg]
tdt = tdt.join(mge, how='left', on=cond2).select("the_transaction_id", "tdt_num_line", "eta_num_etape_eta",
                                                 "sku_num_sku_r3", "tdt_date_transaction", "sku_num_sku",
                                                 "ege_date_maj_etape")

tdt = tdt.withColumn("datediff", F.datediff("ege_date_maj_etape", "tdt_date_transaction")) \
    .withColumn("row_number", row_number().over(
    Window.partitionBy("the_transaction_id", "tdt_num_line", "sku_num_sku").orderBy(desc("datediff")))) \
    .filter(col("row_number") == 1) \
    .select('the_transaction_id', 'tdt_num_line', 'eta_num_etape_eta', 'sku_num_sku_r3', 'tdt_date_transaction')
tdt = tdt.withColumn("year_transaction", year(tdt["tdt_date_transaction"]).cast(StringType())) \
    .withColumn("month_transaction", month(tdt["tdt_date_transaction"]).cast(StringType()))

tdt.repartition(300).write.option("compression", "snappy").mode("overwrite")\
    .insertInto("kylin_usb_mqb.etapes_de_vie")
# Stopping Spark
spark.sparkContext.stop()
