# import libraries
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import functions as F

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.default.parallelism", 80)\
    .set("spark.sql.cbo.enabled", "true")
# Initialize Spark Session
spark = SparkSession.builder.appName("opv_sku").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Initialize Spark Context
sc = spark.sparkContext
sqlContext = HiveContext(sc)

opv = spark.table("kylin_usb_mqb.opv").repartition(80)
opv = opv.withColumnRenamed("code", "mdl_num_model_r3").withColumn("note_bis", opv["note"])
t001w = spark.table("kylin_usb_mqb.t001w2").coalesce(1).persist()
pot = spark.table("kylin_usb_mqb.purchases_organizations_texts").coalesce(1).persist()
sot = spark.table("kylin_usb_mqb.sales_organizations_texts").coalesce(1).persist()
qf = spark.table("kylin_usb_mqb.quotas_fournisseurs").coalesce(1).persist()

# join country & purch_org
cond1 = [t001w.country == opv.country_reference]
opv = opv.join(t001w, how='left', on=cond1) \
    .select(opv.mdl_num_model_r3, opv.product_reference_id, opv.country_reference, opv.id, opv.note, opv.note_bis,
            opv.collaborator, opv.day_published_at, opv.week_published_at, opv.month_published_at,
            opv.year_published_at, t001w.purch_org, t001w.sales_org)

# join purch_org_text
cond2 = [pot.purch_org == opv.purch_org]
opv = opv.join(pot, how='left', on=cond2) \
    .select(opv.mdl_num_model_r3, opv.product_reference_id, opv.country_reference, opv.id, opv.note, opv.note_bis,
            opv.collaborator, opv.day_published_at, opv.week_published_at, opv.month_published_at,
            opv.year_published_at, opv.purch_org, pot.purch_org_text, opv.sales_org)

# join sales_org_text
cond = [sot.sales_org == opv.sales_org]
opv = opv.join(sot, how='left', on=cond) \
    .select(opv.mdl_num_model_r3, opv.product_reference_id, opv.country_reference, opv.id, opv.note, opv.note_bis,
            opv.collaborator, opv.day_published_at, opv.week_published_at, opv.month_published_at,
            opv.year_published_at, opv.purch_org, pot.purch_org_text, opv.sales_org, sot.sales_org_text)
# join qf
cond3 = [qf.mdl_num_model_r3 == opv.mdl_num_model_r3, qf.purch_org == opv.purch_org]
opv = opv.join(qf, how='left', on=cond3) \
    .select(opv.mdl_num_model_r3, opv.product_reference_id, opv.country_reference, opv.id, opv.note, opv.note_bis,
             opv.collaborator, opv.day_published_at, opv.week_published_at, opv.month_published_at,
             opv.year_published_at, opv.purch_org, opv.purch_org_text, opv.sales_org, opv.sales_org_text,
             qf.vendor_id, qf.subcontractor_id)

opv = opv.withColumn("day_published_at", F.dayofmonth(opv["day_published_at"]))
opv.repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet")\
    .saveAsTable("kylin_usb_mqb.kyl_mqb_opv")
