# import libraries
import sys
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# getting arguments
years = sys.argv[1]
months = sys.argv[2]

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G") \
    .set("spark.sql.cbo.enabled", "true")

spark = SparkSession.builder.appName("ALIM_KYL_VTE_" + years + "_" + months)\
                    .config(conf=SparkConf()).enableHiveSupport().getOrCreate()

sc = spark.sparkContext
sqlContext = HiveContext(sc)
sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

months = str(int(months))
years = str(int(years))

# defining HDFS path
hdfs = "hdfs:///user/hive/warehouse/"

# Defining HDFS database used
db = "kylin_usb_mqb.db/"

# defining table used
hdfs_tables = ['f_transaction_detail/', 'd_business_unit/', 'd_sku_h/', 't001w2/', 'sales_organizations_texts/',
               'purchases_organizations_texts/', 'wrf1/', 'mds_detail_elt_gestion/',
               'mds_item_and_model_production_data/', 'process_type_label/', 'etapes_de_vie/', 'quotas_fournisseurs/']

# load tables
f_transaction = spark.table("kylin_usb_mqb.f_transaction_detail").filter(
    (F.col("year_transaction") == years) & (F.col("month_transaction") == months))
f_transaction = f_transaction.withColumn("tdt_date_transaction",
                                         f_transaction["tdt_date_transaction"].cast(T.DateType()))
d_business_unit = spark.table("kylin_usb_mqb.d_business_unit")
d_sku_h = spark.table("kylin_usb_mqb.d_sku_h")
t001w = spark.table("kylin_usb_mqb.t001w2")
sot = spark.table("kylin_usb_mqb.sales_organizations_texts").filter(F.col("language_id") == 'EN')
pot = spark.table("kylin_usb_mqb.purchases_organizations_texts")
wrf1 = spark.table("kylin_usb_mqb.wrf1")
mdg = spark.table("kylin_usb_mqb.mds_detail_elt_gestion")
mpd = spark.table("kylin_usb_mqb.mds_item_and_model_production_data")
lab = spark.table("kylin_usb_mqb.process_type_label").filter(F.col("pty_label_lang") == 'EN')
kyl_edv = spark.table("kylin_usb_mqb.etapes_de_vie").filter(
    (F.col("year_transaction") == years) & (F.col("month_transaction") == months))
qf = spark.table("kylin_usb_mqb.quotas_fournisseurs")
qf = qf.withColumn("rn", F.row_number().over(
    Window.partitionBy("material_id", "mdl_num_model_r3", "purch_org").orderBy(F.desc("date_max")))) \
    .filter((F.col("rn") == 1) & (~F.col("purch_org").isNull())) \
    .withColumn("material_id", qf["material_id"].cast(T.IntegerType())) \
    .withColumn("mdl_num_model_r3", qf["mdl_num_model_r3"].cast(T.IntegerType())).drop_duplicates()

# joining cash receipts and mags
kyl_tdt = f_transaction \
    .join(d_business_unit, how='left',
          on=[f_transaction.but_idr_business_unit == d_business_unit.but_idr_business_unit]) \
    .select(f_transaction.the_transaction_id, f_transaction.tdt_num_line, f_transaction.tdt_date_transaction,
            f_transaction.but_idr_business_unit, f_transaction.sku_idr_sku, f_transaction.f_qty_item,
            d_business_unit.but_num_business_unit, d_business_unit.but_name_business_unit) \
    .withColumn("split", F.split(f_transaction["the_transaction_id"], '-').getItem(1).cast(T.IntegerType()))
# prepare d_sku_h
kyl_sku = d_sku_h.select('sku_idr_sku', 'sku_num_sku', 'sku_num_sku_r3', 'mdl_num_model_r3', 'sku_date_begin',
                         'sku_date_end') \
    .withColumn("sku_date_begin", d_sku_h["sku_date_begin"].cast(T.DateType())) \
    .withColumn("sku_date_end", d_sku_h["sku_date_end"].cast(T.DateType()))
# joint with cash receipts
cond = [kyl_tdt.sku_idr_sku == kyl_sku.sku_idr_sku, kyl_tdt.tdt_date_transaction >= kyl_sku.sku_date_begin,
        kyl_tdt.tdt_date_transaction <= kyl_sku.sku_date_end]
kyl_tdt_sku = kyl_tdt.join(kyl_sku, how='left', on=cond) \
    .select(kyl_tdt.the_transaction_id, kyl_tdt.tdt_num_line, kyl_tdt.tdt_date_transaction,
            kyl_tdt.but_num_business_unit, kyl_tdt.but_name_business_unit, kyl_tdt.sku_idr_sku, kyl_tdt.f_qty_item,
            kyl_sku.sku_num_sku, kyl_sku.sku_num_sku_r3, kyl_sku.mdl_num_model_r3, kyl_tdt.split)
# Getting organisation
kyl_org = t001w.join(sot, how='left', on=[t001w.sales_org == sot.sales_org]) \
    .select(t001w.plant, t001w.purch_org, t001w.sales_org, sot.sales_org_text) \
    .join(pot, how='left', on=[t001w.purch_org == pot.purch_org]) \
    .select(t001w.plant, t001w.purch_org, pot.purch_org_text, t001w.sales_org, sot.sales_org_text) \
    .join(wrf1, how='left', on=[t001w.plant == wrf1.cust_num_plant]) \
    .select(t001w.plant, t001w.purch_org, pot.purch_org_text, t001w.sales_org, sot.sales_org_text, wrf1.distrib_channel)
# Getting stuff
kyl_mdg = mdg.select('ege_basique', 'ege_security_product', 'elg_num_elt_gestion_elg')
# getting second stuff
kyl_mpd = mpd.join(lab, how='left', on=[mpd.pty_process_type_pty == lab.pty_process_type_pty]) \
    .select(mpd.iam_and_item_model_code, lab.pty_process_type_pty, lab.pty_process_type_label) \
    .withColumnRenamed("iam_and_item_model_code", "iam_item_and_model_code")
# transfom into facts table (first step)
ft = kyl_tdt_sku.join(kyl_org, how='left', on=[kyl_tdt_sku.split == kyl_org.plant]) \
    .select(kyl_tdt_sku.the_transaction_id, kyl_tdt_sku.tdt_num_line, kyl_tdt_sku.tdt_date_transaction,
            kyl_tdt_sku.but_num_business_unit, kyl_tdt_sku.but_name_business_unit, kyl_tdt_sku.sku_idr_sku,
            kyl_tdt_sku.f_qty_item, kyl_tdt_sku.sku_num_sku, kyl_tdt_sku.sku_num_sku_r3, kyl_tdt_sku.mdl_num_model_r3,
            kyl_org.purch_org, kyl_org.sales_org, kyl_org.sales_org_text, kyl_org.purch_org_text,
            kyl_org.distrib_channel) \
    .join(kyl_mdg, how='left', on=[kyl_tdt_sku.sku_num_sku == kyl_mdg.elg_num_elt_gestion_elg]) \
    .select(kyl_tdt_sku.the_transaction_id, kyl_tdt_sku.tdt_num_line, kyl_tdt_sku.tdt_date_transaction,
            kyl_tdt_sku.but_num_business_unit, kyl_tdt_sku.but_name_business_unit, kyl_tdt_sku.sku_idr_sku,
            kyl_tdt_sku.f_qty_item, kyl_tdt_sku.sku_num_sku, kyl_tdt_sku.sku_num_sku_r3, kyl_tdt_sku.mdl_num_model_r3,
            kyl_org.purch_org, kyl_org.sales_org, kyl_org.sales_org_text, kyl_org.purch_org_text,
            kyl_org.distrib_channel, kyl_mdg.ege_basique, kyl_mdg.ege_security_product) \
    .join(kyl_mpd, how='left', on=[kyl_mpd.iam_item_and_model_code == kyl_tdt_sku.sku_num_sku]) \
    .select(kyl_tdt_sku.the_transaction_id, kyl_tdt_sku.tdt_num_line, kyl_tdt_sku.tdt_date_transaction,
            kyl_tdt_sku.but_num_business_unit, kyl_tdt_sku.but_name_business_unit, kyl_tdt_sku.sku_idr_sku,
            kyl_tdt_sku.f_qty_item, kyl_tdt_sku.sku_num_sku, kyl_tdt_sku.sku_num_sku_r3, kyl_tdt_sku.mdl_num_model_r3,
            kyl_org.purch_org, kyl_org.sales_org, kyl_org.sales_org_text, kyl_org.purch_org_text,
            kyl_org.distrib_channel, kyl_mdg.ege_basique, kyl_mdg.ege_security_product, kyl_mpd.iam_item_and_model_code,
            kyl_mpd.pty_process_type_pty, kyl_mpd.pty_process_type_label) \
    .join(kyl_edv, how='left', on=[kyl_tdt_sku.the_transaction_id == kyl_edv.the_transaction_id,
                                   kyl_tdt_sku.sku_num_sku_r3 == kyl_edv.sku_num_sku_r3,
                                   kyl_tdt_sku.tdt_num_line == kyl_edv.tdt_num_line]) \
    .select(kyl_tdt_sku.the_transaction_id, kyl_tdt_sku.tdt_num_line, kyl_tdt_sku.tdt_date_transaction,
            kyl_tdt_sku.but_num_business_unit, kyl_tdt_sku.but_name_business_unit, kyl_tdt_sku.sku_idr_sku,
            kyl_tdt_sku.f_qty_item, kyl_tdt_sku.sku_num_sku, kyl_tdt_sku.sku_num_sku_r3, kyl_tdt_sku.mdl_num_model_r3,
            kyl_org.purch_org, kyl_org.sales_org, kyl_org.sales_org_text, kyl_org.purch_org_text,
            kyl_org.distrib_channel, kyl_mdg.ege_basique, kyl_mdg.ege_security_product, kyl_mpd.iam_item_and_model_code,
            kyl_mpd.pty_process_type_pty, kyl_mpd.pty_process_type_label, kyl_edv.eta_num_etape_eta).drop_duplicates()

cond = [qf.material_id == ft.sku_num_sku_r3, ft.purch_org == qf.purch_org]
ft = ft.select(ft.the_transaction_id, ft.tdt_num_line, ft.tdt_date_transaction, ft.but_num_business_unit,
               ft.but_name_business_unit, ft.sku_num_sku_r3, ft.mdl_num_model_r3, ft.purch_org, ft.sales_org,
               ft.sales_org_text, ft.purch_org_text, ft.distrib_channel, ft.ege_basique, ft.ege_security_product,
               ft.eta_num_etape_eta, ft.f_qty_item) \
    .withColumn("week_tdt_date_transaction", F.weekofyear(ft["tdt_date_transaction"])) \
    .withColumn("quarter_tdt_date_transaction", F.quarter(ft["tdt_date_transaction"]))
ft = ft.join(qf, how='left', on=cond) \
    .select(ft.the_transaction_id, ft.week_tdt_date_transaction, ft.quarter_tdt_date_transaction,
            ft.tdt_date_transaction, ft.but_num_business_unit, ft.but_name_business_unit, ft.sku_num_sku_r3,
            ft.mdl_num_model_r3, ft.purch_org, ft.sales_org, ft.sales_org_text, ft.purch_org_text, ft.distrib_channel,
            ft.ege_basique, ft.ege_security_product, ft.eta_num_etape_eta, ft.f_qty_item, qf.vendor_id,
            qf.subcontractor_id)
ft = ft.withColumn("year_transaction", F.year(ft.tdt_date_transaction).cast(T.StringType())) \
    .withColumn("month_transaction", F.month(ft.tdt_date_transaction).cast(T.StringType())) \
    .withColumn("tdt_date_transaction", F.dayofmonth(ft.tdt_date_transaction))
ft.repartition(80).write.option("compression", "snappy").mode("overwrite")\
    .insertInto("kylin_usb_mqb.kyl_mqb_vte")

# Stopping Session
spark.sparkContext.stop()
