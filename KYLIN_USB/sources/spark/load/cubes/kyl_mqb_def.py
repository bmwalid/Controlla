from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G") \
    .set("spark.sql.cbo.enabled", "true")

# Initialize Spark Session
spark = SparkSession.builder.appName("ALIM_KYL_MQB_DEF").config(conf=SparkConf()).enableHiveSupport().getOrCreate()
# Initialize Spark Context
sc = spark.sparkContext
# Initialize Hive Context (allow us to query immediately table through HQL-Hive Query Language)
sqlContext = HiveContext(sc)

# Defining database used
sqlContext.sql("use kylin_usb_mqb")

kyl_org_art_def = sqlContext.sql("""SELECT DISTINCT
        t1w.purch_org,
        pot.purch_org_text,
        t1w.sales_org,
        sot.sales_org_text,
        t1w.plant,
        wrf.distrib_channel
    FROM
        t001w2 t1w
    LEFT JOIN
        sales_organizations_texts sot
    ON
        t1w.sales_org = sot.sales_org
    LEFT JOIN
        purchases_organizations_texts pot
    ON
        t1w.purch_org = pot.purch_org
    LEFT JOIN
        wrf1 wrf
    ON
        t1w.plant = wrf.cust_num_plant
    WHERE
        sot.language_id='EN'""")
kyl_org_art_def.registerTempTable("kyl_org")

defects = sqlContext.sql("""
SELECT
     * 
FROM (
    SELECT
        SAD.*
    FROM
        STC_ARTICLES_DEFECTUEUX SAD
    
    UNION
    
    SELECT
        HIS.*
    FROM
        STC_ARTICLES_DEFECTUEUX SAD
    LEFT JOIN
        STC_ARTICLES_DEFECTUEUX_HISTO HIS
    ON
        SAD.DEF_IDENT_ECHANGE=HIS.DEF_IDENT_ECHANGE
    WHERE 
        SAD.DEF_IDENT_ECHANGE IS NULL) TDT 
WHERE 
    (TDT.DEF_TYPE=1 OR TDT.DEF_TYPE=3) 
AND (TDT.DEF_CODE_DESTINATION=300 OR TDT.DEF_CODE_DESTINATION=500)  
AND TDT.DEF_DATE_VALIDATION >= '2017-01-01'
""")
defects.registerTempTable("defects")

tableFinale = sqlContext.sql("""SELECT DISTINCT
    sku.sku_num_sku_r3,
    sku.mdl_num_model_r3,
    day(sad.def_date_validation) as def_date_validation,
    org.plant,
    sad.def_ident_echange,
    sad.dft_code_dft,
    sad.def_date_achat,
    cast(sad.def_type as int) as def_type,
    sad.loc_code_loc,
    sad.def_is_decathlon,
    sad.def_quantite,
    month(def_date_validation) as month_tdt_date_transaction,
    year(def_date_validation) as year_tdt_date_transaction,
    weekofyear(def_date_validation) as week_tdt_date_transaction,
    def.etr_label as label_defect,
    loc.etr_label as label_loc,
    org.purch_org,
    org.purch_org_text,
    org.sales_org,
    org.sales_org_text,
    org.distrib_channel
FROM
    defects sad
LEFT JOIN
    (SELECT DISTINCT
     sku_num_sku_r3,
     mdl_num_model_r3,
     sku_date_begin,
     sku_date_end
FROM
    d_sku_h where sku_num_sku_r3 is not null) sku
ON
    sku.sku_num_sku_r3=sad.def_code_article_ext
and sku.sku_date_begin <= sad.def_date_validation
and sku.sku_date_end >= sad.def_date_validation
LEFT JOIN
    kyl_org org
ON
    cast(org.plant as int)= cast(sad.tir_num_tiers_tir as int)
LEFT JOIN
    label_texts_defloc def
ON
    ( def.etr_element_id = sad.dft_code_dft and def.lan_language_code_lan = 'EN' and def.etr_element_type = 3)
LEFT JOIN
    label_texts_defloc  loc
ON
    (loc.etr_element_id = sad.loc_code_loc  and loc.lan_language_code_lan = 'EN' and loc.etr_element_type = 2 )
WHERE 
    sku.sku_num_sku_r3 is not null 
and sad.def_quantite != 0
 """)

tableFinale = tableFinale.dropDuplicates()
tableFinale.registerTempTable("table_finale")

tableFinale = sqlContext.sql("""
SELECT 
    tf.* ,
    cast(QF.vendor_id as int),
    cast(QF.subcontractor_id as int),
    QF.price_cession
FROM 
    table_finale tf
LEFT JOIN
  (SELECT
        MATERIAL_ID,
        PURCH_ORG,
        VENDOR_ID,
        CURRENCY_ID,
        DATE_MAX,
        MAX_CMDE,
        PRICE_CESSION,
        MDL_NUM_MODEL_R3,
        SUBCONTRACTOR_ID,
        row_number() over (partition by material_id,mdl_num_model_r3,purch_org order by date_max desc) as rn
FROM
    QUOTAS_FOURNISSEURS) QF
ON
    cast(QF.material_id as integer)=cast(tf.sku_num_sku_r3 as integer)
and qf.purch_org=tf.purch_org
and QF.rn=1
""")

tableFinale.repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.kyl_mqb_def")

# Stopping Spark
spark.stop()
