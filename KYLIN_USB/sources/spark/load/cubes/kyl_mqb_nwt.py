# import libraries
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G") \
    .set("spark.sql.cbo.enabled", "true")
# Initialize spark session
spark = SparkSession.builder.appName("ALIM_KYL_VTE_NWT").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

sc = spark.sparkContext
sqlContext = HiveContext(sc)

# Defining database used
sqlContext.sql("use kylin_usb_mqb")

newton1 = sqlContext.sql("""
SELECT
    ITV.OSR_SERVER_ALIAS_NAME,
    cast(FS.FST_ARTICLE_R3 as integer) AS REPAIR_DETAIL,
    cast(FSA.FST_ARTICLE_R3 as integer) AS MATERIAL,
    ITV.ITV_DOCUMENT_NUMBER AS NOBON,
    0 AS CATEG,
    0 AS DEFAUT,
    0 AS LOC_LOCAL,
    ISP.ISP_QUANTITY AS QTE,
    CASE WHEN ISP.ISP_ID_OPERATION_STATUS = 2 THEN 'B' WHEN ISP.ISP_ID_OPERATION_STATUS = 5 THEN 'H' END AS TYPECH,
    cast(IE1.ITE_DATE as date) AS DATEAC,
    cast(IE2.ITE_DATE as date) AS DATEVA,
    cast(IE3.ITE_DATE as date) AS DATEEM,
    --cast(SUBSTRING(ITV.ITV_CREATION_SITE,9) as int) as TIERS,
    case 
        when cast(SUBSTRING(ITV.ITV_CREATION_SITE,1,3) as int)=700 then cast(SUBSTRING(ITV.ITV_CREATION_SITE,9) as int)
        when cast(SUBSTRING(ITV.ITV_CREATION_SITE,1,3) as int) in (701,702,703) then cast(SUBSTRING(ITV.ITV_CREATION_SITE,8) as int)
    end AS TIERS, 
    QF.PURCH_ORG,
    QF.VENDOR_ID,
    QF.DATE_MAX,
    QF.CURRENCY_ID,
    QF.MAX_CMDE,
    QF.PRICE_CESSION,
    QF.MDL_NUM_MODEL_R3,
    QF.SUBCONTRACTOR_ID
FROM
    NWT_INTERVENTION ITV
INNER JOIN
    NWT_ITV_SPARE_PARTS ISP
ON
    ISP.ISP_ID_INTERVENTION = ITV.ITV_ID
AND ISP.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
AND ISP.ISP_ID_OPERATION_STATUS IN (2,5)
INNER JOIN
    FLAT_STRUCTURE FS
ON
    FS.ELG_NUM_ELT_GESTION_ELG = ISP.ISP_INTERNAL_CODE
AND FS.ORG_NUM_ORGANISATION_ORG=2
LEFT JOIN
    NWT_ITV_EVENT IE1
ON
    IE1.ITV_ID = ITV.ITV_ID
AND IE1.ITE_EVENT_ID = 2
AND IE1.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
LEFT JOIN
    NWT_ITV_EVENT IE2
ON
    IE2.ITV_ID = ITV.ITV_ID
AND IE2.ITE_EVENT_ID = 8
AND IE2.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
LEFT JOIN
    NWT_ITV_EVENT IE3
ON
    IE3.ITV_ID = ITV.ITV_ID
AND IE3.ITE_EVENT_ID = 1
AND IE3.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
LEFT JOIN
    NWT_ITV_MODEL LSM
ON ITV.ITV_ID = LSM.ITV_ID
AND LSM.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
INNER JOIN
    FLAT_STRUCTURE FSA
ON FSA.ELG_NUM_ELT_GESTION_ELG = LSM.IMO_INTERNAL_CODE
AND FSA.ORG_NUM_ORGANISATION_ORG=2
INNER JOIN
    t001w2 t001w
ON
    cast(SUBSTRING(ITV.ITV_CREATION_SITE, 9) as int)=t001w.plant
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
        row_number() over (partition by material_id,purch_org order by date_max desc) as rn
FROM
    QUOTAS_FOURNISSEURS) QF
ON
    cast(QF.MATERIAL_ID as integer)=cast(FSA.FST_ARTICLE_R3 as integer)
and qf.purch_org=t001w.purch_org
and QF.RN=1
""")

newton2 = sqlContext.sql("""
SELECT
    ITV.OSR_SERVER_ALIAS_NAME,
    cast(FS.FST_ARTICLE_R3 as integer) AS REPAIR_DETAIL,
    cast(FSA.FST_ARTICLE_R3 as integer) AS MATERIAL,
    ITV.ITV_DOCUMENT_NUMBER AS NOBON,
    0 AS CATEG,
    0 AS DEFAUT,
    0 AS LOC_LOCAL,
    ISV.ISV_QUANTITY AS QTE,
    CASE WHEN ISV.ISV_ID_OPERATION_STATUS = 2 THEN 'C'WHEN ISV.ISV_ID_OPERATION_STATUS = 5 THEN 'I' END AS TYPECH,
    cast(IE1.ITE_DATE as date) AS DATEAC,
    cast(IE2.ITE_DATE as date) AS DATEVA,
    cast(IE3.ITE_DATE as date) AS DATEEM,
    --cast(SUBSTRING(ITV.ITV_CREATION_SITE,9) as int) as TIERS,
    case 
        when cast(SUBSTRING(ITV.ITV_CREATION_SITE,1,3) as int)=700 then cast(SUBSTRING(ITV.ITV_CREATION_SITE,9) as int)
        when cast(SUBSTRING(ITV.ITV_CREATION_SITE,1,3) as int) in (701,702,703) then cast(SUBSTRING(ITV.ITV_CREATION_SITE,8) as int)
    end AS TIERS, 
    QF.PURCH_ORG,
    QF.VENDOR_ID,
    QF.DATE_MAX,
    QF.CURRENCY_ID,
    QF.MAX_CMDE,
    QF.PRICE_CESSION,
    QF.MDL_NUM_MODEL_R3,
    QF.SUBCONTRACTOR_ID
FROM
    NWT_INTERVENTION ITV
INNER JOIN
    NWT_ITV_SERVICE ISV
ON
    ISV.ISV_ID_INTERVENTION = ITV.ITV_ID
AND ISV.OSR_SERVER_ALIAS_NAME=ITV.OSR_SERVER_ALIAS_NAME
AND ISV.ISV_ID_OPERATION_STATUS IN (2,5)
INNER JOIN
    FLAT_STRUCTURE FS
ON
    FS.ELG_NUM_ELT_GESTION_ELG = ISV.ISV_CODE_ARTICLE
AND FS.ORG_NUM_ORGANISATION_ORG=2
LEFT JOIN
    NWT_ITV_EVENT IE1
ON
    IE1.ITV_ID = ITV.ITV_ID
AND IE1.ITE_EVENT_ID = 2
AND IE1.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
LEFT JOIN
    NWT_ITV_EVENT IE2
ON
    IE2.ITV_ID = ITV.ITV_ID
AND IE2.ITE_EVENT_ID = 8
AND IE2.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
LEFT JOIN
    NWT_ITV_EVENT IE3
ON
    IE3.ITV_ID = ITV.ITV_ID
AND IE3.ITE_EVENT_ID = 1
AND IE3.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
LEFT JOIN
    NWT_ITV_MODEL LSM
ON
    ITV.ITV_ID = LSM.ITV_ID
AND LSM.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
INNER JOIN
    FLAT_STRUCTURE FSA
ON
    FSA.ELG_NUM_ELT_GESTION_ELG = LSM.IMO_INTERNAL_CODE
AND FSA.ORG_NUM_ORGANISATION_ORG=2
INNER JOIN
    t001w2 t001w
ON
    cast(SUBSTRING(ITV.ITV_CREATION_SITE, 9) as int)=t001w.plant
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
        row_number() over (partition by material_id,purch_org order by date_max desc) as rn
FROM
    QUOTAS_FOURNISSEURS) QF
ON
    cast(QF.MATERIAL_ID as integer)=cast(FSA.FST_ARTICLE_R3 as integer)
and qf.purch_org=t001w.purch_org
and QF.RN=1
""")

newton3 = sqlContext.sql("""
SELECT
    ITV.OSR_SERVER_ALIAS_NAME,
    0 AS REPAIR_DETAIL,
    cast(FSA.FST_ARTICLE_R3 as integer) AS MATERIAL,
    ITV.ITV_DOCUMENT_NUMBER AS NOBON,
    CASE WHEN IDL_CATEGORY_ID IS NOT NULL THEN IDL.IDL_CATEGORY_ID ELSE 0 END AS CATEG,
    CASE WHEN IDL.IDL_DEFAULT_ID IS NOT NULL THEN IDL.IDL_DEFAULT_ID ELSE 0 END AS DEFAUT,
    CASE WHEN IDL.IDL_LOCALISATION_ID IS NOT NULL THEN IDL.IDL_LOCALISATION_ID ELSE 0 END AS LOC_LOCAL,
    1 AS QTE,
    CASE WHEN ITV.ITV_ID_INTERVENTION_TYPE = 4 THEN 'G' ELSE 'A' END AS TYPECH,
    cast(IE1.ITE_DATE as date) AS DATEAC,
    cast(IE2.ITE_DATE as date) AS DATEVA,
    cast(IE3.ITE_DATE as date) AS DATEEM,
    --cast(SUBSTRING(ITV.ITV_CREATION_SITE,9) as int) as TIERS,
    case 
       when cast(SUBSTRING(ITV.ITV_CREATION_SITE,1,3) as int)=700 then cast(SUBSTRING(ITV.ITV_CREATION_SITE,9) as int)
       when cast(SUBSTRING(ITV.ITV_CREATION_SITE,1,3) as int) in (701,702,703) then cast(SUBSTRING(ITV.ITV_CREATION_SITE,8) as int)
    end AS TIERS, 
    QF.PURCH_ORG,
    QF.VENDOR_ID,
    QF.DATE_MAX,
    QF.CURRENCY_ID,
    QF.MAX_CMDE,
    QF.PRICE_CESSION,
    QF.MDL_NUM_MODEL_R3,
    QF.SUBCONTRACTOR_ID
FROM
    NWT_INTERVENTION ITV
LEFT JOIN
    NWT_ITV_MODEL ITM
ON
    ITM.ITV_ID = ITV.ITV_ID
AND ITM.OSR_SERVER_ALIAS_NAME=ITV.OSR_SERVER_ALIAS_NAME
INNER JOIN
    FLAT_STRUCTURE FS
ON
    FS.ELG_NUM_ELT_GESTION_ELG = ITM.IMO_INTERNAL_CODE
AND FS.ORG_NUM_ORGANISATION_ORG=2
LEFT JOIN
    NWT_ITV_DEF_LOC IDL
ON
    IDL.ITV_ID = ITV.ITV_ID
AND IDL.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
LEFT JOIN
    NWT_ITV_EVENT IE1
ON
    IE1.ITV_ID = ITV.ITV_ID
AND IE1.ITE_EVENT_ID = 2
AND IE1.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
LEFT JOIN
    NWT_ITV_EVENT IE2
ON
    IE2.ITV_ID = ITV.ITV_ID
AND IE2.ITE_EVENT_ID = 8
AND IE2.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
LEFT JOIN
    NWT_ITV_EVENT IE3
ON
    IE3.ITV_ID = ITV.ITV_ID
AND IE3.ITE_EVENT_ID = 1
AND IE3.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
--INNER JOIN
--    NWT_ITV_CUSTOMER ITC
--ON
--    ITC.ITV_ID = ITV.ITV_ID
LEFT JOIN
    NWT_ITV_MODEL LSM
ON
    ITV.ITV_ID = LSM.ITV_ID
AND LSM.OSR_SERVER_ALIAS_NAME = ITV.OSR_SERVER_ALIAS_NAME
INNER JOIN
    FLAT_STRUCTURE FSA
ON
    FSA.ELG_NUM_ELT_GESTION_ELG = ITM.IMO_INTERNAL_CODE
AND FSA.ORG_NUM_ORGANISATION_ORG=2
INNER JOIN
    t001w2 t001w
ON
    cast(SUBSTRING(ITV.ITV_CREATION_SITE, 9) as int)=t001w.plant
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
        row_number() over (partition by material_id,purch_org order by date_max desc) as rn
FROM
    QUOTAS_FOURNISSEURS) QF
ON
    cast(QF.MATERIAL_ID as integer)=cast(FSA.FST_ARTICLE_R3 as integer)
and qf.purch_org=t001w.purch_org
and QF.RN=1
""")
newton_final = newton1.union(newton2).union(newton3)
newton_final.registerTempTable("kyl_nwt")

newton = sqlContext.sql("""
SELECT DISTINCT
OSR_SERVER_ALIAS_NAME,
REPAIR_DETAIL,
cast(MATERIAL as int) as material,
NOBON,
CATEG,
DEFAUT,
LOC_LOCAL,
QTE,
TYPECH,
DATEAC,
DAY(DATEVA) as dateva,
DATEEM,
TIERS,
PURCH_ORG,
cast(VENDOR_ID as int) as vendor_id,
DATE_MAX,
CURRENCY_ID,
MAX_CMDE,
(PRICE_CESSION * CUR.HDE_SHARE_PRICE) as PRICE_CESSION,
cast(MDL_NUM_MODEL_R3 as int) as mdl_num_model_r3,
SUBCONTRACTOR_ID,
YEAR(nwt.dateva) as year_dateva,
MONTH(nwt.dateva) as month_dateva,
weekofyear(nwt.dateva) as week_dateva
FROM
kyl_nwt nwt
LEFT JOIN
(select
    cur_code_currency,
    cast(hde_effect_date as date) as date_ex,
    hde_share_price
from
    d_currency dcur
inner join
    f_currency_exchange fcure
on
    dcur.cur_idr_currency=fcure.cpt_idr_cur_price
) CUR
ON
    NWT.CURRENCY_ID=CUR.CUR_CODE_CURRENCY
AND NWT.DATEVA=CUR.date_ex
""")

newton.registerTempTable("newton")
query_action_with_one_line = """
select distinct
    A.*
from (
    select
        nobon,
        OSR_SERVER_ALIAS_NAME,
        count(*) as count_line
    from
        newton
    group by
        nobon,osr_server_alias_name) A
inner join
    newton B
on A.nobon=B.nobon
and A.OSR_SERVER_ALIAS_NAME=B.OSR_SERVER_ALIAS_NAME
where B.typech='A' and A.count_line=1"""
nobon_first_filter = sqlContext.sql(query_action_with_one_line)
nobon_first_filter.registerTempTable("to_exclude")

newton = """select
    *
from
    newton
"""
newton = sqlContext.sql(newton)
newton.registerTempTable("newton")

query = """
select
    nwt.*
from
    newton nwt
left join
    to_exclude ex
on
    nwt.nobon=ex.nobon
where ex.nobon is null"""
newton = sqlContext.sql(query)

newton.repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet")\
    .saveAsTable("kylin_usb_mqb.kyl_mqb_nwt")

# Stopping Session
sc.stop()
