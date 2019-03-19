create table kylin_usb_mqb.f_transaction_detail(
the_transaction_id   String,
tdt_num_line   Integer,
but_idr_business_unit   Integer,
sku_idr_sku   Integer,
f_qty_item   Integer,
tdt_type_detail   String)
partitioned by (year_transaction string, month_transaction string, day_transaction string)
location 'hdfs:///user/hive/warehouse/kylin_usb_mqb.db/f_transaction_detail/';

create table kylin_usb_mqb.etapes_de_vie
(
the_transaction_id string,
tdt_num_line int,
eta_num_etape_eta string,
sku_num_sku_r3 int,
tdt_date_transaction timestamp
)
partitioned by (year_transaction string, month_transaction string)
location 'hdfs:///user/hive/warehouse/kylin_usb_mqb.db/etapes_de_vie/';

create table kylin_usb_mqb.kyl_mqb_vte (
the_transaction_id string,
week_tdt_date_transaction int,
quarter_tdt_date_transaction int,
tdt_date_transaction int,
but_num_business_unit int,
but_name_business_unit string,
sku_num_sku_r3 int,
mdl_num_model_r3 int,
purch_org string,
sales_org string,
sales_org_text string,
purch_org_text string,
distrib_channel string,
ege_basique string,
ege_security_product string,
eta_num_etape_eta string,
f_qty_item float,
vendor_id int,
subcontractor_id int)
partitioned by (year_transaction string, month_transaction string)
location 'hdfs:///user/hive/warehouse/kylin_usb_mqb.db/kyl_mqb_vte/';


create table kylin_usb_mqb.d_sku_h(
sku_idr_sku int,
sku_num_sku int,
sku_num_sku_r3 int,
mdl_num_model_r3 int)
partitioned by (year_sku string, month_sku string, day_sku string)
location 'hdfs:///user/hive/warehouse/kylin_usb_mqb.db/d_sku_h/';

create table kylin_usb_mqb.mds_elt_gestion_echange(
elg_num_elt_gestion_elg int,
eta_num_etape_eta int)
partitioned by (year_mge string, month_mge string, day_mge string)
location 'hdfs:///user/hive/warehouse/kylin_usb_mqb.db/mds_elt_gestion_echange/';


