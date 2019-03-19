from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

# Curl Headers
HEADERS = {'Content-type': 'application/json'}

# Kylin authentication
AUTH = ("admin", "KYLIN")

# Defining workspace
workspace = "/home/hadoop/"

# Environment
environment = "KYLIN_USB"

# Spark Master Address
IP = "10.0.76.118"

# loading paths
PLfacts = workspace + environment + "/sources/spark/load/facts/"
PLsources = workspace + environment + "/sources/spark/load/sources/"
PLtransform = workspace + environment + "/sources/spark/load/transform/"
PLhierarchies = workspace + environment + "/sources/spark/load/hierarchies/"
PLdpp = workspace + environment + "/sources/spark/load/dpp/"
PLcubes = workspace + environment + "/sources/spark/load/cubes/"

# metadata paths
PMfacts = workspace + environment + "/sources/spark/metadata/facts/"
PMsources = workspace + environment + "/sources/spark/metadata/sources/"
PMtransform = workspace + environment + "/sources/spark/metadata/transform/"
PMhierarchies = workspace + environment + "/sources/spark/metadata/hierarchies/"
PMdpp = workspace + environment + "/sources/spark/metadata/dpp/"
PMcubes = workspace + environment + "/sources/spark/metadata/cubes/"

project = "pprod_mqb_usb"
spark_conf_path = workspace + environment + "/sources/spark/confs/"
# Create logs directory
log_path = workspace + environment + "/sources/spark/spark_log/"

if not os.path.exists(log_path):
    os.makedirs(log_path)

# define time of execution (day of the week)
instant = datetime.isoweekday(datetime.now())
time = datetime.now()
delta = relativedelta(months=1)
start = datetime(2017, 1, 1)
#
# Defining name of spark scripts we are going to need to execute
Tfacts = ['001-f_transaction_detail.py', '004-stc_articles_defectueux.py', '014-opv_review_review.py',
          '015-nwt_intervention.py', '016-nwt_itv_service.py', '017-nwt_itv_spare_parts.py', '024-nwt_itv_def_loc.py',
          '025-nwt_itv_event.py', '026-nwt_itv_model.py']
Tsources = ['002-d_business_unit.py', '003-d_sku_h.py', '005-language_defloc.py', '006-t001w.py',
            '007-sales_organizations_texts.py', '008-purchases_organizations_texts.py', '009-wrf1.py',
            '010-mds_detail_elt_gestion.py', '011-mds_item_and_model_production_data.py',
            '012-mds_codification_fournisseur.py', '018-mds_rattachement_modele.py', '019-mds_libelle_modele.py',
            '020-mds_elt_niveau.py', '021-mds_libelle_traduction.py', '022-mds_element_gestion.py',
            '023-process_type_label.py', '027-mds_declinaison_article.py', '028-mds_val_grille.py',
            '029-quotas_fournisseurs.py', '030-adresses_cds.py', '031-d_currency.py', '032-f_currency_exchange.py',
            '033-mds_elt_gestion_echange.py', '034-dk_product.py']
Thierarchies = ['hierarchies.py']
Tdpp = ['dpp_hierarchie_article.py', 'dpp_hierarchie_modele.py']
Ttransform = ['etapes_de_vie.py', 't001w_finale.py', 'flat_structure.py']
Tcubes = ['kyl_mqb_vte.py', 'kyl_mqb_def.py', 'kyl_mqb_opv.py', 'kyl_mqb_nwt.py']
