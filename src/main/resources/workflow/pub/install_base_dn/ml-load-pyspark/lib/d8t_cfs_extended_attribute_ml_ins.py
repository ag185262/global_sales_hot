from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tcfsextendedattributemlinspy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """with QQ1 as (select max(edl_ingest_time) as max_time from trans_serviceslob_erp_csi_erpprod.csi_i_extended_attribs) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_extended_attribute_ml_time select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_extended_attribute_ml_time inner join QQ1 on 1=1"""
        print("Job: 'd8t_cfs_extended_attribute_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ TRUNCATE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_extended_attribute_ml"""
        print("Job: 'd8t_cfs_extended_attribute_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ insert into """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_extended_attribute_ML SELECT attribute_id, 8001 AS auto_c019, active_end_date, active_start_date, from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS')) AS c1, attribute_category, attribute_code, description, attribute_level, attribute_name, -1 AS auto_c00, created_by, creation_date, master_organization_id, inventory_item_id, item_category_id, last_update_date, last_update_login, last_updated_by, dtl__capxaction FROM trans_serviceslob_erp_csi_erpprod.csi_i_extended_attribs,""" + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_extended_attribute_ml_time QQ1 where edl_ingest_time > QQ1.start_tm and edl_ingest_time <= QQ1.end_tm"""
        print("Job: 'd8t_cfs_extended_attribute_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tcfsextendedattributemlinspy(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcfsextendedattributemlinspy").enableHiveSupport().getOrCreate()
    main()