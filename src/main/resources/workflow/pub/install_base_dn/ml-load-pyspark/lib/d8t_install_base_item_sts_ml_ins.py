from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tinstallbaseitemstsmlinspy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """with QQ1 as (select max(edl_ingest_time) as max_time from trans_serviceslob_erp_csi_erpprod.CSI_INSTANCE_STATUSES) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_ml_time select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_ml_time inner join QQ1 on 1=1"""
        print("Job: 'd8t_install_base_item_sts_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ TRUNCATE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_ml"""
        print("Job: 'd8t_install_base_item_sts_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ INSERT INTO """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_ml SELECT 8001 AS auto_c021, INSTANCE_STATUS_ID, END_DATE_ACTIVE, START_DATE_ACTIVE, FROM_UNIXTIME( UNIX_TIMESTAMP(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss' ) AS auto_c01, -1 AS auto_c00, CREATED_BY, CREATION_DATE, INCIDENT_ALLOWED_FLAG, DESCRIPTION, NAME, LAST_UPDATE_DATE, LAST_UPDATE_LOGIN, LAST_UPDATED_BY, MIGRATED_FLAG, SEED_STATUS_UPDATEABLE_FLAG, SEEDED_FLAG, SERVICE_ORDER_ALLOWED_FLAG, STATUS_CHANGE_ALLOWED_FLAG, TERMINATED_FLAG, DTL__CAPXACTION, UPDATEABLE_FLAG FROM trans_serviceslob_erp_csi_erpprod.CSI_INSTANCE_STATUSES,""" + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_ml_time QQ1 where edl_ingest_time > QQ1.start_tm and edl_ingest_time <= QQ1.end_tm"""
        print("Job: 'd8t_install_base_item_sts_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tinstallbaseitemstsmlinspy(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tinstallbaseitemstsmlinspy").enableHiveSupport().getOrCreate()
    main()