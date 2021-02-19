from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tinstallbasevrsnlblmlinspy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """with QQ1 as (select max(edl_ingest_time) as max_time from trans_serviceslob_erp_csi_erpprod.CSI_I_VERSION_LABELS) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_ml_time select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_ml_time inner join QQ1 on 1=1"""
        print("Job: 'd8t_install_base_vrsn_lbl_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ TRUNCATE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_ml"""
        print("Job: 'd8t_install_base_vrsn_lbl_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ INSERT INTO """ + db_params.TTMMPPDDBB + """. """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_ml SELECT X.c17, X.VERSION_LABEL_ID, X.ACTIVE_END_DATE, X.ACTIVE_START_DATE, X.c2, c1, X.CREATED_BY, X.CREATION_DATE, X.INSTANCE_ID, X.LAST_UPDATE_DATE, X.LAST_UPDATE_LOGIN, X.LAST_UPDATED_BY, X.MIGRATED_FLAG, X.c3, X.DATE_TIME_STAMP, X.DESCRIPTION, X.VERSION_LABEL FROM ( SELECT -1 AS c1, FROM_UNIXTIME( UNIX_TIMESTAMP(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss' ) AS c2, DTL__CAPXACTION AS c3, VERSION_LABEL_ID AS VERSION_LABEL_ID, INSTANCE_ID AS INSTANCE_ID, VERSION_LABEL AS VERSION_LABEL, DATE_TIME_STAMP AS DATE_TIME_STAMP, DESCRIPTION AS DESCRIPTION, ACTIVE_START_DATE AS ACTIVE_START_DATE, ACTIVE_END_DATE AS ACTIVE_END_DATE, CREATED_BY AS CREATED_BY, CREATION_DATE AS CREATION_DATE, LAST_UPDATED_BY AS LAST_UPDATED_BY, LAST_UPDATE_DATE AS LAST_UPDATE_DATE, LAST_UPDATE_LOGIN AS LAST_UPDATE_LOGIN, MIGRATED_FLAG AS MIGRATED_FLAG, 8001 AS c17, edl_record_sequence AS edl_record_sequence, ROW_NUMBER() OVER ( PARTITION BY VERSION_LABEL_ID ORDER BY edl_ingest_time desc, edl_record_sequence desc ) AS row_nm, edl_ingest_time AS edl_ingest_time FROM trans_serviceslob_erp_csi_erpprod.CSI_I_VERSION_LABELS,""" + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_ml_time QQ1 where edl_ingest_time > QQ1.start_tm and edl_ingest_time <= QQ1.end_tm ) AS X where X.row_nm = 1"""
        print("Job: 'd8t_install_base_vrsn_lbl_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tinstallbasevrsnlblmlinspy(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tinstallbasevrsnlblmlinspy").enableHiveSupport().getOrCreate()
    main()