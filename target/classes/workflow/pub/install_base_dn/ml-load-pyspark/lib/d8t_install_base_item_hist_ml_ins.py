from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tinstallbaseitemhistmlinspy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = """with QQ1 as (select max(edl_ingest_time) as max_time from trans_serviceslob_erp_csi_erpprod.csi_item_instances_h) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_ml_time select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_ml_time inner join QQ1 on 1=1"""
        print("Job: 'd8t_install_base_item_hist_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ TRUNCATE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_ml"""
        print("Job: 'd8t_install_base_item_hist_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ insert into """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_ML select instance_history_id,8001,c11,-1,created_by,creation_date,instance_id,last_update_date,last_update_login,last_updated_by,migrated_flag,dtl__capxaction,transaction_id,new_attribute8,old_attribute8,new_attribute29,old_attribute29,new_attribute30,old_attribute30,null,null,null,null,null,null,null from( select instance_history_id,8001,from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'),'yyyy-MM-dd HH:mm:ss') as c11,-1,created_by,creation_date,instance_id,last_update_date,last_update_login,last_updated_by,migrated_flag,dtl__capxaction,transaction_id,new_attribute8,old_attribute8,new_attribute29,old_attribute29,new_attribute30,old_attribute30,edl_ingest_time,row_number() over (partition by instance_history_id order by edl_ingest_time desc, edl_record_sequence desc) as row_nm from trans_serviceslob_erp_csi_erpprod.csi_item_instances_h,""" + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_ml_time QQ1 where edl_ingest_time > QQ1.start_tm and edl_ingest_time <= QQ1.end_tm) X where row_nm=1"""
        print("Job: 'd8t_install_base_item_hist_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tinstallbaseitemhistmlinspy(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tinstallbaseitemhistmlinspy").enableHiveSupport().getOrCreate()
    main()
