from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tcfsattributevaluemlinspy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """with QQ1 as (select max(edl_ingest_time) as max_time from trans_serviceslob_erp_csi_erpprod.csi_iea_values) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_attribute_value_ml_time select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_attribute_value_ml_time inner join QQ1 on 1=1"""
        print("Job: 'd8t_cfs_attribute_value_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """TRUNCATE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_attribute_value_ml"""
        print("Job: 'd8t_cfs_attribute_value_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ insert into """ + db_params.TTMMPPDDBB + """ .""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_attribute_value_ml select -1,c11,dtl__capxaction,attribute_value_id,attribute_id,instance_id,attribute_value,active_start_date,active_end_date,created_by,creation_date,last_updated_by,last_update_date,last_update_login,8001 from ( select -1,from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'),'yyyy-MM-dd HH:mm:ss') as c11,dtl__capxaction,attribute_value_id,attribute_id,instance_id,attribute_value,active_start_date,active_end_date,created_by,creation_date,last_updated_by,last_update_date,last_update_login,edl_ingest_time,row_number() over (partition by instance_id,attribute_id order by edl_ingest_time desc) as row_num from trans_serviceslob_erp_csi_erpprod.csi_iea_values,""" + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_attribute_value_ml_time QQ1 where edl_ingest_time > QQ1.start_tm and edl_ingest_time <= QQ1.end_tm) x where x.row_num=1"""
        print("Job: 'd8t_cfs_attribute_value_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tcfsattributevaluemlinspy(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcfsattributevaluemlinspy").enableHiveSupport().getOrCreate()
    main()