from pyspark import SparkConf, SparkContext, SQLContext 
from pyspark.sql import SparkSession, HiveContext 
import sys 
import logging 
import db_params

class d8tcontractiontimemlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        counter = 0
        try:

            counter = counter + 1
            print("Running query number: " + str(counter))
            #Original Query:
            """
            """
            sparkSession.sql("""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_oks_erpprod.oks_action_times) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_ACTION_TIME_ML_time select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_ACTION_TIME_ML_time inner join QQ1 on 1=1""")

            counter = counter + 1
            print("Running query number: " + str(counter))
            #Original Query:
            """
                DELETE FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_action_time_ml
            """
            sparkSession.sql("""    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_action_time_ml""")

            counter = counter + 1
            print("Running query number: " + str(counter))
            #Original Query:
            """
                INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_action_time_ml(        action_time_id,        instance_id,        action_time_type_id,        alt_contract_header_id,        as_of_date_time,        batch_nbr,        contract_line_id,        created_by_name,        creation_date_time,        fri_mnt_duration,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        mon_mnt_duration,        sat_mnt_duration,        sun_mnt_duration,        thu_mnt_duration,        tran_code,        tue_mnt_duration,        uom_code,        wed_mnt_duration)SELECT        action_time_id,        instance_id,        action_time_type_id,        alt_contract_header_id,        as_of_date_time,        batch_nbr,        contract_line_id,        created_by_name,        creation_date_time,        fri_mnt_duration,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        mon_mnt_duration,        sat_mnt_duration,        sun_mnt_duration,        thu_mnt_duration,        tran_code,        tue_mnt_duration,        uom_code,        wed_mnt_durationFROM ${EEDDWWDDBB}.${AAPPLLIIDD1}_CONTR_ACTION_TIME_INT EDW1, ${EEDDWWDDBB}.d8_cfs_time EDW2WHERE EDW1.crt_date_time>EDW2.min_timestamp and EDW1.crt_date_time<= EDW2.max_timestampand EDW2.tablename='${AAPPLLIIDD1EENNVV}_contr_action_time_ml'
            """
            sparkSession.sql("""insert into """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_ACTION_TIME_ML select ID, '8001', COV_ACTION_TYPE_ID, DNZ_CHR_ID, from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'),'yyyy-MM-dd HH:mm:ss'), """ + db_params.btch_nbr + """, CLE_ID, CREATED_BY, CREATION_DATE, FRI_DURATION, LAST_UPDATE_DATE, LAST_UPDATE_LOGIN, LAST_UPDATED_BY, MON_DURATION, SAT_DURATION, SUN_DURATION, THU_DURATION, dtl__capxaction, TUE_DURATION, UOM_CODE, WED_DURATION from (select tb1.ID, tb1.COV_ACTION_TYPE_ID, tb1.DNZ_CHR_ID, tb1.DTL__CAPXTIMESTAMP, tb1.CLE_ID, tb1.CREATED_BY, tb1.CREATION_DATE, tb1.FRI_DURATION, tb1.LAST_UPDATE_DATE, tb1.LAST_UPDATE_LOGIN, tb1.LAST_UPDATED_BY, tb1.MON_DURATION, tb1.SAT_DURATION, tb1.SUN_DURATION, tb1.THU_DURATION, tb1.dtl__capxaction, tb1.TUE_DURATION, tb1.UOM_CODE, tb1.WED_DURATION, row_number() over (partition by tb1.ID order by tb1.DTL__CAPXTIMESTAMP desc,tb1.edl_record_sequence desc) as rnk from trans_cfocontroller_erp_oks_erpprod.oks_action_times tb1,""" + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_ACTION_TIME_ML_time QQ1 where  tb1.edl_ingest_time > QQ1.start_tm and tb1.edl_ingest_time <= QQ1.end_tm)tb1 where tb1.rnk=1 """)

        except:
            raise Exception("Error while executing query number {} in the job '{}'.".format(str(counter), "d8tcontractiontimemlinssql"))
        finally:
            print("Execution complete for job 'd8tcontractiontimemlinssql'.")

def main():
    m = d8tcontractiontimemlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontractiontimemlinssql").enableHiveSupport().getOrCreate()
    main()