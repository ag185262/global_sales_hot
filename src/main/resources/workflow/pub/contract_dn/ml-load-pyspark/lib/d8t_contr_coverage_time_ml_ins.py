from pyspark import SparkConf, SparkContext, SQLContext 
from pyspark.sql import SparkSession, HiveContext 
import sys 
import logging 
import db_params

class d8tcontrcoveragetimemlinssql:

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
            sparkSession.sql("""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_oks_erpprod.oks_coverage_times) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_COVERAGE_TIME_ML_time select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_COVERAGE_TIME_ML_time inner join QQ1 on 1=1""")

            counter = counter + 1
            print("Running query number: " + str(counter))
            #Original Query:
            """
                DELETE FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_coverage_time_ml
            """
            sparkSession.sql("""    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_coverage_time_ml""")

            counter = counter + 1
            print("Running query number: " + str(counter))
            #Original Query:
            """
                INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_coverage_time_ml(        contr_cover_time_id,        instance_id,        alt_contract_header_id,        as_of_date_time,        batch_nbr,        contr_cover_timezone_id,        created_by_name,        creation_date_time,        end_hour_nbr,        end_minute_nbr,        fri_coverage_flag,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        mon_coverage_flag,        sat_coverage_flag,        start_hour_nbr,        start_minute_nbr,        sun_coverage_flag,        thu_coverage_flag,        tran_code,        tue_coverage_flag,        wed_coverage_flag)SELECT        contr_cover_time_id,        instance_id,        alt_contract_header_id,        as_of_date_time,        batch_nbr,        contr_cover_timezone_id,        created_by_name,        creation_date_time,        end_hour_nbr,        end_minute_nbr,        fri_coverage_flag,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        mon_coverage_flag,        sat_coverage_flag,        start_hour_nbr,        start_minute_nbr,        sun_coverage_flag,        thu_coverage_flag,        tran_code,        tue_coverage_flag,        wed_coverage_flagFROM ${EEDDWWDDBB}.${AAPPLLIIDD1}_CONTR_COVERAGE_TIME_INT EDW1, ${EEDDWWDDBB}.d8_cfs_time EDW2WHERE EDW1.crt_date_time>EDW2.min_timestamp and EDW1.crt_date_time<= EDW2.max_timestampand EDW2.tablename='${AAPPLLIIDD1EENNVV}_contr_coverage_time_ml'
            """
            sparkSession.sql("""insert into """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_COVERAGE_TIME_ML select ID, '8001', DNZ_CHR_ID, from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'),'yyyy-MM-dd HH:mm:ss'), """ + db_params.btch_nbr + """, COV_TZE_LINE_ID, CREATED_BY, CREATION_DATE, END_HOUR, END_MINUTE, FRIDAY_YN, LAST_UPDATE_DATE, LAST_UPDATE_LOGIN, LAST_UPDATED_BY, MONDAY_YN, SATURDAY_YN, START_HOUR, START_MINUTE, SUNDAY_YN, THURSDAY_YN, dtl__capxaction, TUESDAY_YN, WEDNESDAY_YN from(select tb1.ID, tb1.DNZ_CHR_ID, tb1.DTL__CAPXTIMESTAMP, tb1.COV_TZE_LINE_ID, tb1.CREATED_BY, tb1.CREATION_DATE, tb1.END_HOUR, tb1.END_MINUTE, tb1.FRIDAY_YN, tb1.LAST_UPDATE_DATE, tb1.LAST_UPDATE_LOGIN, tb1.LAST_UPDATED_BY, tb1.MONDAY_YN, tb1.SATURDAY_YN, tb1.START_HOUR, tb1.START_MINUTE, tb1.SUNDAY_YN, tb1.THURSDAY_YN, tb1.dtl__capxaction, tb1.TUESDAY_YN, tb1.WEDNESDAY_YN, row_number() over (partition by tb1.ID order by tb1.DTL__CAPXTIMESTAMP desc,tb1.edl_record_sequence desc) as rnk from trans_cfocontroller_erp_oks_erpprod.oks_coverage_times tb1,""" + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_COVERAGE_TIME_ML_time QQ1 where edl_ingest_time > QQ1.start_tm and edl_ingest_time <= QQ1.end_tm)tb1 where tb1.rnk=1  """)

        except:
            raise Exception("Error while executing query number {} in the job '{}'.".format(str(counter), "d8tcontrcoveragetimemlinssql"))
        finally:
            print("Execution complete for job 'd8tcontrcoveragetimemlinssql'.")

def main():
    m = d8tcontrcoveragetimemlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontrcoveragetimemlinssql").enableHiveSupport().getOrCreate()
    main()