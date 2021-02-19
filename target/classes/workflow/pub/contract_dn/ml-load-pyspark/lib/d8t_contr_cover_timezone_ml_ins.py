from pyspark import SparkConf, SparkContext, SQLContext 
from pyspark.sql import SparkSession, HiveContext 
import sys 
import logging 
import db_params

class d8tcontrcovertimezonemlinssql:

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
            sparkSession.sql("""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_oks_erpprod.oks_coverage_timezones) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_COVER_TIMEZONE_ML_time select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_COVER_TIMEZONE_ML_time inner join QQ1 on 1=1""")

            counter = counter + 1
            print("Running query number: " + str(counter))
            #Original Query:
            """
                DELETE FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_cover_timezone_ml
            """
            sparkSession.sql("""    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_cover_timezone_ml""")

            counter = counter + 1
            print("Running query number: " + str(counter))
            #Original Query:
            """
                INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_cover_timezone_ml(        contr_cover_timezone_id,        instance_id,        alt_contract_header_id,        as_of_date_time,        batch_nbr,        contract_line_id,        created_by_name,        creation_date_time,        default_flag,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        time_zone_id,        tran_code)SELECT        contr_cover_timezone_id,        instance_id,        alt_contract_header_id,        as_of_date_time,        batch_nbr,        contract_line_id,        created_by_name,        creation_date_time,        default_flag,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        time_zone_id,        tran_codeFROM ${EEDDWWDDBB}.${AAPPLLIIDD1}_CONTR_COVER_TIMEZONE_INT EDW1, ${EEDDWWDDBB}.d8_cfs_time EDW2WHERE EDW1.crt_date_time>EDW2.min_timestamp and EDW1.crt_date_time<= EDW2.max_timestampand EDW2.tablename='${AAPPLLIIDD1EENNVV}_contr_cover_timezone_ml'
            """
            sparkSession.sql("""insert into """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_COVER_TIMEZONE_ML select ID, '8001', DNZ_CHR_ID, from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'),'yyyy-MM-dd HH:mm:ss'), """ + db_params.btch_nbr + """, CLE_ID, CREATED_BY, CREATION_DATE, DEFAULT_YN, LAST_UPDATE_DATE, LAST_UPDATE_LOGIN, LAST_UPDATED_BY, TIMEZONE_ID, dtl__capxaction from (select tb1.ID, tb1.DNZ_CHR_ID, tb1.DTL__CAPXTIMESTAMP, tb1.CLE_ID, tb1.CREATED_BY, tb1.CREATION_DATE, tb1.DEFAULT_YN, tb1.LAST_UPDATE_DATE, tb1.LAST_UPDATE_LOGIN, tb1.LAST_UPDATED_BY, tb1.TIMEZONE_ID, tb1.dtl__capxaction, row_number() over (partition by tb1.ID order by tb1.DTL__CAPXTIMESTAMP desc,tb1.edl_record_sequence desc) as rnk from trans_cfocontroller_erp_oks_erpprod.oks_coverage_timezones tb1,""" + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_COVER_TIMEZONE_ML_time QQ1 where edl_ingest_time > QQ1.start_tm and edl_ingest_time <= QQ1.end_tm)tb1 where tb1.rnk=1 """)

        except:
            raise Exception("Error while executing query number {} in the job '{}'.".format(str(counter), "d8tcontrcovertimezonemlinssql"))
        finally:
            print("Execution complete for job 'd8tcontrcovertimezonemlinssql'.")

def main():
    m = d8tcontrcovertimezonemlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontrcovertimezonemlinssql").enableHiveSupport().getOrCreate()
    main()