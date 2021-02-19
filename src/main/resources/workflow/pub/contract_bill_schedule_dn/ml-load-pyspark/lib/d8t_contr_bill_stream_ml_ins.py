from pyspark import SparkConf, SparkContext, SQLContext 
from pyspark.sql import SparkSession, HiveContext 
import sys 
import logging 
import db_params

class d8tcontrbillstreammlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query="""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_oks_erpprod.oks_stream_levels_b) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_ml_time  select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_ml_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_ml"""
        print("Running query: " + query)
        sparkSession.sql(query)

        query = """INSERT  into """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_BILL_STREAM_ML 
    SELECT ID
    ,'8001'
    ,DNZ_CHR_ID
    ,from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss')
    ,""" + db_params.btch_nbr + """
    ,LEVEL_AMOUNT
    ,LEVEL_PERIODS
    ,UOM_PER_PERIOD
    ,UOM_CODE
    ,SEQUENCE_NO
    ,END_DATE
    ,START_DATE
    ,CHR_ID
    ,CLE_ID
    ,CREATED_BY
    ,CREATION_DATE
    ,INTERFACE_OFFSET_DAYS
    ,INVOICE_OFFSET_DAYS
    ,LAST_UPDATE_DATE
    ,LAST_UPDATE_LOGIN
    ,LAST_UPDATED_BY
    ,dtl__capxaction
FROM (
    SELECT tb1.ID
        ,NVL(tb1.DNZ_CHR_ID, '') DNZ_CHR_ID
        ,tb1.DTL__CAPXTIMESTAMP
        ,NVL(tb1.LEVEL_AMOUNT, 0) LEVEL_AMOUNT
        ,tb1.LEVEL_PERIODS
        ,tb1.UOM_PER_PERIOD
        ,NVL(tb1.UOM_CODE, '') UOM_CODE
        ,tb1.SEQUENCE_NO
        ,tb1.END_DATE
        ,tb1.START_DATE
        ,NVL(tb1.CHR_ID, '') CHR_ID
        ,NVL(tb1.CLE_ID, '') CLE_ID
        ,COALESCE(hb1.USER_NAME,'UNKNOWN') CREATED_BY
        ,tb1.CREATION_DATE
        ,tb1.INTERFACE_OFFSET_DAYS
        ,tb1.INVOICE_OFFSET_DAYS
        ,tb1.LAST_UPDATE_DATE
        ,'UNKNOWN' AS LAST_UPDATE_LOGIN
        ,COALESCE(hb2.USER_NAME,'UNKNOWN') LAST_UPDATED_BY
        ,tb1.dtl__capxaction
        ,row_number() OVER (
            PARTITION BY tb1.ID ORDER BY tb1.DTL__CAPXTIMESTAMP DESC
                ,tb1.edl_record_sequence DESC
            ) AS rnk
    FROM trans_cfocontroller_erp_oks_erpprod.oks_stream_levels_b tb1,
""" + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_ml_time QQ1
        
    LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tb1.CREATED_BY
    LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tb1.LAST_UPDATED_BY
     WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
    ) tb1
WHERE tb1.rnk = 1"""
        print("Running query: " + query)
        sparkSession.sql(query)


def main():
    m = d8tcontrbillstreammlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontrbillstreammlinssql").enableHiveSupport().getOrCreate()
    main()