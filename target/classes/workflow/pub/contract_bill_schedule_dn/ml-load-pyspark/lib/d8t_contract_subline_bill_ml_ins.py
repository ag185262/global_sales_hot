from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tcontractsublinebillmlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query="""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_oks_erpprod.OKS_BILL_SUB_LINES) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_ml_time  select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_ml_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_ml"""
        print("Job: 'd8t_contract_subline_bill_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT  INTO """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_ml
         select
            contract_subline_bill_id,
            instance_id,
            as_of_date_time,
            batch_nbr,
            bill_amt_ent,
            bill_avg_amt_ent,
            bill_from_date_time,
            bill_to_date_time,
            contract_line_bill_id,
            contract_line_id,
            created_by_name,
            creation_date_time,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code
         from(
         SELECT 
                COALESCE(ID,'') as contract_subline_bill_id  ,
                8001 as instance_id               ,
                from_unixtime(unix_timestamp(tbl.DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss') as_of_date_time           ,
                 """ + db_params.btch_nbr + """ as  batch_nbr                 ,
                AMOUNT as bill_amt_ent              ,
                COALESCE(AVERAGE,0) as  bill_avg_amt_ent          ,
                DATE_BILLED_FROM as bill_from_date_time       ,
                DATE_BILLED_TO as bill_to_date_time         ,
                COALESCE(BCL_ID, '') contract_line_bill_id     ,
                COALESCE(CLE_ID, '') contract_line_id          ,
                coalesce(hb1.USER_NAME, 'UNKNOWN') created_by_name           ,
                 tbl.CREATION_DATE creation_date_time        ,
                 tbl.LAST_UPDATE_DATE last_update_date_time     ,
                'UNKNOWN' AS  last_update_login_name    ,
                coalesce(hb2.USER_NAME, 'UNKNOWN') AS last_updated_by_name      ,
                dtl__capxaction tran_code,
                row_number() over (partition by tbl.ID order by tbl.DTL__CAPXTIMESTAMP desc,tbl.edl_record_sequence desc) as rnk
                
        FROM trans_cfocontroller_erp_oks_erpprod.OKS_BILL_SUB_LINES tbl,
         """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_ml_time QQ1
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tbl.CREATED_BY
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tbl.LAST_UPDATED_BY
        WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
        )src where src.rnk = 1"""
        print("Job: 'd8t_contract_subline_bill_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tcontractsublinebillmlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontractsublinebillmlinssql").enableHiveSupport().getOrCreate()
    main()