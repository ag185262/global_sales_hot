from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tcontractlinebillmlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query="""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_oks_erpprod.oks_bill_cont_lines) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_ml_time  select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_ml_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_ml"""
        print("Job: 'd8t_contract_line_bill_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT  
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_ml           
        SELECT 
                    contract_line_bill_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    bill_action_code,
                    bill_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    bill_transaction_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    next_invoice_date_time,
                    sent_flag,
                    tran_code          
        from(
        select
                    COALESCE(ID,'') contract_line_bill_id,
                    8001 as instance_id,
                    from_unixtime(unix_timestamp(tbl.DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss') as_of_date_time,
                    """ + db_params.btch_nbr + """ batch_nbr,
                    BILL_ACTION bill_action_code,
                    AMOUNT bill_amt_ent,
                    DATE_BILLED_FROM bill_from_date_time,
                    DATE_BILLED_TO bill_to_date_time,
                    COALESCE(BTN_ID, '') bill_transaction_id,
                    COALESCE(CLE_ID, '')contract_line_id,
                    coalesce(hb1.USER_NAME, 'UNKNOWN') created_by_name,
                    tbl.CREATION_DATE creation_date_time,
                    COALESCE(CURRENCY_CODE, '')curr_code,
                    tbl.LAST_UPDATE_DATE last_update_date_time,
                    'UNKNOWN' as last_update_login_name,
                    COALESCE(hb2.USER_NAME, 'UNKNOWN') AS last_updated_by_name,
                    DATE_NEXT_INVOICE as next_invoice_date_time,
                    COALESCE(SENT_YN, '') sent_flag,
                    tbl.dtl__capxaction as  tran_code,
                    row_number() over (partition by tbl.ID order by tbl.DTL__CAPXTIMESTAMP desc,tbl.edl_record_sequence desc) as rnk
                    
        FROM trans_cfocontroller_erp_oks_erpprod.oks_bill_cont_lines tbl,
        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_ml_time QQ1
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tbl.CREATED_BY
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tbl.LAST_UPDATED_BY
        WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
        )src where src.rnk = 1"""
        print("Job: 'd8t_contract_line_bill_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tcontractlinebillmlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontractlinebillmlinssql").enableHiveSupport().getOrCreate()
    main()