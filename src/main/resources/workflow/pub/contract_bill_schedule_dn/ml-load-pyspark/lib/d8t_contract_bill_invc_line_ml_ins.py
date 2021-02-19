from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tcontractbillinvclinemlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query="""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_oks_erpprod.OKS_BILL_TXN_LINES) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_ml_time  select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_ml_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_ml"""
        print("Job: 'd8t_contract_bill_invc_line_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT  INTO TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_ml
        SELECT 
            bill_transaction_line_id,
            instance_id,
           as_of_date_time,
           batch_nbr,
           bill_instance_nbr,
           bill_transaction_id,
           contract_line_bill_id,
           contract_subline_bill_id,
           created_by_name,
           creation_date_time,
           invoice_line_amt_ent,
           last_update_date_time,
           last_update_login_name,
           last_updated_by_name,
           tran_code,
           cycle_ref_seq_desc,
          invoice_date,
          invoice_nbr,
          invoice_type_code  
        from(
        select 
                   ID as bill_transaction_line_id,
                    8001 as instance_id,
                   from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss') as_of_date_time,
                   """ + db_params.btch_nbr + """ batch_nbr,
                   COALESCE(CAST(BILL_INSTANCE_NUMBER AS DECIMAL(38,0)),'') bill_instance_nbr,
                   COALESCE(BTN_ID,'') bill_transaction_id,
                   COALESCE(BCL_ID,'') contract_line_bill_id,
                   COALESCE(BSL_ID,'') contract_subline_bill_id,
                   COALESCE(hb1.USER_NAME,'UNKNOWN') created_by_name,
                   CREATION_DATE creation_date_time,
                   COALESCE(CAST(CAST (TRX_LINE_AMOUNT AS STRING)AS DECIMAL(20,4)),0) invoice_line_amt_ent,
                   LAST_UPDATE_DATE last_update_date_time,
                   'UNKNOWN' AS last_update_login_name,
                   COALESCE(hb2.USER_NAME,'UNKNOWN') last_updated_by_name,
                  DTL__CAPXACTION   tran_code,
                  COALESCE(CYCLE_REFRENCE,'')  cycle_ref_seq_desc,
                  regexp_replace(CAST(CAST(TRX_DATE AS TIMESTAMP) AS DATE ),'-','')  invoice_date,
                  COALESCE(TRX_NUMBER,'')  invoice_nbr,
                  COALESCE(TRX_CLASS,'')  invoice_type_code,
                  row_number() over (partition by tbl.ID order by tbl.DTL__CAPXTIMESTAMP desc,tbl.edl_record_sequence desc) as rnk
          FROM
          trans_cfocontroller_erp_oks_erpprod.OKS_BILL_TXN_LINES tbl,
          """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_ml_time QQ1
          LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tbl.CREATED_BY
          LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tbl.LAST_UPDATED_BY
        WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
          )src where src.rnk = 1"""
        print("Job: 'd8t_contract_bill_invc_line_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tcontractbillinvclinemlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontractbillinvclinemlinssql").enableHiveSupport().getOrCreate()
    main()