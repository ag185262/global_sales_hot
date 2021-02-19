from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tcontractlvlelementmlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query="""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_oks_erpprod.OKS_LEVEL_ELEMENTS) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_ml_time  select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_ml_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)
        query = """TRUNCATE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_ml"""
        print("Job: 'd8t_contract_lvl_element_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT  INTO TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_ml
        SELECT 
        instance_id,
        level_element_id,
        amount_due_date_time,
        as_of_date_time,
        batch_nbr,
        completed_date_time,
        created_by_name,
        creation_date_time,
        gl_receivable_date_time,
        interface_date_time,
        last_update_date_time,
        last_updated_by_name,
        level_element_amt_ent,
        period_start_date,
        print_date_time,
        rule_id,
        rule_start_date_time,
        sequence_nbr,
        tran_code,
        transaction_date_time,
        alt_contract_header_id,
        contract_line_id,
        parent_contract_line_id,
        period_end_date
        FROM(
        SELECT  
        8001 AS instance_id
        ,ID AS level_element_id
        ,tbl.DATE_DUE as amount_due_date_time
        ,from_unixtime(unix_timestamp(tbl.DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss')       AS as_of_date_time
        ,""" + db_params.btch_nbr + """ AS batch_nbr
        ,DATE_COMPLETED AS completed_date_time
        ,coalesce(hb1.USER_NAME, 'UNKNOWN')   AS created_by_name
        ,tbl.CREATION_DATE AS creation_date_time
        ,DATE_RECEIVABLE_GL AS gl_receivable_date_time
        ,tbl.DATE_TO_INTERFACE as interface_date_time
        ,tbl.LAST_UPDATE_DATE AS last_update_date_time
        ,coalesce(hb2.USER_NAME, 'UNKNOWN')  AS last_updated_by_name
        ,COALESCE(CAST(CAST (tbl.AMOUNT AS STRING)AS DECIMAL(20,4)),0) AS level_element_amt_ent
        ,DATE_START AS period_start_date
        ,DATE_PRINT AS print_date_time
        ,COALESCE(RUL_ID, '') AS rule_id
        ,DATE_REVENUE_RULE_START AS rule_start_date_time
        ,COALESCE(SEQUENCE_NUMBER,0) AS sequence_nbr
        ,dtl__capxaction AS tran_code
        ,DATE_TRANSACTION AS transaction_date_time
        ,COALESCE(DNZ_CHR_ID, '') AS alt_contract_header_id
        ,COALESCE(CLE_ID, '') AS contract_line_id
        ,COALESCE(PARENT_CLE_ID, '') AS parent_contract_line_id
        ,DATE_END AS period_end_date
        , row_number() over (partition by tbl.ID order by tbl.DTL__CAPXTIMESTAMP desc,tbl.edl_record_sequence desc) as rnk
        
        FROM trans_cfocontroller_erp_oks_erpprod.OKS_LEVEL_ELEMENTS tbl,
        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_ml_time QQ1
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tbl.CREATED_BY
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tbl.LAST_UPDATED_BY
        WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
        )src where src.rnk =1"""
        print("Job: 'd8t_contract_lvl_element_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tcontractlvlelementmlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontractlvlelementmlinssql").enableHiveSupport().getOrCreate()
    main()