from pyspark import SparkConf, SparkContext, SQLContext 
from pyspark.sql import SparkSession, HiveContext 
import db_params

class d8tcontractlinesvcsuppmlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = """with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_oks_erpprod.oks_k_lines_b) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTRACT_LINE_SVC_SUPP_ML_time select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTRACT_LINE_SVC_SUPP_ML_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)

        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_ml"""
        print("Running query: " + query)
        sparkSession.sql(query)

        query = """INSERT  INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTRACT_LINE_SVC_SUPP_ML
SELECT ID
    ,'8001'
    ,ACCT_RULE_ID
    ,NVL(ALLOW_BT_DISCOUNT, '') ALLOW_BT_DISCOUNT
    ,DNZ_CHR_ID
    ,from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss')
    ,""" + db_params.btch_nbr + """
    ,NVL(BILLING_SCHEDULE_TYPE,'') BILLING_SCHEDULE_TYPE
    ,CLE_ID
    ,CAST(CLVL_EXTENDED_AMT AS DECIMAL(20,4)) CLVL_EXTENDED_AMT 
    ,CAST(CLVL_LIST_PRICE AS DECIMAL(20,4)) CLVL_LIST_PRICE
    ,NVL(CLVL_QUANTITY, 0) CLVL_QUANTITY
    ,nvl(CLVL_UOM_CODE, '') CLVL_UOM_CODE
    ,NVL(COVERAGE_TYPE, '') COVERAGE_TYPE
    , CREATED_BY
    ,CREATION_DATE
    ,CREDIT_AMOUNT
    ,CUST_PO_NUMBER
    ,CUST_PO_NUMBER_REQ_YN
    ,NVL(FULL_CREDIT,'') FULL_CREDIT
    ,NVL(INCIDENT_SEVERITY_ID, '') INCIDENT_SEVERITY_ID
    ,INV_PRINT_FLAG
    ,LAST_UPDATE_DATE
    ,'UNKNOWN' AS LAST_UPDATE_LOGIN
    , LAST_UPDATED_BY
    ,OFFSET_DURATION
    ,NVL(OFFSET_PERIOD, '') OFFSET_PERIOD
    ,OVERRIDE_AMOUNT
    ,PRICE_UOM
    ,NVL(PDF_ID, '') PDF_ID
    ,NVL(PROD_PRICE, 0) PROD_PRICE
    ,NVL(PROD_UPGRADE_YN, '') PROD_UPGRADE_YN
    ,NVL(REACT_ACTIVE_YN,'') REACT_ACTIVE_YN
    ,NVL(SERVICE_PRICE, 0) SERVICE_PRICE
    ,SUPPRESSED_CREDIT
    ,NVL(SYNC_DATE_INSTALL, '') SYNC_DATE_INSTALL
    ,TAX_AMOUNT
    ,TAX_STATUS
    ,NVL(TAX_EXEMPTION_ID, 0) TAX_EXEMPTION_ID 
    ,TAX_CODE
    ,TAX_INCLUSIVE_YN
    ,CAST(TOPLVL_ADJ_PRICE AS DECIMAL(20,4)) TOPLVL_ADJ_PRICE
    ,NVL(TOPLVL_OPERAND_CODE, '')
    ,CAST(TOPLVL_OPERAND_VAL AS DECIMAL(20,4)) TOPLVL_OPERAND_VAL
    ,NVL(TOPLVL_UOM_CODE, '') TOPLVL_UOM_CODE
    ,NVL(TOPLVL_PRICE_QTY, 0) TOPLVL_PRICE_QTY
    ,NVL(TOPLVL_QUANTITY, 0)    TOPLVL_QUANTITY
    ,dtl__capxaction
    ,NVL(TRANSFER_OPTION, '') TRANSFER_OPTION
    ,UBT_AMOUNT
    ,NVL(WORK_THRU_YN, '') WORK_THRU_YN
FROM (
    SELECT ID
        ,NVL(ACCT_RULE_ID, '') ACCT_RULE_ID
        ,ALLOW_BT_DISCOUNT
        ,NVL(tb1.DNZ_CHR_ID, '') DNZ_CHR_ID
        ,DTL__CAPXTIMESTAMP
        ,BILLING_SCHEDULE_TYPE
        ,CLE_ID
        ,CLVL_EXTENDED_AMT
        ,CLVL_LIST_PRICE
        ,CLVL_QUANTITY
        ,CLVL_UOM_CODE
        ,COVERAGE_TYPE
        ,COALESCE(hb1.USER_NAME,'UNKNOWN') CREATED_BY
        ,CREATION_DATE
        ,CAST(CREDIT_AMOUNT AS DECIMAL(20,4)) CREDIT_AMOUNT
        ,NVL(CUST_PO_NUMBER, '') CUST_PO_NUMBER
        ,nvl(CUST_PO_NUMBER_REQ_YN, '') CUST_PO_NUMBER_REQ_YN
        ,FULL_CREDIT
        ,INCIDENT_SEVERITY_ID
        ,NVL(INV_PRINT_FLAG, '') INV_PRINT_FLAG
        ,LAST_UPDATE_DATE
        ,LAST_UPDATE_LOGIN
        ,COALESCE(hb2.USER_NAME,'UNKNOWN') LAST_UPDATED_BY
        ,OFFSET_DURATION
        ,OFFSET_PERIOD
        ,CAST(OVERRIDE_AMOUNT as DECIMAL(20,4)) OVERRIDE_AMOUNT
        ,NVL(PRICE_UOM, '') PRICE_UOM
        ,PDF_ID
        ,PROD_PRICE
        ,PROD_UPGRADE_YN
        ,REACT_ACTIVE_YN
        ,SERVICE_PRICE
        ,NVL(SUPPRESSED_CREDIT, 0) SUPPRESSED_CREDIT
        ,SYNC_DATE_INSTALL
        ,CAST(TAX_AMOUNT AS DECIMAL(20,4)) TAX_AMOUNT
        ,NVL(TAX_STATUS, '') TAX_STATUS
        ,TAX_EXEMPTION_ID
        ,NVL(TAX_CODE, 0) TAX_CODE
        ,NVL(TAX_INCLUSIVE_YN, '') TAX_INCLUSIVE_YN
        ,TOPLVL_ADJ_PRICE
        ,TOPLVL_OPERAND_CODE
        ,TOPLVL_OPERAND_VAL
        ,TOPLVL_UOM_CODE
        ,TOPLVL_PRICE_QTY
        ,TOPLVL_QUANTITY
        ,dtl__capxaction
        ,TRANSFER_OPTION
        ,CAST(UBT_AMOUNT AS DECIMAL(20,4)) UBT_AMOUNT
        ,WORK_THRU_YN
        ,row_number() OVER (
            PARTITION BY ID ORDER BY DTL__CAPXTIMESTAMP DESC
                ,edl_record_sequence DESC
            ) AS rnk
    FROM trans_cfocontroller_erp_oks_erpprod.oks_k_lines_b tb1,
    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_ml_time QQ1   
    LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tb1.CREATED_BY
    LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tb1.LAST_UPDATED_BY
     WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
    ) tb1
WHERE tb1.rnk = 1"""
        print("Running query: " + query)
        sparkSession.sql(query)

def main():
    m = d8tcontractlinesvcsuppmlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontractlinesvcsuppmlinssql").enableHiveSupport().getOrCreate()
    main()
