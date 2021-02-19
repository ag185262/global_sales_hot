from pyspark import SparkConf, SparkContext, SQLContext 
from pyspark.sql import SparkSession, HiveContext 
import sys 
import logging 
import db_params

class d8tcontrheadersvcsuppmlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = """with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_oks_erpprod.oks_k_headers_b) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_HEADER_SVC_SUPP_ML_time select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_HEADER_SVC_SUPP_ML_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)

        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_ml"""
        print("Running query: " + query)
        sparkSession.sql(query)

        query = """INSERT  INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_CONTR_HEADER_SVC_SUPP_ML
SELECT ID
    ,'8001'
    ,ACCT_RULE_ID
    ,AR_INTERFACE_YN
    ,from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss')
    ,""" + db_params.btch_nbr + """
    ,BILLING_SCHEDULE_TYPE
    ,CHR_ID
    ,CREATED_BY
    ,CREATION_DATE
    ,INV_TRX_TYPE
    ,EST_REV_DATE
    ,EST_REV_PERCENT
    ,REV_EST_PERIOD_USED
    ,REV_EST_DURATION_USED
    ,REV_EST_PERCENT_USED
    ,GRACE_DURATION
    ,GRACE_PERIOD
    ,HOLD_BILLING
    ,INV_PRINT_PROFILE
    ,LAST_UPDATE_DATE
    ,LAST_UPDATE_LOGIN
    ,LAST_UPDATED_BY
    ,PAYMENT_TYPE
    ,RENEWAL_PO_NUMBER
    ,RENEWAL_PO_REQUIRED
    ,RENEWAL_PO_USED
    ,RENEWAL_EST_REV_DURATION
    ,RENEWAL_EST_REV_PERCENT
    ,RENEWAL_EST_REV_PERIOD
    ,RENEWAL_GRACE_PERIOD_USED
    ,RENEWAL_GRACE_DURATION_USED
    ,RENEWAL_MARKUP_PERCENT
    ,RENEWAL_MARKUP_PERCENT_USED
    ,RENEWAL_NOTIFICATION_TO
    ,'IMP'
    ,RENEWAL_PRICE_LIST
    ,RENEWAL_PRICE_LIST_USED
    ,RENEWAL_PRICING_TYPE
    ,RENEWAL_PRICING_TYPE_USED
    ,RENEWAL_TYPE_USED
    ,SERVICE_PO_NUMBER
    ,SERVICE_PO_REQUIRED
    ,SUMMARY_TRX_YN
    ,TAX_STATUS
    ,TAX_EXEMPTION_ID
    ,dtl__capxaction
    ,APPROVAL_TYPE_USED
    ,EVN_THRESHOLD_AMT
    ,ERN_THRESHOLD_AMT
FROM (
    SELECT tb1.ID
        ,NVL(tb1.ACCT_RULE_ID, 0) ACCT_RULE_ID
        ,NVL(tb1.AR_INTERFACE_YN, '') AR_INTERFACE_YN
        ,tb1.DTL__CAPXTIMESTAMP
        ,NVL(tb1.BILLING_SCHEDULE_TYPE, '') BILLING_SCHEDULE_TYPE
        ,NVL(tb1.CHR_ID, '') CHR_ID
        ,COALESCE(hb1.USER_NAME,'UNKNOWN') CREATED_BY
        ,tb1.CREATION_DATE
        ,NVL(tb1.INV_TRX_TYPE, '') INV_TRX_TYPE
        ,tb1.EST_REV_DATE
        ,CAST(tb1.EST_REV_PERCENT AS DECIMAL(20,4)) EST_REV_PERCENT
        ,NVL(tb1.REV_EST_PERIOD_USED, '') REV_EST_PERIOD_USED
        ,CAST(tb1.REV_EST_DURATION_USED AS DECIMAL(20,4)) REV_EST_DURATION_USED
        ,CAST(tb1.REV_EST_PERCENT_USED AS DECIMAL(20,4)) REV_EST_PERCENT_USED
        ,tb1.GRACE_DURATION
        ,tb1.GRACE_PERIOD
        ,NVL(tb1.HOLD_BILLING, '') HOLD_BILLING
        ,NVL(tb1.INV_PRINT_PROFILE, '') INV_PRINT_PROFILE
        ,tb1.LAST_UPDATE_DATE
        ,'UNKNOWN' as LAST_UPDATE_LOGIN
        ,COALESCE(hb2.USER_NAME,'UNKNOWN') LAST_UPDATED_BY
        ,tb1.PAYMENT_TYPE
        ,tb1.RENEWAL_PO_NUMBER
        ,tb1.RENEWAL_PO_REQUIRED
        ,NVL(tb1.RENEWAL_PO_USED, '') RENEWAL_PO_USED
        ,tb1.RENEWAL_EST_REV_DURATION
        ,CAST(tb1.RENEWAL_EST_REV_PERCENT AS DECIMAL(20,4)) RENEWAL_EST_REV_PERCENT
        ,NVL(tb1.RENEWAL_EST_REV_PERIOD, '') RENEWAL_EST_REV_PERIOD
        ,tb1.RENEWAL_GRACE_PERIOD_USED
        ,tb1.RENEWAL_GRACE_DURATION_USED
        ,CAST(tb1.RENEWAL_MARKUP_PERCENT AS DECIMAL(20,4)) RENEWAL_MARKUP_PERCENT
        ,tb1.RENEWAL_MARKUP_PERCENT_USED
        ,NVL(tb1.RENEWAL_NOTIFICATION_TO, 0) RENEWAL_NOTIFICATION_TO
        ,tb1.RENEWAL_PRICE_LIST
        ,NVL(tb1.RENEWAL_PRICE_LIST_USED, 0) RENEWAL_PRICE_LIST_USED 
        ,NVL(tb1.RENEWAL_PRICING_TYPE, '') RENEWAL_PRICING_TYPE
        ,NVL(tb1.RENEWAL_PRICING_TYPE_USED, '') RENEWAL_PRICING_TYPE_USED
        ,NVL(tb1.RENEWAL_TYPE_USED, '') RENEWAL_TYPE_USED
        ,NVL(tb1.SERVICE_PO_NUMBER, '') SERVICE_PO_NUMBER
        ,NVL(tb1.SERVICE_PO_REQUIRED, '') SERVICE_PO_REQUIRED
        ,NVL(tb1.SUMMARY_TRX_YN, '') SUMMARY_TRX_YN
        ,tb1.TAX_STATUS
        ,tb1.TAX_EXEMPTION_ID
        ,tb1.dtl__capxaction
        ,tb1.APPROVAL_TYPE_USED
        ,tb1.EVN_THRESHOLD_AMT
        ,tb1.ERN_THRESHOLD_AMT
        ,row_number() OVER (
            PARTITION BY tb1.ID ORDER BY tb1.DTL__CAPXTIMESTAMP DESC
                ,tb1.edl_record_sequence DESC
            ) AS rnk
    FROM trans_cfocontroller_erp_oks_erpprod.oks_k_headers_b tb1,
      """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_ml_time QQ1  
    LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tb1.CREATED_BY
    LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tb1.LAST_UPDATED_BY
     WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
    ) tb1
WHERE tb1.rnk = 1"""
        print("Running query: " + query)
        sparkSession.sql(query)

def main():
    m = d8tcontrheadersvcsuppmlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontrheadersvcsuppmlinssql").enableHiveSupport().getOrCreate()
    main()
