from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tcontractlinestylelkmlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query="""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_okc_erpprod.OKC_LINE_STYLES_TL) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_ml_time  select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_ml_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_ml"""
        print("Job: 'd8t_contract_line_style_lk_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT  into """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_ml 
        select
        contract_line_style_id,
        instance_id,
        language_code,
        as_of_date_time,
        batch_nbr,
        created_by_name,
        creation_date_time,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        line_style_desc,
        line_style_name,
        source_language_code,
        tran_code
        from(
        select 
           
          ID AS contract_line_style_id
        ,8001 AS instance_id
        ,COALESCE(LANGUAGE,'') AS language_code
        , from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss') AS as_of_date_time
        , """ + db_params.btch_nbr + """ AS batch_nbr
        ,COALESCE(hb1.USER_NAME,'UNKNOWN')  AS created_by_name
        ,CREATION_DATE AS creation_date_time
        ,LAST_UPDATE_DATE AS last_update_date_time
        , 'UNKNOWN' AS last_update_login_name
        ,COALESCE(hb2.USER_NAME,'UNKNOWN')  AS last_updated_by_name
        ,COALESCE(DESCRIPTION,'') AS line_style_desc
        ,COALESCE(NAME,'') AS line_style_name
        ,COALESCE(SOURCE_LANG,'') AS source_language_code
        ,  dtl__capxaction AS tran_code
        , row_number() over (partition by tbl.ID order by tbl.DTL__CAPXTIMESTAMP desc,tbl.edl_record_sequence desc) as rnk
        from 
        trans_cfocontroller_erp_okc_erpprod.OKC_LINE_STYLES_TL tbl,
        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_ml_time QQ1
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tbl.CREATED_BY
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tbl.LAST_UPDATED_BY
        WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
        )src where src.rnk = 1"""
        print("Job: 'd8t_contract_line_style_lk_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tcontractlinestylelkmlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontractlinestylelkmlinssql").enableHiveSupport().getOrCreate()
    main()