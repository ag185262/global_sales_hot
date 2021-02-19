from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tcontractgroupxrefmlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query="""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_okc_erpprod.OKC_K_GRPINGS) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_ml_time  select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_ml_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_ml"""
        print("Job: 'd8t_contract_group_xref_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT  into """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_ml 
        select 
            contract_group_xref_id,
            instance_id,
            as_of_date_time,
            batch_nbr,
            created_by_name,
            creation_date_time,
            included_contract_group_id,
            included_contract_header_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            parent_contract_group_id,
            subclass_code,
            tran_code
        from(
        select 
          
                    COALESCE(ID,'') contract_group_xref_id,
                    8001 as instance_id,
                    from_unixtime(unix_timestamp(DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss') as_of_date_time,
                    """ + db_params.btch_nbr + """ as batch_nbr,
                    COALESCE(hb1.USER_NAME,'UNKNOWN') created_by_name,
                    CREATION_DATE as creation_date_time,
                    COALESCE(INCLUDED_CGP_ID,0) included_contract_group_id,
                    COALESCE(INCLUDED_CHR_ID,'') included_contract_header_id,
                    LAST_UPDATE_DATE last_update_date_time,
                    COALESCE(hb2.USER_NAME,'UNKNOWN') AS  last_update_login_name,
                    'UNKNOWN' AS last_updated_by_name,
                    COALESCE(CGP_PARENT_ID,0) parent_contract_group_id,
                    COALESCE(SCS_CODE,'') subclass_code,
                    dtl__capxaction as tran_code,
                    row_number() over (partition by tbl.ID order by tbl.DTL__CAPXTIMESTAMP desc,tbl.edl_record_sequence desc) as rnk
        from 
          trans_cfocontroller_erp_okc_erpprod.OKC_K_GRPINGS tbl,
         """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_ml_time QQ1
          LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tbl.CREATED_BY
          LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tbl.LAST_UPDATED_BY
        WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
          )src where src.rnk = 1"""
        print("Job: 'd8t_contract_group_xref_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tcontractgroupxrefmlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontractgroupxrefmlinssql").enableHiveSupport().getOrCreate()
    main()