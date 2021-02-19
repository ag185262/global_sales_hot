from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tcontractlinexrefmlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query="""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_okc_erpprod.OKC_K_ITEMS) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_ml_time  select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_ml_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_ml"""
        print("Job: 'd8t_contract_line_xref_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT  
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_ml 
        select
            contract_line_xref_id,
            instance_id,
            alt_contract_header_id,
            as_of_date_time,
            batch_nbr,
            contract_header_id,
            contract_line_id,
            created_by_name,
            creation_date_time,
            exception_flag,
            for_contract_line_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            nbr_of_items,
            object1_code,
            object1_id1,
            object1_id2,
            priced_item_flag,
            tran_code,
            uom_code,
            upg_orig_sys_ref_id,
            upg_orig_sys_ref_name      
        from(
        SELECT  
        NVL(ID, '') AS contract_line_xref_id
        ,8001 AS instance_id
        ,NVL(DNZ_CHR_ID, '') AS alt_contract_header_id
        ,from_unixtime(unix_timestamp(tbl.DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss') AS as_of_date_time
        ,""" + db_params.btch_nbr + """ AS batch_nbr
        ,NVL(CHR_ID, '') AS contract_header_id
        ,NVL(CLE_ID, '') AS contract_line_id
        ,coalesce(hb1.USER_NAME, 'UNKNOWN')   AS created_by_name
        ,tbl.CREATION_DATE  AS creation_date_time
        ,NVL(EXCEPTION_YN, '') AS exception_flag
        ,NVL(CLE_ID_FOR, '') AS for_contract_line_id
        ,tbl.LAST_UPDATE_DATE AS last_update_date_time
        ,'UNKNOWN'  AS last_update_login_name
        ,coalesce(hb2.USER_NAME, 'UNKNOWN')  AS last_updated_by_name
        ,NVL(NUMBER_OF_ITEMS,0) AS nbr_of_items
        ,NVL(JTOT_OBJECT1_CODE, '') AS object1_code
        ,NVL(OBJECT1_ID1, '') AS object1_id1
        ,NVL(OBJECT1_ID2, '') AS object1_id2
        ,NVL(PRICED_ITEM_YN, '') AS priced_item_flag
        ,tbl.dtl__capxaction AS tran_code
        ,NVL(UOM_CODE, '') AS uom_code
        ,NVL(UPG_ORIG_SYSTEM_REF_ID, '') AS upg_orig_sys_ref_id
        ,NVL(UPG_ORIG_SYSTEM_REF, '') AS upg_orig_sys_ref_name
        , row_number() over (partition by tbl.ID order by tbl.DTL__CAPXTIMESTAMP desc,tbl.edl_record_sequence desc) as rnk
        FROM trans_cfocontroller_erp_okc_erpprod.OKC_K_ITEMS tbl,
        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_ml_time QQ1
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tbl.CREATED_BY
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tbl.LAST_UPDATED_BY
        WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
        )src where src.rnk =1"""
        print("Job: 'd8t_contract_line_xref_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tcontractlinexrefmlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontractlinexrefmlinssql").enableHiveSupport().getOrCreate()
    main()