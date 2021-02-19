from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8tcontractpartyrolemlinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query="""with QQ1 as (select max(edl_ingest_time) as max_time from trans_cfocontroller_erp_okc_erpprod.OKC_K_PARTY_ROLES_B) insert overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_ml_time  select end_tm,max_time from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_ml_time inner join QQ1 on 1=1"""
        print("Running query: " + query)
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_ml"""
        print("Job: 'd8t_contract_party_role_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT  
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_ml
        SELECT
            instance_id,
            party_role_id,
            alt_contract_header_id,
            as_of_date_time,
            batch_nbr,
            created_by_name,
            contract_line_id,
            contract_header_id,
            creation_date_time,
            facility_code,
            government_code,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            minority_group_code,
            object1_code,
            object1_id1,
            object1_id2,
            role_code,
            parent_party_role_id,
            primary_party_flag,
            tran_code,
            small_business_flag,
            women_owned_flag
        from(
        select  
            8001 as instance_id, 
            ID as party_role_id,
            NVL(DNZ_CHR_ID, 0) as alt_contract_header_id,
            from_unixtime(unix_timestamp(tbl.DTL__CAPXTIMESTAMP, 'yyyyMMddHHmmssSSSSSS'), 'yyyy-MM-dd HH:mm:ss') as as_of_date_time,
            """ + db_params.btch_nbr + """ as batch_nbr,
            coalesce(hb1.USER_NAME, 'UNKNOWN')  AS created_by_name,
            nvl(CLE_ID,0) as contract_line_id,
            nvl(CHR_ID,0) as contract_header_id,
            tbl.CREATION_DATE as creation_date_time,
            NVL(FACILITY,0) as facility_code,    
            NVL(CODE,0) as government_code,
            tbl.LAST_UPDATE_DATE as last_update_date_time,
            'UNKNOWN' AS last_update_login_name,
            coalesce(hb2.USER_NAME, 'UNKNOWN') AS last_updated_by_name,
            nvl(MINORITY_GROUP_LOOKUP_CODE,0) as minority_group_code,
            NVL(JTOT_OBJECT1_CODE,0) as object1_code,
            OBJECT1_ID1 as object1_id1,
            NVL(OBJECT1_ID2,0) as object1_id2,
            NVL(RLE_CODE,0) as role_code,
            NVL(CPL_ID, '')as parent_party_role_id ,
            NVL(PRIMARY_YN, '') as primary_party_flag,
            tbl.dtl__capxaction as tran_code,
            nvl(SMALL_BUSINESS_FLAG,0) as small_business_flag,
            nvl(WOMEN_OWNED_FLAG,0) as women_owned_flag,
            row_number() over (partition by tbl.ID order by tbl.DTL__CAPXTIMESTAMP desc,tbl.edl_record_sequence desc) as rnk
        FROM
        trans_cfocontroller_erp_okc_erpprod.OKC_K_PARTY_ROLES_B tbl,
        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_ml_time QQ1
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb1 ON hb1.USER_ID = tbl.CREATED_BY
        LEFT OUTER JOIN (SELECT USER_ID,USER_NAME FROM hub_common.HUB_REF_FND_USER) hb2 ON hb2.USER_ID = tbl.LAST_UPDATED_BY
        WHERE edl_ingest_time > QQ1.start_tm
        AND edl_ingest_time <= QQ1.end_tm
        )src where src.rnk =1"""
        print("Job: 'd8t_contract_party_role_ml'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8tcontractpartyrolemlinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8tcontractpartyrolemlinssql").enableHiveSupport().getOrCreate()
    main()