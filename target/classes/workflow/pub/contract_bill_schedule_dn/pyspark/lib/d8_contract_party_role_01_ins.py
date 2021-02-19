from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractpartyrole01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_party_role_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t1"""
        print("Job: 'd8_contract_party_role_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        with qq1 as (
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
                            women_owned_flag,
                            ROW_NUMBER() over (PARTITION BY party_role_id, instance_id 
                                ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK 
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_ml 
                        WHERE
                            upper(trim(tran_code)) <> upper(trim('D'))  ) 
        INSERT INTO  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t1 
        SELECT
                   EDW2.instance_id,
                   EDW2.party_role_id,
                   EDW2.alt_contract_header_id,
                   EDW2.as_of_date_time,
                   EDW2.batch_nbr,
                   EDW2.created_by_name,
                   EDW2.contract_line_id,
                   EDW2.contract_header_id,
                   EDW2.creation_date_time,
                   EDW2.facility_code,
                   EDW2.government_code,
                   EDW2.last_update_date_time,
                   EDW2.last_update_login_name,
                   EDW2.last_updated_by_name,
                   EDW2.minority_group_code,
                   EDW2.object1_code,
                   EDW2.object1_id1,
                   EDW2.object1_id2,
                   EDW2.role_code,
                   EDW2.parent_party_role_id,
                   EDW2.primary_party_flag,
                   EDW2.tran_code,
                   EDW2.small_business_flag,
                   EDW2.women_owned_flag
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_ml  
         EDW2 
          INNER JOIN (
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
                    women_owned_flag, 
                   RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.party_role_id)) =upper(trim(IBVL.party_role_id)) 
           AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_party_role_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_party_role_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2"""
        print("Job: 'd8_contract_party_role_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2           SELECT
                    instance_id,
                    party_role_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    contract_line_id,
                    contract_header_id,
                    creation_date_time,
                    'Y',
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
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT party_role_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t1 
                    ) AS autoAlias_217 
                        ON (
                            upper(trim(party_role_id)) = upper(trim(autoAlias_217.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_217.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_party_role_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t3"""
        print("Job: 'd8_contract_party_role_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t3           SELECT
                    instance_id,
                    party_role_id,
                    alt_contract_header_id,
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
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT party_role_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t1 
                    ) AS autoAlias_219 
                        ON (
                            upper(trim(party_role_id)) = upper(trim(autoAlias_219.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_219.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_party_role_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_party_role_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t3           SELECT
                    instance_id,
                    party_role_id,
                    alt_contract_header_id,
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
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT party_role_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2 
                    ) AS autoAlias_221 
                        ON (
                            upper(trim(party_role_id)) = upper(trim(autoAlias_221.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_221.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_party_role_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_party_role_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2 SELECT
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.party_role_id AS party_role_id ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.alt_contract_header_id AS alt_contract_header_id ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.contract_line_id AS contract_line_id ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.contract_header_id AS contract_header_id ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_223.auto_c00 IS not null and autoAlias_223.auto_c01 IS not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_contract_party_role_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.facility_code AS facility_code ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.government_code AS government_code ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.minority_group_code AS minority_group_code ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.object1_code AS object1_code ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.object1_id1 AS object1_id1 ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.object1_id2 AS object1_id2 ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.role_code AS role_code ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.parent_party_role_id AS parent_party_role_id ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.primary_party_flag AS primary_party_flag ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.tran_code AS tran_code ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.small_business_flag AS small_business_flag ,
                    AAPPLLIIDD1EENNVV_contract_party_role_t2.women_owned_flag AS women_owned_flag 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2 AS AAPPLLIIDD1EENNVV_contract_party_role_t2 
                Left Outer Join
                    (
                        SELECT
                            party_role_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t3      
                        GROUP BY
                            party_role_id,
                            instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_223 
                        ON (
                           upper(trim( autoAlias_223.auto_c00 ))= upper(trim(AAPPLLIIDD1EENNVV_contract_party_role_t2.party_role_id)) 
                            AND upper(trim(autoAlias_223.auto_c01)) = upper(trim(AAPPLLIIDD1EENNVV_contract_party_role_t2.instance_id))
                        )"""
        print("Job: 'd8_contract_party_role_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_party_role_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld
        SELECT A.*
        FROM """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld A
        LEFT JOIN (
            SELECT T2.party_role_id
                ,T2.instance_id
            FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t1 T1
            LEFT JOIN (
                SELECT party_role_id
                    ,instance_id
                FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2
                WHERE current_record_ind = 'D'
                ) T2 
                ON upper(trim(T1.party_role_id)) = upper(trim(T2.party_role_id))
                AND upper(trim(T1.instance_id)) = upper(trim(T2.instance_id))
                WHERE T2.party_role_id IS NULL
                AND T2.instance_id IS NULL
            ) T 
            ON upper(trim(A.party_role_id)) = upper(trim(T.party_role_id))
            AND upper(trim(A.instance_id)) = upper(trim(T.instance_id))
            WHERE T.party_role_id IS NULL
            AND T.instance_id IS NULL"""
        print("Job: 'd8_contract_party_role_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld           SELECT
                    instance_id,
                    party_role_id,
                    as_of_date_time,
                    batch_nbr,
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
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT party_role_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_227 
                        ON (
                            upper(trim(party_role_id)) = upper(trim(autoAlias_227.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_227.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_227.AUTO_C00 IS  NULL  
                    AND autoAlias_227.AUTO_C01 IS  NULL"""
        print("Job: 'd8_contract_party_role_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_party_role_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t1"""
        print("Job: 'd8_contract_party_role_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    with qq1 as (
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
                            women_owned_flag,
                            ROW_NUMBER() over (PARTITION BY party_role_id, instance_id 
                                ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK 
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_ml 
                        WHERE
                            upper(trim(tran_code)) = upper(trim('D'))  ) 
        INSERT INTO  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t1 
        SELECT
                   EDW2.instance_id,
                   EDW2.party_role_id,
                   EDW2.alt_contract_header_id,
                   EDW2.as_of_date_time,
                   EDW2.batch_nbr,
                   EDW2.created_by_name,
                   EDW2.contract_line_id,
                   EDW2.contract_header_id,
                   EDW2.creation_date_time,
                   EDW2.facility_code,
                   EDW2.government_code,
                   EDW2.last_update_date_time,
                   EDW2.last_update_login_name,
                   EDW2.last_updated_by_name,
                   EDW2.minority_group_code,
                   EDW2.object1_code,
                   EDW2.object1_id1,
                   EDW2.object1_id2,
                   EDW2.role_code,
                   EDW2.parent_party_role_id,
                   EDW2.primary_party_flag,
                   EDW2.tran_code,
                   EDW2.small_business_flag,
                   EDW2.women_owned_flag
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_ml  
         EDW2 
          INNER JOIN (
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
                    women_owned_flag, 
                   RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.party_role_id)) =upper(trim(IBVL.party_role_id ))
           AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_party_role_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2"""
        print("Job: 'd8_contract_party_role_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH CTE AS 
        (
        SELECT t.party_role_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
         CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat
        from  """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld    AS l
        INNER JOIN 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t1    AS t   
        ON        upper(trim(l.party_role_id)) = upper(trim(t.party_role_id )) 
                    AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
        )
        INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2           SELECT
                    l.instance_id,
                    l.party_role_id,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.alt_contract_header_id,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.created_by_name,
                    l.contract_line_id,
                    l.contract_header_id,
                    l.creation_date_time,
                    null,
                    l.facility_code,
                    l.government_code,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    l.minority_group_code,
                    l.object1_code,
                    l.object1_id1,
                    l.object1_id2,
                    l.role_code,
                    l.parent_party_role_id,
                    l.primary_party_flag,
                    t.tran_code,
                    l.small_business_flag,
                    l.women_owned_flag  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld    AS l   
                    INNER JOIN CTE as t
                    ON
                    upper(trim(l.party_role_id)) = upper(trim(t.party_role_id))
                    AND upper(trim(l.instance_id)) = upper(trim(t.instance_id ))  
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) =t.iCat"""
        print("Job: 'd8_contract_party_role_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_party_role_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                    TABLE """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld
            select
            Q1.* 
            From
            """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld Q1 
            Left Join
                (
                    SELECT
                        party_role_id,
                        instance_id 
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2
                )
                As Q2 
                On upper(trim(Q1.party_role_id)) = upper(trim(Q2.party_role_id)) 
                And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id)) 
            Where
            Q2.party_role_id Is NULL 
            And Q2.instance_id Is Null """
        print("Job: 'd8_contract_party_role_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld           SELECT
                    instance_id,
                    party_role_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
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
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_party_role_t2"""
        print("Job: 'd8_contract_party_role_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractpartyrole01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractpartyrole01inssql").enableHiveSupport().getOrCreate()
    main()
