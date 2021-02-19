from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractgrouplk01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_group_lk_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t1"""
        print("Job: 'd8_contract_group_lk_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """with qq1 as (
        SELECT
                            contract_group_id,
                            instance_id,
                            language_code,
                            as_of_date_time,
                            batch_nbr,
                            contract_group_name,
                            created_by_name,
                            creation_date_time,
                            last_update_date_time,
                            last_update_login_name,
                            last_updated_by_name,
                            contract_group_short_desc,
                            source_language_code,
                            tran_code,
                            ROW_NUMBER() OVER(PARTITION BY contract_group_id ,instance_id ,language_code  
                            ORDER BY ( CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                            batch_nbr)) DESC) AS RNK
                            FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_ml    
                            WHERE upper(trim(tran_code)) <> upper(trim('D'))) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t1   
        SELECT 
                    EDW2.contract_group_id,
                    EDW2.instance_id,
                    EDW2.language_code,
                    EDW2.as_of_date_time,
                    EDW2.batch_nbr,
                    EDW2.contract_group_name,
                    EDW2.created_by_name,
                    EDW2.creation_date_time,
                    EDW2.last_update_date_time,
                    EDW2.last_update_login_name,
                    EDW2.last_updated_by_name,
                    EDW2.contract_group_short_desc,
                    EDW2.source_language_code,
                    EDW2.tran_code 
        FROM 
          """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_ml EDW2 
          INNER JOIN (
            SELECT 
             contract_group_id,
            instance_id,
            language_code,
            as_of_date_time,
               batch_nbr, 
        contract_group_name,
        created_by_name,
        creation_date_time,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        contract_group_short_desc,
        source_language_code,
        tran_code , 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND EDW2.contract_group_id =IBVL.contract_group_id 
          AND upper(trim(EDW2.language_code)) =upper(trim(IBVL.language_code))
          AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_group_lk_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_group_lk_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2"""
        print("Job: 'd8_contract_group_lk_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2           SELECT
                    contract_group_id,
                    instance_id,
                    language_code,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_group_name,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    contract_group_short_desc,
                    source_language_code,
                    tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_group_lk_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_group_id AS auto_c00,
                            instance_id AS auto_c01,
                            language_code AS auto_c02  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t1 
                    ) AS autoAlias_45 
                        ON (
                            contract_group_id = autoAlias_45.AUTO_C00 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_45.AUTO_C01)) 
                            AND upper(trim(language_code)) = upper(trim(autoAlias_45.AUTO_C02)) 
                        )"""
        print("Job: 'd8_contract_group_lk_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t3"""
        print("Job: 'd8_contract_group_lk_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t3           SELECT
                    contract_group_id,
                    instance_id,
                    language_code,
                    contract_group_name,
                    created_by_name,
                    creation_date_time,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    contract_group_short_desc,
                    source_language_code,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_group_id AS auto_c00,
                            instance_id AS auto_c01,
                            language_code AS auto_c02  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t1 
                    ) AS autoAlias_47 
                        ON (
                            contract_group_id = autoAlias_47.AUTO_C00 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_47.AUTO_C01)) 
                            AND upper(trim(language_code)) = upper(trim(autoAlias_47.AUTO_C02)) 
                        )"""
        print("Job: 'd8_contract_group_lk_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_group_lk_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t3           SELECT
                    contract_group_id,
                    instance_id,
                    language_code,
                    contract_group_name,
                    created_by_name,
                    creation_date_time,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    contract_group_short_desc,
                    source_language_code,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_group_id AS auto_c00,
                            instance_id AS auto_c01,
                            language_code AS auto_c02  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2 
                    ) AS autoAlias_293 
                        ON (
                            contract_group_id = autoAlias_293.AUTO_C00 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_293.AUTO_C01)) 
                            AND upper(trim(language_code)) = upper(trim(autoAlias_293.AUTO_C02)) 
                        )"""
        print("Job: 'd8_contract_group_lk_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_group_lk_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2 SELECT
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.contract_group_id AS contract_group_id ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.language_code AS language_code ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.contract_group_name AS contract_group_name ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_49.auto_c00 IS not null and autoAlias_49.auto_c01 is not null and autoAlias_49.auto_c02 is not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_contract_group_lk_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.contract_group_short_desc AS contract_group_short_desc ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.source_language_code AS source_language_code ,
                    AAPPLLIIDD1EENNVV_contract_group_lk_t2.tran_code AS tran_code 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2 AS AAPPLLIIDD1EENNVV_contract_group_lk_t2 
                Left Outer Join
                    (
                        SELECT
                            contract_group_id AS auto_c00,
                            instance_id AS auto_c01,
                            language_code AS auto_c02  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t3      
                        GROUP BY
                            contract_group_id,
                            instance_id,
                            language_code 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_49 
                        ON (
                            autoAlias_49.auto_c00 = AAPPLLIIDD1EENNVV_contract_group_lk_t2.contract_group_id 
                            AND upper(trim( autoAlias_49.auto_c01)) = upper(trim( AAPPLLIIDD1EENNVV_contract_group_lk_t2.instance_id)) 
                            AND upper(trim( autoAlias_49.auto_c02)) = upper(trim( AAPPLLIIDD1EENNVV_contract_group_lk_t2.language_code))
                        )"""
        print("Job: 'd8_contract_group_lk_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_group_lk_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE
                    TABLE """ + db_params.EEDDWWDDBB1 + """.contract_group_lk_ld 
                select
                Q1.* 
                From
                """ + db_params.EEDDWWDDBB1 + """.contract_group_lk_ld Q1 
                Left Join
                    (
                        select
                            A.contract_group_id,
                            A.instance_id,
                            A.language_code 
                        From
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t1 As A 
                            Left Join
                            (
                                SELECT
                                    contract_group_id,
                                    instance_id,
                                    language_code 
                                FROM
                                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2 
                                WHERE
                                    upper(trim(current_record_ind)) = 'D' 
                            )
                            As B 
                            On A.contract_group_id = b.contract_group_id 
                            And upper(trim( A.instance_id )) = upper(trim( B.instance_id ))
                            And upper(trim( A.language_code)) = upper(trim( B.language_code ))
                        Where
                            B.contract_group_id Is NULL 
                            And B.instance_id Is Null 
                            And B.language_code Is Null 
                    )
                    As Q2 
                    On Q1.contract_group_id = Q2.contract_group_id 
                    And Q1.instance_id = Q2.instance_id 
                    And Q1.language_code = Q2.language_code 
                Where
                Q2.contract_group_id Is NULL 
                And Q2.instance_id Is Null 
                And Q2.language_code Is Null"""
        print("Job: 'd8_contract_group_lk_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_group_lk_ld           SELECT
                    contract_group_id,
                    instance_id,
                    language_code,
                    as_of_date_time,
                    batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_group_name,
                    created_by_name,
                    creation_date_time,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    contract_group_short_desc,
                    source_language_code,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT contract_group_id AS auto_c00,
                            instance_id AS auto_c01,
                            language_code AS auto_c02  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_53 
                        ON (
                            contract_group_id = autoAlias_53.AUTO_C00 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_53.AUTO_C01)) 
                            AND upper(trim(language_code)) = upper(trim(autoAlias_53.AUTO_C02)) 
                        ) 
                WHERE
                    autoAlias_53.AUTO_C00 IS  NULL  
                    AND autoAlias_53.AUTO_C01 IS  NULL  
                    AND autoAlias_53.AUTO_C02 IS  NULL"""
        print("Job: 'd8_contract_group_lk_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_group_lk_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t1"""
        print("Job: 'd8_contract_group_lk_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """with qq1 as (
        SELECT
                            contract_group_id,
                            instance_id,
                            language_code,
                            as_of_date_time,
                            batch_nbr,
                            contract_group_name,
                            created_by_name,
                            creation_date_time,
                            last_update_date_time,
                            last_update_login_name,
                            last_updated_by_name,
                            contract_group_short_desc,
                            source_language_code,
                            tran_code,
                            ROW_NUMBER() OVER(PARTITION BY contract_group_id ,instance_id ,language_code  
                            ORDER BY ( CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                            batch_nbr)) DESC) AS RNK
                            FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_ml    
                            WHERE upper(trim(tran_code)) = upper(trim('D'))) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t1 
        SELECT
                            EDW2.contract_group_id,
                            EDW2.instance_id,
                            EDW2.language_code,
                            EDW2.as_of_date_time,
                            EDW2.batch_nbr,
                            EDW2.contract_group_name,
                            EDW2.created_by_name,
                            EDW2.creation_date_time,
                            EDW2.last_update_date_time,
                            EDW2.last_update_login_name,
                            EDW2.last_updated_by_name,
                            EDW2.contract_group_short_desc,
                            EDW2.source_language_code,
                            EDW2.tran_code FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_ml EDW2 
          INNER JOIN (
            SELECT 
              contract_group_id,
                            instance_id,
                            language_code,
                            as_of_date_time,
                            batch_nbr,
                            contract_group_name,
                            created_by_name,
                            creation_date_time,
                            last_update_date_time,
                            last_update_login_name,
                            last_updated_by_name,
                            contract_group_short_desc,
                            source_language_code, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND EDW2.contract_group_id =IBVL.contract_group_id 
          AND upper(trim(EDW2.language_code)) =upper(trim(IBVL.language_code))
          AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_group_lk_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2"""
        print("Job: 'd8_contract_group_lk_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2           SELECT
                    l.contract_group_id,
                    l.instance_id,
                    l.language_code,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.contract_group_name,
                    l.created_by_name,
                    l.creation_date_time,
                    null,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    l.contract_group_short_desc,
                    l.source_language_code,
                    t.tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_group_lk_ld    AS l   ,
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t1    AS t   
                WHERE
                    l.contract_group_id = t.contract_group_id  
                    AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))   
                    AND upper(trim(l.language_code)) = upper(trim(t.language_code))   
                    AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)"""
        print("Job: 'd8_contract_group_lk_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_group_lk_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE
                        TABLE """ + db_params.EEDDWWDDBB1 + """.contract_group_lk_ld       
                SELECT
                Q1.* 
                FROM
                """ + db_params.EEDDWWDDBB1 + """.contract_group_lk_ld AS Q1
                Left Join
                    (
                        SELECT
                            contract_group_id,
                            instance_id,
                            language_code 
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2
                    )
                    As Q2 
                    On  Q1.contract_group_id = Q2.contract_group_id
                    And upper(trim( Q1.instance_id ))= upper(trim( Q2.instance_id)) 
                    And upper(trim( Q1.language_code)) = upper(trim( Q2.language_code)) 
                Where
                Q2.contract_group_id Is NULL 
                And Q2.instance_id Is Null
                And Q2.language_code Is Null """
        print("Job: 'd8_contract_group_lk_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_group_lk_ld           SELECT
                    contract_group_id,
                    instance_id,
                    language_code,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_group_name,
                    created_by_name,
                    creation_date_time,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    contract_group_short_desc,
                    source_language_code,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_lk_t2"""
        print("Job: 'd8_contract_group_lk_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractgrouplk01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractgrouplk01inssql").enableHiveSupport().getOrCreate()
    main()
