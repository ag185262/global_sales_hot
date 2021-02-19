from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractlinestylelk01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_style_lk_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t1"""
        print("Job: 'd8_contract_line_style_lk_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        with qq1 as (
        SELECT
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
                        tran_code,
                        ROW_NUMBER() over (PARTITION BY  contract_line_style_id ,instance_id,language_code 
                    ORDER BY
                        CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK 
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_ml
                    WHERE
                        upper(trim(tran_code)) <> upper(trim('D')) ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t1 
        SELECT
                    EDW2.contract_line_style_id,
                    EDW2.instance_id,
                    EDW2.language_code,
                    EDW2.as_of_date_time,
                    EDW2.batch_nbr,
                    EDW2.created_by_name,
                    EDW2.creation_date_time,
                    EDW2.last_update_date_time,
                    EDW2.last_update_login_name,
                    EDW2.last_updated_by_name,
                    EDW2.line_style_desc,
                    EDW2.line_style_name,
                    EDW2.source_language_code,
                    EDW2.tran_code
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_ml 
         EDW2 
          INNER JOIN (
            SELECT 
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
                    tran_code, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND EDW2.contract_line_style_id =IBVL.contract_line_style_id 
            AND upper(trim(EDW2.language_code)) =upper(trim(IBVL.language_code)) 
          AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_line_style_lk_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_style_lk_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2"""
        print("Job: 'd8_contract_line_style_lk_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2           SELECT
                    contract_line_style_id,
                    instance_id,
                    language_code,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    line_style_desc,
                    line_style_name,
                    source_language_code,
                    tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_style_id AS auto_c00,
                            instance_id AS auto_c01,
                            language_code AS auto_c02  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t1 
                    ) AS autoAlias_149 
                        ON (
                            contract_line_style_id = autoAlias_149.AUTO_C00 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_149.AUTO_C01)) 
                            AND upper(trim(language_code)) = upper(trim(autoAlias_149.AUTO_C02)) 
                        )"""
        print("Job: 'd8_contract_line_style_lk_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t3"""
        print("Job: 'd8_contract_line_style_lk_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT INTO TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t3
            SELECT contract_line_style_id,
               instance_id,
               language_code,
               alt_as_of_date_time,
               alt_batch_nbr , as_of_date_time,
                                  batch_nbr , created_by_name,
                                                 creation_date_time,
                                                 current_record_ind,
                                                 last_update_date_time,
                                                 last_update_login_name,
                                                 last_updated_by_name,
                                                 line_style_desc,
                                                 line_style_name,
                                                 source_language_code,
                                                 tran_code
            FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2
            INNER JOIN
            (SELECT DISTINCT contract_line_style_id AS auto_c00,
                           instance_id AS auto_c01,
                           language_code AS auto_c02
            FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t1) AS autoAlias_151 
            ON (contract_line_style_id = autoAlias_151.AUTO_C00
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_151.AUTO_C01))
                    AND upper(trim(language_code)) = upper(trim(autoAlias_151.AUTO_C02)))"""
        print("Job: 'd8_contract_line_style_lk_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_style_lk_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t3           SELECT
                    contract_line_style_id,
                    instance_id,
                    language_code,
                    null,
                    null,
                    null,
                    null,
                    created_by_name,
                    creation_date_time,
                    null,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    line_style_desc,
                    line_style_name,
                    source_language_code,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_style_id AS auto_c00,
                            instance_id AS auto_c01,
                            language_code AS auto_c02  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2 
                    ) AS autoAlias_489 
                        ON (
                            contract_line_style_id = autoAlias_489.AUTO_C00 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_489.AUTO_C01)) 
                            AND upper(trim(language_code)) = upper(trim(autoAlias_489.AUTO_C02)) 
                        )"""
        print("Job: 'd8_contract_line_style_lk_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_style_lk_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2 SELECT
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.contract_line_style_id AS contract_line_style_id ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.language_code AS language_code ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_491.auto_c00 IS not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.line_style_desc AS line_style_desc ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.line_style_name AS line_style_name ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.source_language_code AS source_language_code ,
                    AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.tran_code AS tran_code 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2 AS AAPPLLIIDD1EENNVV_contract_line_style_lk_t2 
                Left Outer Join
                    (
                        SELECT
                            contract_line_style_id AS auto_c00,
                            instance_id AS auto_c01,
                            language_code AS auto_c02  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t3      
                        GROUP BY
                            contract_line_style_id,
                            instance_id,
                            language_code 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_491 
                        ON (
                            autoAlias_491.auto_c00 = AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.contract_line_style_id 
                            AND upper(trim(autoAlias_491.auto_c01)) = upper(trim(AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.instance_id)) 
                            AND upper(trim(autoAlias_491.auto_c02)) = upper(trim(AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.language_code))
                        )"""
        print("Job: 'd8_contract_line_style_lk_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_style_lk_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk_ld 
        select
           Q1.* 
        From
           """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk_ld Q1 
           Left Join
              (
                 select
                    A.contract_line_style_id,
                    A.instance_id,
                    A.language_code 
                 From
                   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t1 As A 
                    Left Join
                       (
                          SELECT
                             contract_line_style_id,
                             instance_id,
                             language_code 
                          FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2
                          WHERE
                             current_record_ind = 'D' 
                       )
                       As B 
                       On A.contract_line_style_id = B.contract_line_style_id 
                       And trim(upper(A.instance_id)) =  trim(upper(B.instance_id)) 
                       And  trim(upper(A.language_code)) = trim(upper(B.language_code)) 
                 Where
                    B.contract_line_style_id Is NULL 
                    And B.instance_id Is Null 
                    And B.language_code Is Null 
              )
              As Q2 
              On Q1.contract_line_style_id = Q2.contract_line_style_id 
              And trim(upper(Q1.instance_id)) = trim(upper(Q2.instance_id)) 
              And trim(upper(Q1.language_code)) = trim(upper(Q2.language_code)) 
        Where
           Q2.contract_line_style_id Is NULL 
           And Q2.instance_id Is Null 
           And Q2.language_code Is Null"""
        print("Job: 'd8_contract_line_style_lk_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk_ld           SELECT
                    contract_line_style_id,
                    instance_id,
                    language_code,
                    as_of_date_time,
                    batch_nbr,
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
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_style_id AS auto_c00,
                            instance_id AS auto_c01,
                            language_code AS auto_c02  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_157 
                        ON (
                            contract_line_style_id = autoAlias_157.AUTO_C00 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_157.AUTO_C01)) 
                            AND upper(trim(language_code)) = upper(trim(autoAlias_157.AUTO_C02)) 
                        ) 
                WHERE
                    autoAlias_157.AUTO_C00 IS  NULL  
                    AND autoAlias_157.AUTO_C01 IS  NULL  
                    AND autoAlias_157.AUTO_C02 IS  NULL"""
        print("Job: 'd8_contract_line_style_lk_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_style_lk_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t1"""
        print("Job: 'd8_contract_line_style_lk_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """     with qq1 as (
        SELECT
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
                        tran_code,
                        ROW_NUMBER() over (PARTITION BY  contract_line_style_id ,instance_id,language_code 
                    ORDER BY
                        CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK 
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_ml
                    WHERE
                        upper(trim(tran_code)) = upper(trim('D')) ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t1 
        SELECT
                    EDW2.contract_line_style_id,
                    EDW2.instance_id,
                    EDW2.language_code,
                    EDW2.as_of_date_time,
                    EDW2.batch_nbr,
                    EDW2.created_by_name,
                    EDW2.creation_date_time,
                    EDW2.last_update_date_time,
                    EDW2.last_update_login_name,
                    EDW2.last_updated_by_name,
                    EDW2.line_style_desc,
                    EDW2.line_style_name,
                    EDW2.source_language_code,
                    EDW2.tran_code
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_ml 
         EDW2 
          INNER JOIN (
            SELECT 
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
                    tran_code, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND EDW2.contract_line_style_id =IBVL.contract_line_style_id 
            AND upper(trim(EDW2.language_code)) =upper(trim(IBVL.language_code)) 
          AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_line_style_lk_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2"""
        print("Job: 'd8_contract_line_style_lk_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH CTE AS 
        (
        SELECT t.contract_line_style_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,t.language_code,
         CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat
        from  """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk_ld    AS l
        INNER JOIN 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t1    AS t   
        ON        l.contract_line_style_id = t.contract_line_style_id  
                    AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))
                    AND trim(upper(l.language_code))=trim(upper(t.language_code))         
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
        )
        INSERT  
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2           SELECT
                    l.contract_line_style_id,
                    l.instance_id,
                    l.language_code,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.created_by_name,
                    l.creation_date_time,
                    null,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    l.line_style_desc,
                    l.line_style_name,
                    l.source_language_code,
                    t.tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk_ld    AS l  
                    INNER JOIN CTE as t
                    on 
                    l.contract_line_style_id = t.contract_line_style_id  
                    AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))   
                    AND trim(upper(l.language_code)) = trim(upper(t.language_code))   
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat"""
        print("Job: 'd8_contract_line_style_lk_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_style_lk_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE
                        TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk_ld  
                select
                Q1.* 
                From
                   """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk_ld  Q1 
                Left Join
                    (SELECT
                        contract_line_style_id,
                        instance_id,
                        language_code 
                    FROM
                       """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2          
                    )
                    As Q2 
                    On Q1.contract_line_style_id = Q2.contract_line_style_id 
                    And trim(upper(Q1.instance_id)) = trim(upper(Q2.instance_id)) 
                    And trim(upper(Q1.language_code)) = trim(upper(Q2.language_code)) 
                Where
                Q2.contract_line_style_id Is NULL 
                And Q2.instance_id Is Null 
                And Q2.language_code Is Null"""
        print("Job: 'd8_contract_line_style_lk_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
               """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk_ld           SELECT
                    contract_line_style_id,
                    instance_id,
                    language_code,
                    alt_as_of_date_time,
                    alt_batch_nbr,
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
                FROM
                   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_style_lk_t2"""
        print("Job: 'd8_contract_line_style_lk_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractlinestylelk01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractlinestylelk01inssql").enableHiveSupport().getOrCreate()
    main()
