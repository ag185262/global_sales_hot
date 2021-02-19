from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractsublinebill01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_subline_bill_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t1"""
        print("Job: 'd8_contract_subline_bill_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    with qq1 as (
           SELECT
                        contract_subline_bill_id,
                        instance_id,
                        as_of_date_time,
                        batch_nbr,
                        bill_amt_ent,
                        bill_avg_amt_ent,
                        bill_from_date_time,
                        bill_to_date_time,
                        contract_line_bill_id,
                        contract_line_id,
                        created_by_name,
                        creation_date_time,
                        last_update_date_time,
                        last_update_login_name,
                        last_updated_by_name,
                        tran_code,
                        ROW_NUMBER() over (PARTITION BY contract_subline_bill_id, instance_id 
                            ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK 
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_ml 
                    WHERE
                        upper(trim(tran_code)) <> upper(trim('D')) 
                ) 
        INSERT INTO  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t1  
        SELECT
                EDW2.contract_subline_bill_id,
                EDW2.instance_id,
                EDW2.as_of_date_time,
                EDW2.batch_nbr,
                EDW2.bill_amt_ent,
                EDW2.bill_avg_amt_ent,
                EDW2.bill_from_date_time,
                EDW2.bill_to_date_time,
                EDW2.contract_line_bill_id,
                EDW2.contract_line_id,
                EDW2.created_by_name,
                EDW2.creation_date_time,
                EDW2.last_update_date_time,
                EDW2.last_update_login_name,
                EDW2.last_updated_by_name,
                EDW2.tran_code 
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_ml   
         EDW2 
          INNER JOIN (
            SELECT 
                    contract_subline_bill_id,
                instance_id,
                as_of_date_time,
                batch_nbr,
                bill_amt_ent,
                bill_avg_amt_ent,
                bill_from_date_time,
                bill_to_date_time,
                contract_line_bill_id,
                contract_line_id,
                created_by_name,
                creation_date_time,
                last_update_date_time,
                last_update_login_name,
                last_updated_by_name,
                tran_code, 
                   RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.contract_subline_bill_id)) =upper(trim(IBVL.contract_subline_bill_id ))
           AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_subline_bill_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_subline_bill_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2"""
        print("Job: 'd8_contract_subline_bill_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2           SELECT
                    contract_subline_bill_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    bill_amt_ent,
                    bill_avg_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    contract_line_bill_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_subline_bill_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t1 
                    ) AS autoAlias_235 
                        ON (
                            upper(trim(contract_subline_bill_id)) = upper(trim(autoAlias_235.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_235.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_subline_bill_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t3"""
        print("Job: 'd8_contract_subline_bill_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t3           SELECT
                    contract_subline_bill_id,
                    instance_id,
                    bill_amt_ent,
                    bill_avg_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    contract_line_bill_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_subline_bill_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t1 
                    ) AS autoAlias_237 
                        ON (
                            upper(trim(contract_subline_bill_id)) = upper(trim(autoAlias_237.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_237.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_subline_bill_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_subline_bill_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t3           SELECT
                    contract_subline_bill_id,
                    instance_id,
                    bill_amt_ent,
                    bill_avg_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    contract_line_bill_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_subline_bill_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2 
                    ) AS autoAlias_239 
                        ON (
                            upper(trim(contract_subline_bill_id)) = upper(trim(autoAlias_239.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_239.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_subline_bill_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_subline_bill_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2 SELECT
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.contract_subline_bill_id AS contract_subline_bill_id ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.bill_amt_ent AS bill_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.bill_avg_amt_ent AS bill_avg_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.bill_from_date_time AS bill_from_date_time ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.bill_to_date_time AS bill_to_date_time ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.contract_line_bill_id AS contract_line_bill_id ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.contract_line_id AS contract_line_id ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_241.auto_c00 IS not null and autoAlias_241.auto_c01 IS not null then 'D'
                        ELSE AAPPLLIIDD1EENNVV_contract_subline_bill_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contract_subline_bill_t2.tran_code AS tran_code 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2 AS AAPPLLIIDD1EENNVV_contract_subline_bill_t2 
                Left Outer Join
                    (
                        SELECT
                            contract_subline_bill_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t3      
                        GROUP BY
                            contract_subline_bill_id,
                            instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_241 
                        ON (
                            upper(trim(autoAlias_241.auto_c00)) =       upper(trim(AAPPLLIIDD1EENNVV_contract_subline_bill_t2.contract_subline_bill_id ))
                            AND       upper(trim(autoAlias_241.auto_c01)) =       upper(trim(AAPPLLIIDD1EENNVV_contract_subline_bill_t2.instance_id))
                        )"""
        print("Job: 'd8_contract_subline_bill_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_subline_bill_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE
                    TABLE """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld
                select
                Q1.* 
                From
                """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld Q1 
                Left Join
                    (
                        select
                            A.contract_subline_bill_id,
                            A.instance_id
                        From
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t1   As A 
                            Left Join
                            (
                                SELECT
                                    contract_subline_bill_id,
                                    instance_id 
                                FROM
                                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2  
                                WHERE
                                    current_record_ind = 'D' 
                            )
                            As B 
                            On upper(trim(A.contract_subline_bill_id)) = upper(trim(b.contract_subline_bill_id)) 
                            And upper(trim(A.instance_id)) = upper(trim(B.instance_id))               
                        Where
                            B.contract_subline_bill_id Is NULL 
                            And B.instance_id Is Null            
                    )
                    As Q2 
                    On upper(trim(Q1.contract_subline_bill_id)) = upper(trim(Q2.contract_subline_bill_id ))
                    And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id))        
                Where
                Q2.contract_subline_bill_id Is NULL 
                And Q2.instance_id Is Null """
        print("Job: 'd8_contract_subline_bill_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld           SELECT
                    contract_subline_bill_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    bill_amt_ent,
                    bill_avg_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    contract_line_bill_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT contract_subline_bill_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_245 
                        ON (
                            upper(trim(contract_subline_bill_id)) = upper(trim(autoAlias_245.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_245.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_245.AUTO_C00 IS  NULL  
                    AND autoAlias_245.AUTO_C01 IS  NULL"""
        print("Job: 'd8_contract_subline_bill_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_subline_bill_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t1"""
        print("Job: 'd8_contract_subline_bill_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   with qq1 as (
           SELECT
                        contract_subline_bill_id,
                        instance_id,
                        as_of_date_time,
                        batch_nbr,
                        bill_amt_ent,
                        bill_avg_amt_ent,
                        bill_from_date_time,
                        bill_to_date_time,
                        contract_line_bill_id,
                        contract_line_id,
                        created_by_name,
                        creation_date_time,
                        last_update_date_time,
                        last_update_login_name,
                        last_updated_by_name,
                        tran_code,
                        ROW_NUMBER() over (PARTITION BY contract_subline_bill_id, instance_id 
                            ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK 
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_ml 
                    WHERE
                        upper(trim(tran_code)) = upper(trim('D')) 
                ) 
        INSERT INTO  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t1  
        SELECT
                EDW2.contract_subline_bill_id,
                EDW2.instance_id,
                EDW2.as_of_date_time,
                EDW2.batch_nbr,
                EDW2.bill_amt_ent,
                EDW2.bill_avg_amt_ent,
                EDW2.bill_from_date_time,
                EDW2.bill_to_date_time,
                EDW2.contract_line_bill_id,
                EDW2.contract_line_id,
                EDW2.created_by_name,
                EDW2.creation_date_time,
                EDW2.last_update_date_time,
                EDW2.last_update_login_name,
                EDW2.last_updated_by_name,
                EDW2.tran_code 
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_ml   
         EDW2 
          INNER JOIN (
            SELECT 
                    contract_subline_bill_id,
                instance_id,
                as_of_date_time,
                batch_nbr,
                bill_amt_ent,
                bill_avg_amt_ent,
                bill_from_date_time,
                bill_to_date_time,
                contract_line_bill_id,
                contract_line_id,
                created_by_name,
                creation_date_time,
                last_update_date_time,
                last_update_login_name,
                last_updated_by_name,
                tran_code, 
                   RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.contract_subline_bill_id)) =upper(trim(IBVL.contract_subline_bill_id)) 
           AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_subline_bill_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2"""
        print("Job: 'd8_contract_subline_bill_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH CTE AS 
        (
        SELECT t.contract_subline_bill_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
         CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat
        from  """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld    AS l
        INNER JOIN 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t1    AS t   
        ON        l.contract_subline_bill_id = t.contract_subline_bill_id  
                    AND l.instance_id = t.instance_id
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
        )
        INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2           SELECT
                    l.contract_subline_bill_id,
                    l.instance_id,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.bill_amt_ent,
                    l.bill_avg_amt_ent,
                    l.bill_from_date_time,
                    l.bill_to_date_time,
                    l.contract_line_bill_id,
                    l.contract_line_id,
                    l.created_by_name,
                    l.creation_date_time,
                    null,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    t.tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld    AS l   
                    INNER JOIN CTE as t 
                    ON
                    l.contract_subline_bill_id = t.contract_subline_bill_id  
                    AND l.instance_id = t.instance_id   
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) =t.iCat"""
        print("Job: 'd8_contract_subline_bill_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_subline_bill_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld       
                select
                Q1.* 
                From
               """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld Q1 
                Left Join
                    (
                        SELECT
                            contract_subline_bill_id,
                            instance_id 
                        FROM
                           """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2 
                    )
                    As Q2 
                    On upper(trim(Q1.contract_subline_bill_id)) = upper(trim(Q2.contract_subline_bill_id)) 
                    And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id)) 
                Where
                Q2.contract_subline_bill_id Is NULL 
                And Q2.instance_id Is Null"""
        print("Job: 'd8_contract_subline_bill_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld           SELECT
                    contract_subline_bill_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    bill_amt_ent,
                    bill_avg_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    contract_line_bill_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_subline_bill_t2"""
        print("Job: 'd8_contract_subline_bill_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractsublinebill01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractsublinebill01inssql").enableHiveSupport().getOrCreate()
    main()
