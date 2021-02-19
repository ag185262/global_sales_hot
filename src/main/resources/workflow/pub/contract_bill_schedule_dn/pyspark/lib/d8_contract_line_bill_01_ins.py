from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractlinebill01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_bill_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t1"""
        print("Job: 'd8_contract_line_bill_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """     with qq1 as (
         SELECT
                            contract_line_bill_id,
                            instance_id,
                            as_of_date_time,
                            batch_nbr,
                            bill_action_code,
                            bill_amt_ent,
                            bill_from_date_time,
                            bill_to_date_time,
                            bill_transaction_id,
                            contract_line_id,
                            created_by_name,
                            creation_date_time,
                            curr_code,
                            last_update_date_time,
                            last_update_login_name,
                            last_updated_by_name,
                            next_invoice_date_time,
                            sent_flag,
                            tran_code,
                            ROW_NUMBER() over (PARTITION BY contract_line_bill_id, instance_id 
                            ORDER BY
                            CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK 
                            FROM
                                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_ml 
                            WHERE
                            upper(trim(tran_code)) <> upper(trim('D'))  ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t1 
        SELECT
                EDW2.contract_line_bill_id,
                EDW2.instance_id,
                EDW2.as_of_date_time,
                EDW2.batch_nbr,
                EDW2.bill_action_code,
                EDW2.bill_amt_ent,
                EDW2.bill_from_date_time,
                EDW2.bill_to_date_time,
                EDW2.bill_transaction_id,
                EDW2.contract_line_id,
                EDW2.created_by_name,
                EDW2.creation_date_time,
                EDW2.curr_code,
                EDW2.last_update_date_time,
                EDW2.last_update_login_name,
                EDW2.last_updated_by_name,
                EDW2.next_invoice_date_time,
                EDW2.sent_flag,
                EDW2.tran_code
        FROM     """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_ml 
         EDW2 
          INNER JOIN (
            SELECT 
               contract_line_bill_id,
                instance_id,
                as_of_date_time,
                batch_nbr,
                bill_action_code,
                bill_amt_ent,
                bill_from_date_time,
                bill_to_date_time,
                bill_transaction_id,
                contract_line_id,
                created_by_name,
                creation_date_time,
                curr_code,
                last_update_date_time,
                last_update_login_name,
                last_updated_by_name,
                next_invoice_date_time,
                sent_flag,
                tran_code, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.contract_line_bill_id)) =upper(trim(IBVL.contract_line_bill_id ))
          AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_line_bill_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_bill_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2"""
        print("Job: 'd8_contract_line_bill_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2           SELECT
                    contract_line_bill_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    bill_action_code,
                    bill_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    bill_transaction_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    curr_code,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    next_invoice_date_time,
                    sent_flag,
                    tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_bill_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t1 
                    ) AS autoAlias_131 
                        ON (
                            upper(trim(contract_line_bill_id)) = upper(trim(autoAlias_131.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_131.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_line_bill_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t3"""
        print("Job: 'd8_contract_line_bill_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t3           SELECT
                    contract_line_bill_id,
                    instance_id,
                    bill_action_code,
                    bill_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    bill_transaction_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    next_invoice_date_time,
                    sent_flag,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_bill_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t1 
                    ) AS autoAlias_133 
                        ON (
                            upper(trim(contract_line_bill_id)) = upper(trim(autoAlias_133.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_133.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_line_bill_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_bill_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t3           SELECT
                    contract_line_bill_id,
                    instance_id,
                    bill_action_code,
                    bill_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    bill_transaction_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    next_invoice_date_time,
                    sent_flag,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_bill_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2 
                    ) AS autoAlias_135 
                        ON (
                            upper(trim(contract_line_bill_id)) = upper(trim(autoAlias_135.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_135.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_line_bill_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_bill_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2 SELECT
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.contract_line_bill_id AS contract_line_bill_id ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.bill_action_code AS bill_action_code ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.bill_amt_ent AS bill_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.bill_from_date_time AS bill_from_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.bill_to_date_time AS bill_to_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.bill_transaction_id AS bill_transaction_id ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.contract_line_id AS contract_line_id ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_137.auto_c00 IS not null and autoAlias_137.auto_c01 is not null  THEN 'D'
                        ELSE AAPPLLIIDD1EENNVV_contract_line_bill_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.curr_code AS curr_code ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.next_invoice_date_time AS next_invoice_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.sent_flag AS sent_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_bill_t2.tran_code AS tran_code 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2 AS AAPPLLIIDD1EENNVV_contract_line_bill_t2 
                Left Outer Join
                    (
                        SELECT
                            contract_line_bill_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t3      
                        GROUP BY
                            contract_line_bill_id,
                            instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_137 
                        ON (
                            upper(trim( autoAlias_137.auto_c00)) = upper(trim( AAPPLLIIDD1EENNVV_contract_line_bill_t2.contract_line_bill_id ))
                            AND upper(trim( autoAlias_137.auto_c01)) = upper(trim( AAPPLLIIDD1EENNVV_contract_line_bill_t2.instance_id))
                        )"""
        print("Job: 'd8_contract_line_bill_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_bill_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld          
        select Q1.*  From """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld   Q1
          Left Join
            ( select A.contract_line_bill_id,A.instance_id 
                  From  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t1 A
                 Left Join(select contract_line_bill_id,instance_id From """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2 Where current_record_ind='D') As B
                    On upper(trim( A.contract_line_bill_id))=upper(trim( b.contract_line_bill_id ))And upper(trim( A.instance_id))=upper(trim( B.instance_id))
                    Where B.contract_line_bill_id Is NULL And B.instance_id Is Null
            ) As Q2
            On upper(trim( Q1.contract_line_bill_id))=upper(trim( Q2.contract_line_bill_id)) And upper(trim( Q1.instance_id))=upper(trim( Q2.instance_id))
        Where Q2.contract_line_bill_id Is Null And Q2.instance_id Is Null"""
        print("Job: 'd8_contract_line_bill_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld           SELECT
                    contract_line_bill_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    bill_action_code,
                    bill_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    bill_transaction_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    next_invoice_date_time,
                    sent_flag,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_bill_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_141 
                        ON (
                            upper(trim(contract_line_bill_id)) = upper(trim(autoAlias_141.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_141.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_141.AUTO_C00 IS  NULL  
                    AND autoAlias_141.AUTO_C01 IS  NULL"""
        print("Job: 'd8_contract_line_bill_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_bill_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t1"""
        print("Job: 'd8_contract_line_bill_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """       with qq1 as (
         SELECT
                            contract_line_bill_id,
                            instance_id,
                            as_of_date_time,
                            batch_nbr,
                            bill_action_code,
                            bill_amt_ent,
                            bill_from_date_time,
                            bill_to_date_time,
                            bill_transaction_id,
                            contract_line_id,
                            created_by_name,
                            creation_date_time,
                            curr_code,
                            last_update_date_time,
                            last_update_login_name,
                            last_updated_by_name,
                            next_invoice_date_time,
                            sent_flag,
                            tran_code,
                            ROW_NUMBER() over (PARTITION BY contract_line_bill_id, instance_id 
                            ORDER BY
                            CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK 
                            FROM
                                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_ml 
                            WHERE
                            upper(trim(tran_code)) = upper(trim('D'))  ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t1 
        SELECT
                EDW2.contract_line_bill_id,
                EDW2.instance_id,
                EDW2.as_of_date_time,
                EDW2.batch_nbr,
                EDW2.bill_action_code,
                EDW2.bill_amt_ent,
                EDW2.bill_from_date_time,
                EDW2.bill_to_date_time,
                EDW2.bill_transaction_id,
                EDW2.contract_line_id,
                EDW2.created_by_name,
                EDW2.creation_date_time,
                EDW2.curr_code,
                EDW2.last_update_date_time,
                EDW2.last_update_login_name,
                EDW2.last_updated_by_name,
                EDW2.next_invoice_date_time,
                EDW2.sent_flag,
                EDW2.tran_code
        FROM     """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_ml 
         EDW2 
          INNER JOIN (
            SELECT 
               contract_line_bill_id,
                instance_id,
                as_of_date_time,
                batch_nbr,
                bill_action_code,
                bill_amt_ent,
                bill_from_date_time,
                bill_to_date_time,
                bill_transaction_id,
                contract_line_id,
                created_by_name,
                creation_date_time,
                curr_code,
                last_update_date_time,
                last_update_login_name,
                last_updated_by_name,
                next_invoice_date_time,
                sent_flag,
                tran_code, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.contract_line_bill_id ))=upper(trim(IBVL.contract_line_bill_id ))
          AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_line_bill_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2"""
        print("Job: 'd8_contract_line_bill_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH CTE AS 
        (
        SELECT t.contract_line_bill_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
         CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat
        from  """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld    AS l
        INNER JOIN 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t1    AS t   
        ON        upper(trim(l.contract_line_bill_id)) = upper(trim(t.contract_line_bill_id )) 
                    AND upper(trim(l.instance_id)) = upper(trim(t.instance_id ))  
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
        )
        INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2           SELECT
                    l.contract_line_bill_id,
                    l.instance_id,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.bill_action_code,
                    l.bill_amt_ent,
                    l.bill_from_date_time,
                    l.bill_to_date_time,
                    l.bill_transaction_id,
                    l.contract_line_id,
                    l.created_by_name,
                    l.creation_date_time,
                    null,
                    l.curr_code,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    l.next_invoice_date_time,
                    l.sent_flag,
                    t.tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld    AS l  
                   INNER JOIN CTE AS t
                   ON upper(trim(t.contract_line_bill_id))=upper(trim(l.contract_line_bill_id))
                   and upper(trim(t.instance_id))=upper(trim(l.instance_id))
                   where CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat"""
        print("Job: 'd8_contract_line_bill_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_bill_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                    TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld          
             Select Q1.* from """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld Q1
               Left Join (SELECT contract_line_bill_id, instance_id   FROM  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2 ) As Q2
                On upper(trim( Q1.contract_line_bill_id))=upper(trim( Q2.contract_line_bill_id))
                And upper(trim( Q1.instance_id))=upper(trim( Q2.instance_id))
             Where Q2.contract_line_bill_id Is NULL
               And Q2.instance_id Is Null"""
        print("Job: 'd8_contract_line_bill_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld           SELECT
                    contract_line_bill_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    bill_action_code,
                    bill_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    bill_transaction_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    next_invoice_date_time,
                    sent_flag,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_bill_t2"""
        print("Job: 'd8_contract_line_bill_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractlinebill01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractlinebill01inssql").enableHiveSupport().getOrCreate()
    main()
