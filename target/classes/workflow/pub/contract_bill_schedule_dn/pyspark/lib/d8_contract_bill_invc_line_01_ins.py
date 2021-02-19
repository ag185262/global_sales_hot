from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractbillinvcline01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_invc_line_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t1"""
        print("Job: 'd8_contract_bill_invc_line_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    with qq1 as (
        SELECT
                    bill_transaction_line_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    ROW_NUMBER() OVER (PARTITION BY bill_transaction_line_id, instance_id ORDER BY (CONCAT (CAST (as_of_date_time AS CHAR(26)),batch_nbr)) DESC)  AS RNK,
                    bill_instance_nbr,
                    bill_transaction_id,
                    contract_line_bill_id,
                    contract_subline_bill_id,
                    created_by_name,
                    creation_date_time,
                    invoice_line_amt_ent,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code,
                    cycle_ref_seq_desc,
                    invoice_date,
                    invoice_nbr,
                    invoice_type_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_ml
                WHERE upper(trim(tran_code)) <> 'D' ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t1 
        SELECT 
                 EDW2.bill_transaction_line_id,
                 EDW2.instance_id,
                 EDW2.as_of_date_time,
                 EDW2.batch_nbr,
                 EDW2.bill_instance_nbr,
                 EDW2.bill_transaction_id,
                 EDW2.contract_line_bill_id,
                 EDW2.contract_subline_bill_id,
                 EDW2.created_by_name,
                 EDW2.creation_date_time,
                 EDW2.invoice_line_amt_ent,
                 EDW2.last_update_date_time,
                 EDW2.last_update_login_name,
                 EDW2.last_updated_by_name,
                 EDW2.tran_code,
                 EDW2.cycle_ref_seq_desc,
                 EDW2.invoice_date,
                 EDW2.invoice_nbr,
                 EDW2.invoice_type_code 
        FROM 
          """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_ml EDW2 
          INNER JOIN (
            SELECT 
                    bill_transaction_line_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    bill_instance_nbr,
                    bill_transaction_id,
                    contract_line_bill_id,
                    contract_subline_bill_id,
                    created_by_name,
                    creation_date_time,
                    invoice_line_amt_ent,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code,
                    cycle_ref_seq_desc,
                    invoice_date,
                    invoice_nbr,
                    invoice_type_code, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL ON upper(trim(EDW2.instance_id)) = upper(trim(IBVL.instance_id ))
          WHERE upper(trim(EDW2.bill_transaction_line_id)) = upper(trim(IBVL.bill_transaction_line_id ))
          AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_bill_invc_line_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_invc_line_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2"""
        print("Job: 'd8_contract_bill_invc_line_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2           SELECT
                    bill_transaction_line_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    bill_instance_nbr,
                    bill_transaction_id,
                    contract_line_bill_id,
                    contract_subline_bill_id,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    invoice_line_amt_ent,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code,
                    cycle_ref_seq_desc,
                    invoice_date,
                    invoice_nbr,
                    invoice_type_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT bill_transaction_line_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t1 
                    ) AS autoAlias_19 
                        ON (
                            upper(trim(bill_transaction_line_id)) = upper(trim(autoAlias_19.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_19.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_bill_invc_line_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t3"""
        print("Job: 'd8_contract_bill_invc_line_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t3           SELECT
                    bill_transaction_line_id,
                    instance_id,
                    bill_instance_nbr,
                    bill_transaction_id,
                    contract_line_bill_id,
                    contract_subline_bill_id,
                    created_by_name,
                    creation_date_time,
                    invoice_line_amt_ent,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code,
                    cycle_ref_seq_desc,
                    invoice_date,
                    invoice_nbr,
                    invoice_type_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT bill_transaction_line_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t1 
                    ) AS autoAlias_21 
                        ON (
                            upper(trim(bill_transaction_line_id)) = upper(trim(autoAlias_21.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_21.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_bill_invc_line_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_invc_line_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t3           SELECT
                    bill_transaction_line_id,
                    instance_id,
                    bill_instance_nbr,
                    bill_transaction_id,
                    contract_line_bill_id,
                    contract_subline_bill_id,
                    created_by_name,
                    creation_date_time,
                    invoice_line_amt_ent,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code,
                    cycle_ref_seq_desc,
                    invoice_date,
                    invoice_nbr,
                    invoice_type_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT bill_transaction_line_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2 
                    ) AS autoAlias_23 
                        ON (
                            upper(trim(bill_transaction_line_id)) = upper(trim(autoAlias_23.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_23.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_bill_invc_line_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_invc_line_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2 SELECT
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.bill_transaction_line_id AS bill_transaction_line_id ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.bill_instance_nbr AS bill_instance_nbr ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.bill_transaction_id AS bill_transaction_id ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.contract_line_bill_id AS contract_line_bill_id ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.contract_subline_bill_id AS contract_subline_bill_id ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.creation_date_time AS creation_date_time ,
                    CASE 
        				WHEN autoAlias_25.auto_c00 IS not null AND autoAlias_25.auto_c00 IS not null THEN 'D'
                        ELSE AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.invoice_line_amt_ent AS invoice_line_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.tran_code AS tran_code ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.cycle_ref_seq_desc AS cycle_ref_seq_desc ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.invoice_date AS invoice_date ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.invoice_nbr AS invoice_nbr ,
                    AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.invoice_type_code AS invoice_type_code 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2 AS AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2 
                Left Outer Join
                    (
                        SELECT distinct
                            bill_transaction_line_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t3      
                    )autoAlias_25 
                        ON (
                            upper(trim( autoAlias_25.auto_c00)) = upper(trim( AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.bill_transaction_line_id)) 
                            AND upper(trim( autoAlias_25.auto_c01)) = upper(trim( AAPPLLIIDD1EENNVV_contract_bill_invc_line_t2.instance_id))
                        )"""
        print("Job: 'd8_contract_bill_invc_line_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_bill_invc_line_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld 
                select
                Q1.* 
                From
                """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld Q1 
                Left Join
                    (
                        select
                            A.bill_transaction_line_id,
                            A.instance_id 
                        From
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t1 As A 
                            Left Join
                            (
                                SELECT
                                    bill_transaction_line_id,
                                    instance_id 
                                FROM
                                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2 
                                WHERE
                                    upper(trim( current_record_ind)) = 'D' 
                            )
                            As B 
                            On upper(trim( A.bill_transaction_line_id)) = upper(trim( b.bill_transaction_line_id ))
                            And upper(trim( A.instance_id)) = upper(trim( B.instance_id ))
                        Where
                            B.bill_transaction_line_id Is NULL 
                            And B.instance_id Is Null 
                    )
                    As Q2 
                    On upper(trim( Q1.bill_transaction_line_id)) = upper(trim( Q2.bill_transaction_line_id ))
                    And upper(trim( Q1.instance_id)) = upper(trim( Q2.instance_id ))
                Where
                Q2.bill_transaction_line_id Is NULL 
                And Q2.instance_id Is Null """
        print("Job: 'd8_contract_bill_invc_line_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld           SELECT
                    bill_transaction_line_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    bill_instance_nbr,
                    bill_transaction_id,
                    contract_line_bill_id,
                    contract_subline_bill_id,
                    created_by_name,
                    creation_date_time,
                    invoice_line_amt_ent,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code,
                    cycle_ref_seq_desc,
                    invoice_date,
                    invoice_nbr,
                    invoice_type_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT bill_transaction_line_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_29 
                        ON (
                            upper(trim(bill_transaction_line_id)) = upper(trim(autoAlias_29.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_29.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_29.AUTO_C00 IS  NULL  
                    AND autoAlias_29.AUTO_C01 IS  NULL"""
        print("Job: 'd8_contract_bill_invc_line_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_invc_line_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t1"""
        print("Job: 'd8_contract_bill_invc_line_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    with qq1 as (
        SELECT
                    bill_transaction_line_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    RANK() OVER 
                    (PARTITION BY bill_transaction_line_id, instance_id 
                    ORDER BY 
                    (CONCAT (CAST (as_of_date_time AS CHAR(26)),batch_nbr)) DESC)  AS RNK,
                    bill_instance_nbr,
                    bill_transaction_id,
                    contract_line_bill_id,
                    contract_subline_bill_id,
                    created_by_name,
                    creation_date_time,
                    invoice_line_amt_ent,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code,
                    cycle_ref_seq_desc,
                    invoice_date,
                    invoice_nbr,
                    invoice_type_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_ml 
                    where upper(trim(tran_code)) = upper(trim('D')) ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t1 
        SELECT 
            EDW2.bill_transaction_line_id,
            EDW2.instance_id,
            EDW2.as_of_date_time,
            EDW2.batch_nbr,
            EDW2.bill_instance_nbr,
            EDW2.bill_transaction_id,
            EDW2.contract_line_bill_id,
            EDW2.contract_subline_bill_id,
            EDW2.created_by_name,
            EDW2.creation_date_time,
            EDW2.invoice_line_amt_ent,
            EDW2.last_update_date_time,
            EDW2.last_update_login_name,
            EDW2.last_updated_by_name,
            EDW2.tran_code,
            EDW2.cycle_ref_seq_desc,
            EDW2.invoice_date,
            EDW2.invoice_nbr,
            EDW2.invoice_type_code  
        FROM 
          """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_ml EDW2 
          INNER JOIN (
            SELECT 
              bill_transaction_line_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    bill_instance_nbr,
                    bill_transaction_id,
                    contract_line_bill_id,
                    contract_subline_bill_id,
                    created_by_name,
                    creation_date_time,
                    invoice_line_amt_ent,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code,
                    cycle_ref_seq_desc,
                    invoice_date,
                    invoice_nbr,
                    invoice_type_code, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL ON upper(trim(EDW2.instance_id)) = upper(trim(IBVL.instance_id ))
          WHERE upper(trim(EDW2.bill_transaction_line_id)) = upper(trim(IBVL.bill_transaction_line_id ))
          AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_bill_invc_line_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2"""
        print("Job: 'd8_contract_bill_invc_line_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH CTE AS 
        (
        SELECT t.bill_transaction_line_id,t.instance_id,CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) AS icat, t.batch_nbr, t.as_of_date_time, t.tran_code
        FROM
        """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld    AS l   
        INNER JOIN """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t1    AS t   
        ON 
        upper(trim(l.bill_transaction_line_id)) = upper(trim(t.bill_transaction_line_id )) 
        AND upper(trim(l.instance_id)) = upper(trim(t.instance_id ))  
        WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
        )
        INSERT 
        INTO
        TABLE
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2           SELECT
        l.bill_transaction_line_id,
        l.instance_id,
        t.as_of_date_time,
        t.batch_nbr,
        l.as_of_date_time,
        l.batch_nbr,
        l.bill_instance_nbr,
        l.bill_transaction_id,
        l.contract_line_bill_id,
        l.contract_subline_bill_id,
        l.created_by_name,
        l.creation_date_time,
        null,
        null,
        l.last_update_date_time,
        l.last_update_login_name,
        l.last_updated_by_name,
        t.tran_code,
        l.cycle_ref_seq_desc,
        l.invoice_date,
        l.invoice_nbr,
        l.invoice_type_code  
        FROM
        """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld    AS l   INNER JOIN CTE AS t   
        ON upper(trim(l.bill_transaction_line_id)) = upper(trim(t.bill_transaction_line_id )) 
        AND upper(trim(l.instance_id)) = upper(trim(t.instance_id ))  
        AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) =  t.icat"""
        print("Job: 'd8_contract_bill_invc_line_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_bill_invc_line_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld 
                SELECT
                Q1.* 
                FROM
                """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld AS Q1 
                Left Join
                    (
                        SELECT
                            bill_transaction_line_id,
                            instance_id 
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2 
                    )
                    As Q2 
                    On upper(trim(Q1.bill_transaction_line_id)) = upper(trim(Q2.bill_transaction_line_id ))
                    And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id ))
                Where
                Q2.bill_transaction_line_id Is NULL 
                And Q2.instance_id Is Null """
        print("Job: 'd8_contract_bill_invc_line_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld           SELECT
                    bill_transaction_line_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    bill_instance_nbr,
                    bill_transaction_id,
                    contract_line_bill_id,
                    contract_subline_bill_id,
                    created_by_name,
                    creation_date_time,
                    invoice_line_amt_ent,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code,
                    cycle_ref_seq_desc,
                    invoice_date,
                    invoice_nbr,
                    invoice_type_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_bill_invc_line_t2"""
        print("Job: 'd8_contract_bill_invc_line_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractbillinvcline01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractbillinvcline01inssql").enableHiveSupport().getOrCreate()
    main()
