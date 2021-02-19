from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contrbillstream01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contr_bill_stream_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t1"""
        print("Job: 'd8_contr_bill_stream_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """with qq1 as (
          SELECT
                        contr_bill_stream_id,
                        instance_id,
                        alt_contract_header_id,
                        as_of_date_time,
                        batch_nbr,
                        billing_amt_ent,
                        billing_period_cnt,
                        billing_period_uom_cnt,
                        billing_period_uom_code,
                        billing_seq_nbr,
                        billing_stream_end_date,
                        billing_stream_start_date,
                        contract_header_id,
                        contract_line_id,
                        created_by_name,
                        creation_date_time,
                        interface_offset_day_nbr,
                        invoice_offset_day_nbr,
                        last_update_date_time,
                        last_update_login_name,
                        last_updated_by_name,
                        tran_code ,
                        ROW_NUMBER() OVER(PARTITION BY contr_bill_stream_id ,instance_id  ORDER BY (CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                                batch_nbr)) DESC) RNK
                       FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_ml 
                        WHERE upper(trim(tran_code)) <> upper(trim('D')) 
                ) 
        INSERT INTO    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t1 
        SELECT
                EDW2.contr_bill_stream_id,
                    EDW2.instance_id,
                    EDW2.alt_contract_header_id,
                    EDW2.as_of_date_time,
                    EDW2.batch_nbr,
                    EDW2.billing_amt_ent,
                    EDW2.billing_period_cnt,
                    EDW2.billing_period_uom_cnt,
                    EDW2.billing_period_uom_code,
                    EDW2.billing_seq_nbr,
                    EDW2.billing_stream_end_date,
                    EDW2.billing_stream_start_date,
                    EDW2.contract_header_id,
                    EDW2.contract_line_id,
                    EDW2.created_by_name,
                    EDW2.creation_date_time,
                    EDW2.interface_offset_day_nbr,
                    EDW2.invoice_offset_day_nbr,
                    EDW2.last_update_date_time,
                    EDW2.last_update_login_name,
                    EDW2.last_updated_by_name,
                    EDW2.tran_code
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_ml   
         EDW2 
          INNER JOIN (
            SELECT 
                contr_bill_stream_id,
                    instance_id,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    billing_amt_ent,
                    billing_period_cnt,
                    billing_period_uom_cnt,
                    billing_period_uom_code,
                    billing_seq_nbr,
                    billing_stream_end_date,
                    billing_stream_start_date,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    interface_offset_day_nbr,
                    invoice_offset_day_nbr,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  , 
                   RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.contr_bill_stream_id)) =upper(trim(IBVL.contr_bill_stream_id)) 
           AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contr_bill_stream_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contr_bill_stream_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2"""
        print("Job: 'd8_contr_bill_stream_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2           SELECT
                    contr_bill_stream_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    billing_amt_ent,
                    billing_period_cnt,
                    billing_period_uom_cnt,
                    billing_period_uom_code,
                    billing_seq_nbr,
                    billing_stream_end_date,
                    billing_stream_start_date,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    interface_offset_day_nbr,
                    invoice_offset_day_nbr,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contr_bill_stream_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t1 
                    ) AS autoAlias_3 
                        ON (
                            upper(trim(contr_bill_stream_id)) = upper(trim(autoAlias_3.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_3.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contr_bill_stream_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t3"""
        print("Job: 'd8_contr_bill_stream_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t3           SELECT
                    contr_bill_stream_id,
                    instance_id,
                    alt_contract_header_id,
                    billing_amt_ent,
                    billing_period_cnt,
                    billing_period_uom_cnt,
                    billing_period_uom_code,
                    billing_seq_nbr,
                    billing_stream_end_date,
                    billing_stream_start_date,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    interface_offset_day_nbr,
                    invoice_offset_day_nbr,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contr_bill_stream_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t1 
                    ) AS autoAlias_5 
                        ON (
                            upper(trim(contr_bill_stream_id)) = upper(trim(autoAlias_5.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_5.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contr_bill_stream_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contr_bill_stream_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t3           SELECT
                    contr_bill_stream_id,
                    instance_id,
                    alt_contract_header_id,
                    billing_amt_ent,
                    billing_period_cnt,
                    billing_period_uom_cnt,
                    billing_period_uom_code,
                    billing_seq_nbr,
                    billing_stream_end_date,
                    billing_stream_start_date,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    interface_offset_day_nbr,
                    invoice_offset_day_nbr,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contr_bill_stream_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2 
                    ) AS autoAlias_7 
                        ON (
                            upper(trim(contr_bill_stream_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contr_bill_stream_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contr_bill_stream_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2 SELECT
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.contr_bill_stream_id AS contr_bill_stream_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.alt_contract_header_id AS alt_contract_header_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_amt_ent AS billing_amt_ent ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_period_cnt AS billing_period_cnt ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_period_uom_cnt AS billing_period_uom_cnt ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_period_uom_code AS billing_period_uom_code ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_seq_nbr AS billing_seq_nbr ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_stream_end_date AS billing_stream_end_date ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_stream_start_date AS billing_stream_start_date ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.contract_header_id AS contract_header_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.contract_line_id AS contract_line_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_9.auto_c00 IS not null and autoAlias_9.auto_c01 is not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_contr_bill_stream_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.interface_offset_day_nbr AS interface_offset_day_nbr ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.invoice_offset_day_nbr AS invoice_offset_day_nbr ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contr_bill_stream_t2.tran_code AS tran_code 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2 AS AAPPLLIIDD1EENNVV_contr_bill_stream_t2 
                Left Outer Join
                    (
                        SELECT
                            contr_bill_stream_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t3      
                        GROUP BY
                            contr_bill_stream_id,
                            instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_9 
                        ON (
                            upper(trim(autoAlias_9.auto_c00)) =  upper(trim(AAPPLLIIDD1EENNVV_contr_bill_stream_t2.contr_bill_stream_id ))
                            AND  upper(trim(autoAlias_9.auto_c01)) =  upper(trim(AAPPLLIIDD1EENNVV_contr_bill_stream_t2.instance_id))
                        )"""
        print("Job: 'd8_contr_bill_stream_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contr_bill_stream_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                    TABLE """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld
            select
            Q1.* 
            From
            """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld Q1 
            Left Join
                (
                    select
                        A.contr_bill_stream_id,
                        A.instance_id
                    From
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t1  As A 
                        Left Join
                        (
                            SELECT
                                contr_bill_stream_id,
                                instance_id 
                            FROM
                                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2
                            WHERE
                                current_record_ind = 'D' 
                        )
                        As B 
                        On upper(trim(A.contr_bill_stream_id)) = upper(trim(b.contr_bill_stream_id ))
                        And upper(trim(A.instance_id)) = upper(trim(B.instance_id))
                    Where
                        B.contr_bill_stream_id Is NULL 
                        And B.instance_id Is Null            
                )
                As Q2 
                On upper(trim(Q1.contr_bill_stream_id)) = upper(trim(Q2.contr_bill_stream_id)) 
                And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id))        
            Where
            Q2.contr_bill_stream_id Is NULL 
            And Q2.instance_id Is Null """
        print("Job: 'd8_contr_bill_stream_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld           SELECT
                    contr_bill_stream_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    billing_amt_ent,
                    billing_period_cnt,
                    billing_period_uom_cnt,
                    billing_period_uom_code,
                    billing_seq_nbr,
                    billing_stream_end_date,
                    billing_stream_start_date,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    interface_offset_day_nbr,
                    invoice_offset_day_nbr,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT contr_bill_stream_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_13 
                        ON (
                            upper(trim(contr_bill_stream_id)) = upper(trim(autoAlias_13.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_13.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_13.AUTO_C00 IS  NULL  
                    AND autoAlias_13.AUTO_C01 IS  NULL"""
        print("Job: 'd8_contr_bill_stream_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contr_bill_stream_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t1"""
        print("Job: 'd8_contr_bill_stream_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """with qq1 as (
                        SELECT
                    contr_bill_stream_id,
                    instance_id,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    billing_amt_ent,
                    billing_period_cnt,
                    billing_period_uom_cnt,
                    billing_period_uom_code,
                    billing_seq_nbr,
                    billing_stream_end_date,
                    billing_stream_start_date,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    interface_offset_day_nbr,
                    invoice_offset_day_nbr,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code,
                    ROW_NUMBER() OVER (PARTITION BY contr_bill_stream_id, instance_id order by CONCAT (CAST (as_of_date_time AS CHAR(26)) ,batch_nbr) DESC) AS RNK
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_ml
                WHERE       upper(trim(tran_code)) = 'D'
                )
        INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t1          
        SELECT
                    EDW2.contr_bill_stream_id,
                    EDW2.instance_id,
                    EDW2.alt_contract_header_id,
                    EDW2.as_of_date_time,
                    EDW2.batch_nbr,
                    EDW2.billing_amt_ent,
                    EDW2.billing_period_cnt,
                    EDW2.billing_period_uom_cnt,
                    EDW2.billing_period_uom_code,
                    EDW2.billing_seq_nbr,
                    EDW2.billing_stream_end_date,
                    EDW2.billing_stream_start_date,
                    EDW2.contract_header_id,
                    EDW2.contract_line_id,
                    EDW2.created_by_name,
                    EDW2.creation_date_time,
                    EDW2.interface_offset_day_nbr,
                    EDW2.invoice_offset_day_nbr,
                    EDW2.last_update_date_time,
                    EDW2.last_update_login_name,
                    EDW2.last_updated_by_name,
                    EDW2.tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_ml EDW2
                INNER JOIN
                   (
                        SELECT
                    contr_bill_stream_id,
                    instance_id,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    billing_amt_ent,
                    billing_period_cnt,
                    billing_period_uom_cnt,
                    billing_period_uom_code,
                    billing_seq_nbr,
                    billing_stream_end_date,
                    billing_stream_start_date,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    interface_offset_day_nbr,
                    invoice_offset_day_nbr,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code
                  FROM QQ1 WHERE RNK = 1
                  )IBVL
        on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND EDW2.contr_bill_stream_id =IBVL.contr_bill_stream_id 
           AND upper(trim(EDW2.tran_code)) ='D'"""
        print("Job: 'd8_contr_bill_stream_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2"""
        print("Job: 'd8_contr_bill_stream_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH QQ1 AS 
        (
        SELECT DISTINCT 
        ld.contr_bill_stream_id,
        ld.instance_id,
        t1.as_of_date_time AS alt_as_of_date_time,
        t1.batch_nbr AS alt_batch_nbr,
        ld.alt_contract_header_id,
        ld.as_of_date_time AS as_of_date_time,
        ld.batch_nbr,
        ld.billing_amt_ent,
        ld.billing_period_cnt,
        ld.billing_period_uom_cnt,
        ld.billing_period_uom_code,
        ld.billing_seq_nbr,
        ld.billing_stream_end_date,
        ld.billing_stream_start_date,
        ld.contract_header_id,
        ld.contract_line_id,
        ld.created_by_name,
        ld.creation_date_time,
        null,
        ld.interface_offset_day_nbr,
        ld.invoice_offset_day_nbr,
        ld.last_update_date_time,
        ld.last_update_login_name,
        ld.last_updated_by_name,
        t1.tran_code  
        FROM """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld    ld
        INNER JOIN
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t1 t1 
        ON
        upper(trim(ld.contr_bill_stream_id)) = upper(trim(t1.contr_bill_stream_id))  
        AND upper(trim(ld.instance_id)) = upper(trim(t1.instance_id)) WHERE CONCAT (CAST (ld.as_of_date_time AS VARCHAR(26)) , ld.batch_nbr) <  CONCAT (CAST (t1.as_of_date_time AS VARCHAR(26)) , t1.batch_nbr))                       
        INSERT INTO TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2
        SELECT  * FROM QQ1"""
        print("Job: 'd8_contr_bill_stream_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contr_bill_stream_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE
                        TABLE """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld
                SELECT
                Q1.* 
                FROM
                """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld AS Q1
                Left Join
                    (
                        SELECT
                            contr_bill_stream_id,
                            instance_id 
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2
                    )
                    As Q2 
                    On upper(trim(Q1.contr_bill_stream_id)) = upper(trim(Q2.contr_bill_stream_id ))
                    And upper(trim(Q1.instance_id ))= upper(trim(Q2.instance_id ))
                Where
                Q2.contr_bill_stream_id Is NULL 
                And Q2.instance_id Is Null """
        print("Job: 'd8_contr_bill_stream_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld           SELECT
                    contr_bill_stream_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    billing_amt_ent,
                    billing_period_cnt,
                    billing_period_uom_cnt,
                    billing_period_uom_code,
                    billing_seq_nbr,
                    billing_stream_end_date,
                    billing_stream_start_date,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    interface_offset_day_nbr,
                    invoice_offset_day_nbr,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_t2"""
        print("Job: 'd8_contr_bill_stream_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contrbillstream01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contrbillstream01inssql").enableHiveSupport().getOrCreate()
    main()
