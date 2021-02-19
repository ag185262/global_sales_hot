from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractlvlelement01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1"""
        print("Job: 'd8_contract_lvl_element_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """       with qq1 as (
          SELECT
                         instance_id,
                         level_element_id,
                         amount_due_date_time,
                         as_of_date_time,
                         batch_nbr,
                         completed_date_time,
                         created_by_name,
                         creation_date_time,
                         gl_receivable_date_time,
                         interface_date_time,
                         last_update_date_time,
                         last_updated_by_name,
                         level_element_amt_ent,
                         period_start_date,
                         print_date_time,
                         rule_id,
                         rule_start_date_time,
                         sequence_nbr,
                         tran_code,
                         transaction_date_time,
                         alt_contract_header_id,
                         contract_line_id,
                         parent_contract_line_id,
                         period_end_date,
                         null,
                         ROW_NUMBER() over (PARTITION BY instance_id , level_element_id 
                            ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) , tran_code, batch_nbr) DESC) As RNK
                      FROM
                         """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_ml 
                      WHERE
                         upper(trim(tran_code)) <> upper(trim('D')) ) 
        INSERT INTO  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1  
        SELECT
                   EDW2.instance_id,
                   EDW2.level_element_id,
                   EDW2.amount_due_date_time,
                   EDW2.as_of_date_time,
                   EDW2.batch_nbr,
                   EDW2.completed_date_time,
                   EDW2.created_by_name,
                   EDW2.creation_date_time,
                   EDW2.gl_receivable_date_time,
                   EDW2.interface_date_time,
                   EDW2.last_update_date_time,
                   EDW2.last_updated_by_name,
                   EDW2.level_element_amt_ent,
                   EDW2.period_start_date,
                   EDW2.print_date_time,
                   EDW2.rule_id,
                   EDW2.rule_start_date_time,
                   EDW2.sequence_nbr,
                   EDW2.tran_code,
                   EDW2.transaction_date_time,
                   EDW2.alt_contract_header_id,
                   EDW2.contract_line_id,
                   EDW2.parent_contract_line_id,
                   EDW2.period_end_date,
                   null
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_ml 
         EDW2 
          INNER JOIN (
            SELECT 
                    instance_id,
                   level_element_id,
                   amount_due_date_time,
                   as_of_date_time,
                   batch_nbr,
                   completed_date_time,
                   created_by_name,
                   creation_date_time,
                   gl_receivable_date_time,
                   interface_date_time,
                   last_update_date_time,
                   last_updated_by_name,
                   level_element_amt_ent,
                   period_start_date,
                   print_date_time,
                   rule_id,
                   rule_start_date_time,
                   sequence_nbr,
                   tran_code,
                   transaction_date_time,
                   alt_contract_header_id,
                   contract_line_id,
                   parent_contract_line_id,
                   period_end_date, 
                   RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.level_element_id)) =upper(trim(IBVL.level_element_id)) 
           AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_lvl_element_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element COMPUTE STATISTICS  FOR COLUMNS level_element_id,instance_id"""
        print("Job: 'd8_contract_lvl_element_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld COMPUTE STATISTICS"""
        print("Job: 'd8_contract_lvl_element_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2"""
        print("Job: 'd8_contract_lvl_element_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2           SELECT
                    instance_id,
                    level_element_id,
                    amount_due_date_time,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    completed_date_time,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    gl_receivable_date_time,
                    interface_date_time,
                    last_update_date_time,
                    last_updated_by_name,
                    level_element_amt_ent,
                    period_start_date,
                    print_date_time,
                    rule_id,
                    rule_start_date_time,
                    sequence_nbr,
                    tran_code,
                    transaction_date_time,
                    alt_contract_header_id,
                    contract_line_id,
                    parent_contract_line_id,
                    period_end_date,
                    null  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld     
                WHERE
                    EXISTS  (
                        SELECT
                            1  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1     
                        WHERE
                            upper(trim(""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1.level_element_id)) =     upper(trim(contract_lvl_element_ld.level_element_id )) 
                            AND     upper(trim(""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1.instance_id)) =     upper(trim(contract_lvl_element_ld.instance_id ))     
                    )  
                    AND upper(trim(contract_lvl_element_ld.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_lvl_element_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t3"""
        print("Job: 'd8_contract_lvl_element_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t3           SELECT
                    instance_id,
                    level_element_id,
                    amount_due_date_time,
                    completed_date_time,
                    created_by_name,
                    creation_date_time,
                    gl_receivable_date_time,
                    interface_date_time,
                    last_update_date_time,
                    last_updated_by_name,
                    level_element_amt_ent,
                    period_start_date,
                    print_date_time,
                    rule_id,
                    rule_start_date_time,
                    sequence_nbr,
                    tran_code,
                    transaction_date_time,
                    alt_contract_header_id,
                    contract_line_id,
                    parent_contract_line_id,
                    period_end_date,
                    null  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT level_element_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1 
                    ) AS autoAlias_201 
                        ON (
                            upper(trim(level_element_id)) = upper(trim(autoAlias_201.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_201.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_lvl_element_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t3           SELECT
                    instance_id,
                    level_element_id,
                    amount_due_date_time,
                    completed_date_time,
                    created_by_name,
                    creation_date_time,
                    gl_receivable_date_time,
                    interface_date_time,
                    last_update_date_time,
                    last_updated_by_name,
                    level_element_amt_ent,
                    period_start_date,
                    print_date_time,
                    rule_id,
                    rule_start_date_time,
                    sequence_nbr,
                    tran_code,
                    transaction_date_time,
                    alt_contract_header_id,
                    contract_line_id,
                    parent_contract_line_id,
                    period_end_date,
                    null  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT level_element_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2 
                    ) AS autoAlias_203 
                        ON (
                            upper(trim(level_element_id)) = upper(trim(autoAlias_203.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_203.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_lvl_element_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2 SELECT
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.level_element_id AS level_element_id ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.amount_due_date_time AS amount_due_date_time ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.completed_date_time AS completed_date_time ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_205.auto_c00 IS not null and autoAlias_205.auto_c01 IS not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_contract_lvl_element_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.gl_receivable_date_time AS gl_receivable_date_time ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.interface_date_time AS interface_date_time ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.level_element_amt_ent AS level_element_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.period_start_date AS period_start_date ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.print_date_time AS print_date_time ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.rule_id AS rule_id ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.rule_start_date_time AS rule_start_date_time ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.sequence_nbr AS sequence_nbr ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.tran_code AS tran_code ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.transaction_date_time AS transaction_date_time ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.alt_contract_header_id AS alt_contract_header_id ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.contract_line_id AS contract_line_id ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.parent_contract_line_id AS parent_contract_line_id ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.period_end_date AS period_end_date ,
                    AAPPLLIIDD1EENNVV_contract_lvl_element_t2.level_element_amt_us AS level_element_amt_us 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2 AS AAPPLLIIDD1EENNVV_contract_lvl_element_t2 
                Left Outer Join
                    (
                        SELECT
                            level_element_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t3      
                        GROUP BY
                            level_element_id,
                            instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_205 
                        ON (
                            upper(trim(autoAlias_205.auto_c00)) =  upper(trim(AAPPLLIIDD1EENNVV_contract_lvl_element_t2.level_element_id)) 
                            AND  upper(trim(autoAlias_205.auto_c01)) =  upper(trim(AAPPLLIIDD1EENNVV_contract_lvl_element_t2.instance_id))
                        )"""
        print("Job: 'd8_contract_lvl_element_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld 
            select
            Q1.* 
            From
           """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld Q1 
            Left Join
                (
                    select
                        A.level_element_id,
                        A.instance_id 
                    From
                       """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1 As A 
                        Left Join
                        (
                            SELECT
                                level_element_id,
                                instance_id 
                            FROM
                               """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2 
                            WHERE
                                current_record_ind = 'D' 
                        )
                        As B 
                        On upper(trim(A.level_element_id)) = upper(trim(B.level_element_id ))
                        And upper(trim( A.instance_id)) = upper(trim(B.instance_id ))
                    Where
                        B.level_element_id Is NULL 
                        And B.instance_id Is Null 
                )
                As Q2 
                On upper(trim(Q1.level_element_id)) = upper(trim(Q2.level_element_id ))
                And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id)) 
            Where
            Q2.level_element_id Is NULL 
            And Q2.instance_id Is Null """
        print("Job: 'd8_contract_lvl_element_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld           SELECT
                    instance_id,
                    level_element_id,
                    as_of_date_time,
                    batch_nbr,
                    amount_due_date_time,
                    as_of_date_time,
                    batch_nbr,
                    completed_date_time,
                    created_by_name,
                    creation_date_time,
                    gl_receivable_date_time,
                    interface_date_time,
                    last_update_date_time,
                    last_updated_by_name,
                    level_element_amt_ent,
                    period_start_date,
                    print_date_time,
                    rule_id,
                    rule_start_date_time,
                    sequence_nbr,
                    tran_code,
                    transaction_date_time,
                    alt_contract_header_id,
                    contract_line_id,
                    parent_contract_line_id,
                    period_end_date,
                    null  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT level_element_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_209 
                        ON (
                            upper(trim(level_element_id)) = upper(trim(autoAlias_209.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_209.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_209.AUTO_C00 IS  NULL  
                    AND autoAlias_209.AUTO_C01 IS  NULL  
                GROUP BY
                    instance_id ,
                    level_element_id ,
                    as_of_date_time ,
                    batch_nbr ,
                    amount_due_date_time ,
                    as_of_date_time ,
                    batch_nbr ,
                    completed_date_time ,
                    created_by_name ,
                    creation_date_time ,
                    gl_receivable_date_time ,
                    interface_date_time ,
                    last_update_date_time ,
                    last_updated_by_name ,
                    level_element_amt_ent ,
                    period_start_date ,
                    print_date_time ,
                    rule_id ,
                    rule_start_date_time ,
                    sequence_nbr ,
                    tran_code ,
                    transaction_date_time ,
                    alt_contract_header_id ,
                    contract_line_id ,
                    parent_contract_line_id ,
                    period_end_date"""
        print("Job: 'd8_contract_lvl_element_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_05a'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
        TABLE """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element SELECT
        Q1.* 
        FROM
            """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element Q1  
        LEFT OUTER JOIN
        (
        SELECT DISTINCT A.*
        FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1 A 
        LEFT OUTER JOIN """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2 B
        ON upper(trim(A.level_element_id)) = upper(trim(B.level_element_id))
        and upper(trim(A.instance_id)) = upper(trim(B.instance_id))
        WHERE B.level_element_id IS NULL AND B.instance_id IS NULL AND upper(trim(current_record_ind)) = 'D'
        )Q2
        ON
        upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id))
        AND upper(trim(Q1.level_element_id)) = upper(trim(Q2.level_element_id)) AND Q2.level_element_id IS NOT NULL AND Q2.instance_id IS NOT NULL"""
        print("Job: 'd8_contract_lvl_element_05a'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element           SELECT
                    instance_id,
                    level_element_id,
                    as_of_date_time,
                    batch_nbr,
                    amount_due_date_time,
                    as_of_date_time,
                    batch_nbr,
                    completed_date_time,
                    created_by_name,
                    creation_date_time,
                    gl_receivable_date_time,
                    interface_date_time,
                    last_update_date_time,
                    last_updated_by_name,
                    level_element_amt_ent,
                    period_start_date,
                    print_date_time,
                    rule_id,
                    rule_start_date_time,
                    sequence_nbr,
                    tran_code,
                    transaction_date_time,
                    alt_contract_header_id,
                    contract_line_id,
                    parent_contract_line_id,
                    period_end_date,
                    null  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT level_element_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_622 
                        ON (
                            upper(trim(level_element_id)) = upper(trim(autoAlias_622.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_622.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_622.AUTO_C00 IS  NULL  
                    AND autoAlias_622.AUTO_C01 IS  NULL  
                GROUP BY
                    instance_id ,
                    level_element_id ,
                    as_of_date_time ,
                    batch_nbr ,
                    amount_due_date_time ,
                    as_of_date_time ,
                    batch_nbr ,
                    completed_date_time ,
                    created_by_name ,
                    creation_date_time ,
                    gl_receivable_date_time ,
                    interface_date_time ,
                    last_update_date_time ,
                    last_updated_by_name ,
                    level_element_amt_ent ,
                    period_start_date ,
                    print_date_time ,
                    rule_id ,
                    rule_start_date_time ,
                    sequence_nbr ,
                    tran_code ,
                    transaction_date_time ,
                    alt_contract_header_id ,
                    contract_line_id ,
                    parent_contract_line_id ,
                    period_end_date"""
        print("Job: 'd8_contract_lvl_element_05a'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1"""
        print("Job: 'd8_contract_lvl_element_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    with qq1 as (
          SELECT
                         instance_id,
                         level_element_id,
                         amount_due_date_time,
                         as_of_date_time,
                         batch_nbr,
                         completed_date_time,
                         created_by_name,
                         creation_date_time,
                         gl_receivable_date_time,
                         interface_date_time,
                         last_update_date_time,
                         last_updated_by_name,
                         level_element_amt_ent,
                         period_start_date,
                         print_date_time,
                         rule_id,
                         rule_start_date_time,
                         sequence_nbr,
                         tran_code,
                         transaction_date_time,
                         alt_contract_header_id,
                         contract_line_id,
                         parent_contract_line_id,
                         period_end_date,
                         null,
                         ROW_NUMBER() over (PARTITION BY instance_id , level_element_id 
                            ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK
                      FROM
                         """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_ml 
                      WHERE
                         upper(trim(tran_code)) = upper(trim('D')) ) 
        INSERT INTO  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1  
        SELECT
                   EDW2.instance_id,
                   EDW2.level_element_id,
                   EDW2.amount_due_date_time,
                   EDW2.as_of_date_time,
                   EDW2.batch_nbr,
                   EDW2.completed_date_time,
                   EDW2.created_by_name,
                   EDW2.creation_date_time,
                   EDW2.gl_receivable_date_time,
                   EDW2.interface_date_time,
                   EDW2.last_update_date_time,
                   EDW2.last_updated_by_name,
                   EDW2.level_element_amt_ent,
                   EDW2.period_start_date,
                   EDW2.print_date_time,
                   EDW2.rule_id,
                   EDW2.rule_start_date_time,
                   EDW2.sequence_nbr,
                   EDW2.tran_code,
                   EDW2.transaction_date_time,
                   EDW2.alt_contract_header_id,
                   EDW2.contract_line_id,
                   EDW2.parent_contract_line_id,
                   EDW2.period_end_date,
                   null
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_ml 
         EDW2 
          INNER JOIN (
            SELECT 
                    instance_id,
                   level_element_id,
                   amount_due_date_time,
                   as_of_date_time,
                   batch_nbr,
                   completed_date_time,
                   created_by_name,
                   creation_date_time,
                   gl_receivable_date_time,
                   interface_date_time,
                   last_update_date_time,
                   last_updated_by_name,
                   level_element_amt_ent,
                   period_start_date,
                   print_date_time,
                   rule_id,
                   rule_start_date_time,
                   sequence_nbr,
                   tran_code,
                   transaction_date_time,
                   alt_contract_header_id,
                   contract_line_id,
                   parent_contract_line_id,
                   period_end_date, 
                   RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.level_element_id ))=upper(trim(IBVL.level_element_id ))
           AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_lvl_element_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2"""
        print("Job: 'd8_contract_lvl_element_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH CTE AS 
        (
        SELECT t.level_element_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
         CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat,
         upper(trim(l.contract_line_id)) AS cl_Id,upper(trim(l.rule_id)) AS rl_id
        from  """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld    AS l
        INNER JOIN 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1    AS t   
        ON        upper(trim(l.level_element_id)) = upper(trim(t.level_element_id )) 
                    AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
                    AND upper(trim(l.contract_line_id)) <> upper(trim(' '))   
                    AND upper(trim(l.rule_id)) <> upper(trim(' '))
        )
        INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2           SELECT
                    l.instance_id,
                    l.level_element_id,
                    l.amount_due_date_time,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.completed_date_time,
                    l.created_by_name,
                    l.creation_date_time,
                    null,
                    l.gl_receivable_date_time,
                    l.interface_date_time,
                    l.last_update_date_time,
                    l.last_updated_by_name,
                    l.level_element_amt_ent,
                    l.period_start_date,
                    l.print_date_time,
                    l.rule_id,
                    l.rule_start_date_time,
                    l.sequence_nbr,
                    t.tran_code,
                    l.transaction_date_time,
                    l.alt_contract_header_id,
                    l.contract_line_id,
                    l.parent_contract_line_id,
                    l.period_end_date,
                    null  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld    AS l   
                    INNER JOIN CTE AS t
                    ON
                    upper(trim(l.level_element_id)) = upper(trim(t.level_element_id ))
                    AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))   
                    AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) =t.iCat   
                    AND upper(trim(l.contract_line_id)) =t.cl_Id
                    AND upper(trim(l.rule_id)) =t.rl_id"""
        print("Job: 'd8_contract_lvl_element_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.d8t_contract_lvl_element_t1 COMPUTE STATISTICS  FOR COLUMNS level_element_id"""
        print("Job: 'd8_contract_lvl_element_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.d8t_contract_lvl_element_t1 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_lvl_element_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH CTE AS 
        (
        SELECT t.level_element_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
         CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat,
         upper(trim(l.contract_line_id)) AS cl_Id,upper(trim(l.rule_id)) AS rl_id
        from  """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld    AS l
        INNER JOIN 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t1    AS t   
        ON        upper(trim(l.level_element_id)) = upper(trim(t.level_element_id  ))
                    AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
                    AND upper(trim(l.contract_line_id)) <> upper(trim(' '))   
                    AND t.contract_line_id = ''
        )
        INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2           SELECT
                    l.instance_id,
                    l.level_element_id,
                    l.amount_due_date_time,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.completed_date_time,
                    l.created_by_name,
                    l.creation_date_time,
                    null,
                    l.gl_receivable_date_time,
                    l.interface_date_time,
                    l.last_update_date_time,
                    l.last_updated_by_name,
                    l.level_element_amt_ent,
                    l.period_start_date,
                    l.print_date_time,
                    l.rule_id,
                    l.rule_start_date_time,
                    l.sequence_nbr,
                    t.tran_code,
                    l.transaction_date_time,
                    l.alt_contract_header_id,
                    l.contract_line_id,
                    l.parent_contract_line_id,
                    l.period_end_date,
                    null  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld    AS l   
                    INNER JOIN CTE as t 
                    ON 
                   upper(trim( l.level_element_id)) = upper(trim(t.level_element_id  ))
                    AND upper(trim(l.instance_id)) =upper(trim( t.instance_id   ))
                    where  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat
                    AND upper(trim(l.contract_line_id)) = t.rl_id"""
        print("Job: 'd8_contract_lvl_element_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld
                select
                Q1.* 
                From
                """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld Q1 
                Left Join
                    (
                        SELECT level_element_id, instance_id     FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2            
                    )
                    As Q2 
                    On upper(trim(Q1.level_element_id)) = upper(trim(Q2.level_element_id ))
                    And upper(trim(Q1.instance_id))= upper(trim( Q2.instance_id))        
                Where
                Q2.level_element_id Is NULL 
                And Q2.instance_id Is Null """
        print("Job: 'd8_contract_lvl_element_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld           SELECT
                    instance_id,
                    level_element_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    amount_due_date_time,
                    as_of_date_time,
                    batch_nbr,
                    completed_date_time,
                    created_by_name,
                    creation_date_time,
                    gl_receivable_date_time,
                    interface_date_time,
                    last_update_date_time,
                    last_updated_by_name,
                    level_element_amt_ent,
                    period_start_date,
                    print_date_time,
                    rule_id,
                    rule_start_date_time,
                    sequence_nbr,
                    tran_code,
                    transaction_date_time,
                    alt_contract_header_id,
                    contract_line_id,
                    parent_contract_line_id,
                    period_end_date,
                    null  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2"""
        print("Job: 'd8_contract_lvl_element_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_07a'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element SELECT
                    Q1.* 
                FROM """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element Q1  
                LEFT OUTER JOIN
                    """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element  Q2 
                        ON COALESCE( upper(trim(Q1.instance_id)) ,'1' ) = COALESCE( upper(trim(Q2.instance_id)) ,'1' ) 
                    AND COALESCE( upper(trim(Q1.level_element_id)) ,'1' ) = COALESCE( upper(trim(Q2.level_element_id)) ,'1' )          
                WHERE
                    Q2.instance_id IS NULL 
                    AND Q2.level_element_id IS NULL """
        print("Job: 'd8_contract_lvl_element_07a'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element           SELECT
                    instance_id,
                    level_element_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    amount_due_date_time,
                    as_of_date_time,
                    batch_nbr,
                    completed_date_time,
                    created_by_name,
                    creation_date_time,
                    gl_receivable_date_time,
                    interface_date_time,
                    last_update_date_time,
                    last_updated_by_name,
                    level_element_amt_ent,
                    period_start_date,
                    print_date_time,
                    rule_id,
                    rule_start_date_time,
                    sequence_nbr,
                    tran_code,
                    transaction_date_time,
                    alt_contract_header_id,
                    contract_line_id,
                    parent_contract_line_id,
                    period_end_date,
                    null  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_lvl_element_t2"""
        print("Job: 'd8_contract_lvl_element_07a'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractlvlelement01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractlvlelement01inssql").enableHiveSupport().getOrCreate()
    main()
