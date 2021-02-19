from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractlinexref01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_xref_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t1"""
        print("Job: 'd8_contract_line_xref_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   with qq1 as (
         SELECT
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
                        upg_orig_sys_ref_name, 
                        ROW_NUMBER() over (PARTITION BY contract_line_xref_id , instance_id 
                            ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) , tran_code , batch_nbr) DESC) As RNK 
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_ml 
                    WHERE
                        upper(trim(tran_code)) <> upper(trim('D')) ) 
        INSERT INTO  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t1 
        SELECT
                    EDW2.contract_line_xref_id,
                    EDW2.instance_id,
                    EDW2.alt_contract_header_id,
                    EDW2.as_of_date_time,
                    EDW2.batch_nbr,
                    EDW2.contract_header_id,
                    EDW2.contract_line_id,
                    EDW2.created_by_name,
                    EDW2.creation_date_time,
                    EDW2.exception_flag,
                    EDW2.for_contract_line_id,
                    EDW2.last_update_date_time,
                    EDW2.last_update_login_name,
                    EDW2.last_updated_by_name,
                    EDW2.nbr_of_items,
                    EDW2.object1_code,
                    EDW2.object1_id1,
                    EDW2.object1_id2,
                    EDW2.priced_item_flag,
                    EDW2.tran_code,
                    EDW2.uom_code,
                    EDW2.upg_orig_sys_ref_id,
                    EDW2.upg_orig_sys_ref_name
        FROM     """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_ml
         EDW2 
          INNER JOIN (
            SELECT 
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
                    upg_orig_sys_ref_name, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND EDW2.contract_line_xref_id =IBVL.contract_line_xref_id 
           AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_line_xref_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_xref_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2"""
        print("Job: 'd8_contract_line_xref_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2           SELECT
                    contract_line_xref_id,
                    instance_id,
                    alt_contract_header_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    'Y',
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
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_xref_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t1 
                    ) AS autoAlias_183 
                        ON (
                            upper(trim(contract_line_xref_id)) = upper(trim(autoAlias_183.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_183.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_line_xref_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t3"""
        print("Job: 'd8_contract_line_xref_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t3           SELECT
                    contract_line_xref_id,
                    instance_id,
                    alt_contract_header_id,
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
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_xref_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t1 
                    ) AS autoAlias_185 
                        ON (
                            upper(trim(contract_line_xref_id)) = upper(trim(autoAlias_185.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_185.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_line_xref_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_xref_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t3           SELECT
                    contract_line_xref_id,
                    instance_id,
                    alt_contract_header_id,
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
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_xref_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2 
                    ) AS autoAlias_187 
                        ON (
                            upper(trim(contract_line_xref_id)) = upper(trim(autoAlias_187.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_187.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_line_xref_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_xref_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2 SELECT
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.contract_line_xref_id AS contract_line_xref_id ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.alt_contract_header_id AS alt_contract_header_id ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.contract_header_id AS contract_header_id ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.contract_line_id AS contract_line_id ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_189.auto_c00 IS not null and autoAlias_189.auto_c01 IS not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_contract_line_xref_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.exception_flag AS exception_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.for_contract_line_id AS for_contract_line_id ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.nbr_of_items AS nbr_of_items ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.object1_code AS object1_code ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.object1_id1 AS object1_id1 ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.object1_id2 AS object1_id2 ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.priced_item_flag AS priced_item_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.tran_code AS tran_code ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.uom_code AS uom_code ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.upg_orig_sys_ref_id AS upg_orig_sys_ref_id ,
                    AAPPLLIIDD1EENNVV_contract_line_xref_t2.upg_orig_sys_ref_name AS upg_orig_sys_ref_name 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2 AS AAPPLLIIDD1EENNVV_contract_line_xref_t2 
                Left Outer Join
                    (
                        SELECT
                            contract_line_xref_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t3      
                        GROUP BY
                            contract_line_xref_id,
                            instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_189 
                        ON (
                            trim(upper(autoAlias_189.auto_c00)) = trim(upper(AAPPLLIIDD1EENNVV_contract_line_xref_t2.contract_line_xref_id)) 
                            AND trim(upper(autoAlias_189.auto_c01)) = trim(upper(AAPPLLIIDD1EENNVV_contract_line_xref_t2.instance_id))
                        )"""
        print("Job: 'd8_contract_line_xref_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_xref_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld   
        select
           Q1.* 
        From
           """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld Q1 
           Left Join
              (
                 select
                    A.contract_line_xref_id,
                    A.instance_id
                 From
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t1   As A 
                    Left Join
                       (
                          SELECT
                             contract_line_xref_id,
                             instance_id 
                          FROM
                             """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2  
                          WHERE
                             current_record_ind = 'D' 
                       )
                       As B 
                       On trim(upper(A.contract_line_xref_id)) = trim(upper(b.contract_line_xref_id)) 
                       And trim(upper(A.instance_id)) = trim(upper(B.instance_id))               
                 Where
                    B.contract_line_xref_id Is NULL 
                    And B.instance_id Is Null            
              )
              As Q2 
              On trim(upper(Q1.contract_line_xref_id)) = trim(upper(Q2.contract_line_xref_id)) 
              And trim(upper(Q1.instance_id)) = trim(upper(Q2.instance_id))        
        Where
           Q2.contract_line_xref_id Is NULL 
           And Q2.instance_id Is Null """
        print("Job: 'd8_contract_line_xref_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld           SELECT
                    contract_line_xref_id,
                    instance_id,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
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
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_xref_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_193 
                        ON (
                            upper(trim(contract_line_xref_id)) = upper(trim(autoAlias_193.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_193.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_193.AUTO_C00 IS  NULL  
                    AND autoAlias_193.AUTO_C01 IS  NULL"""
        print("Job: 'd8_contract_line_xref_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_xref_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t1"""
        print("Job: 'd8_contract_line_xref_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   with qq1 as (
         SELECT
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
                        upg_orig_sys_ref_name, 
                        ROW_NUMBER() over (PARTITION BY contract_line_xref_id , instance_id 
                            ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) ,  batch_nbr) DESC) As RNK 
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_ml 
                    WHERE
                        upper(trim(tran_code)) = upper(trim('D')) ) 
        INSERT INTO  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t1 
        SELECT
                    EDW2.contract_line_xref_id,
                    EDW2.instance_id,
                    EDW2.alt_contract_header_id,
                    EDW2.as_of_date_time,
                    EDW2.batch_nbr,
                    EDW2.contract_header_id,
                    EDW2.contract_line_id,
                    EDW2.created_by_name,
                    EDW2.creation_date_time,
                    EDW2.exception_flag,
                    EDW2.for_contract_line_id,
                    EDW2.last_update_date_time,
                    EDW2.last_update_login_name,
                    EDW2.last_updated_by_name,
                    EDW2.nbr_of_items,
                    EDW2.object1_code,
                    EDW2.object1_id1,
                    EDW2.object1_id2,
                    EDW2.priced_item_flag,
                    EDW2.tran_code,
                    EDW2.uom_code,
                    EDW2.upg_orig_sys_ref_id,
                    EDW2.upg_orig_sys_ref_name
        FROM     """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_ml
         EDW2 
          INNER JOIN (
            SELECT 
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
                    upg_orig_sys_ref_name, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND EDW2.contract_line_xref_id =IBVL.contract_line_xref_id 
           AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_line_xref_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2"""
        print("Job: 'd8_contract_line_xref_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH CTE AS 
        (
        SELECT t.contract_line_xref_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
         CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat
        from  """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld    AS l
        INNER JOIN 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t1    AS t   
        ON        trim(upper(l.contract_line_xref_id)) = trim(upper(t.contract_line_xref_id))  
                    AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
        ) 
        INSERT INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2
        SELECT
                    l.contract_line_xref_id,
                    l.instance_id,
                    l.alt_contract_header_id,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.contract_header_id,
                    l.contract_line_id,
                    l.created_by_name,
                    l.creation_date_time,
                    null,
                    l.exception_flag,
                    l.for_contract_line_id,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    l.nbr_of_items,
                    l.object1_code,
                    l.object1_id1,
                    l.object1_id2,
                    l.priced_item_flag,
                    t.tran_code,
                    l.uom_code,
                    l.upg_orig_sys_ref_id,
                    l.upg_orig_sys_ref_name  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld    AS l   
                    INNER JOIN CTE as t
                    ON
                    trim(upper(l.contract_line_xref_id)) = trim(upper(t.contract_line_xref_id))  
                    AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))   
                    AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat"""
        print("Job: 'd8_contract_line_xref_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_xref_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                    TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld   
            select
            Q1.* 
            From
            """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld Q1 
            Left Join
                (
                    SELECT contract_line_xref_id, instance_id  FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2         
                )
                As Q2 
                On trim(upper(Q1.contract_line_xref_id)) = trim(upper(Q2.contract_line_xref_id)) 
                And trim(upper(Q1.instance_id)) = trim(upper(Q2.instance_id))
            Where
            Q2.contract_line_xref_id Is NULL 
            And Q2.instance_id Is Null """
        print("Job: 'd8_contract_line_xref_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld           SELECT
                    contract_line_xref_id,
                    instance_id,
                    alt_contract_header_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
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
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_xref_t2"""
        print("Job: 'd8_contract_line_xref_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractlinexref01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractlinexref01inssql").enableHiveSupport().getOrCreate()
    main()
