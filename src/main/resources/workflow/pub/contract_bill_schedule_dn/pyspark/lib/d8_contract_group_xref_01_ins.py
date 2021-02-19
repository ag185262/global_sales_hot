from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractgroupxref01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_group_xref_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t1"""
        print("Job: 'd8_contract_group_xref_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """with qq1 as (
        SELECT
                            contract_group_xref_id,
                            instance_id,
                            as_of_date_time,
                            batch_nbr,
                            created_by_name,
                            creation_date_time,
                            included_contract_group_id,
                            included_contract_header_id,
                            last_update_date_time,
                            last_update_login_name,
                            last_updated_by_name,
                            parent_contract_group_id,
                            subclass_code,
                            tran_code,
                            ROW_NUMBER() OVER(PARTITION BY contract_group_xref_id,instance_id ORDER BY 
                            (CONCAT (CAST (as_of_date_time AS CHAR(26)),batch_nbr)) DESC) AS RNK
                            FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_ml              WHERE
                             upper(trim(tran_code)) <> upper(trim('D')) ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t1 
        SELECT
                    EDW2.contract_group_xref_id,
                    EDW2.instance_id,
                    EDW2.as_of_date_time,
                    EDW2.batch_nbr,
                    EDW2.created_by_name,
                    EDW2.creation_date_time,
                    EDW2.included_contract_group_id,
                    EDW2.included_contract_header_id,
                    EDW2.last_update_date_time,
                    EDW2.last_update_login_name,
                    EDW2.last_updated_by_name,
                    EDW2.parent_contract_group_id,
                    EDW2.subclass_code,
                    EDW2.tran_code FROM  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_ml EDW2 
          INNER JOIN (
            SELECT 
               contract_group_xref_id,
                            instance_id,
                            as_of_date_time,
                            batch_nbr,
                            created_by_name,
                            creation_date_time,
                            included_contract_group_id,
                            included_contract_header_id,
                            last_update_date_time,
                            last_update_login_name,
                            last_updated_by_name,
                            parent_contract_group_id,
                            subclass_code,
                            tran_code, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.contract_group_xref_id)) = upper(trim(IBVL.contract_group_xref_id)) 
          AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_group_xref_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_group_xref_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2"""
        print("Job: 'd8_contract_group_xref_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2           SELECT
                    contract_group_xref_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    included_contract_group_id,
                    included_contract_header_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    parent_contract_group_id,
                    subclass_code,
                    tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_group_xref_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t1 
                    ) AS autoAlias_61 
                        ON (
                            upper(trim(contract_group_xref_id)) = upper(trim(autoAlias_61.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_61.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_group_xref_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t3"""
        print("Job: 'd8_contract_group_xref_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t3           SELECT
                    contract_group_xref_id,
                    instance_id,
                    created_by_name,
                    creation_date_time,
                    included_contract_group_id,
                    included_contract_header_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    parent_contract_group_id,
                    subclass_code,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_group_xref_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t1 
                    ) AS autoAlias_63 
                        ON (
                            upper(trim(contract_group_xref_id)) = upper(trim(autoAlias_63.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_63.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_group_xref_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_group_xref_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t3           SELECT
                    contract_group_xref_id,
                    instance_id,
                    created_by_name,
                    creation_date_time,
                    included_contract_group_id,
                    included_contract_header_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    parent_contract_group_id,
                    subclass_code,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_group_xref_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2 
                    ) AS autoAlias_65 
                        ON (
                            upper(trim(contract_group_xref_id)) = upper(trim(autoAlias_65.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_65.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_group_xref_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_group_xref_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2 SELECT
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.contract_group_xref_id AS contract_group_xref_id ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_67.auto_c00 IS not null and  autoAlias_67.auto_c01 IS not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_contract_group_xref_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.included_contract_group_id AS included_contract_group_id ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.included_contract_header_id AS included_contract_header_id ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.parent_contract_group_id AS parent_contract_group_id ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.subclass_code AS subclass_code ,
                    AAPPLLIIDD1EENNVV_contract_group_xref_t2.tran_code AS tran_code 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2 AS AAPPLLIIDD1EENNVV_contract_group_xref_t2 
                Left Outer Join
                    (
                        SELECT
                            contract_group_xref_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t3      
                        GROUP BY
                            contract_group_xref_id,
                            instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_67 
                        ON (
                            upper(trim( autoAlias_67.auto_c00)) = upper(trim( AAPPLLIIDD1EENNVV_contract_group_xref_t2.contract_group_xref_id)) 
                            AND upper(trim( autoAlias_67.auto_c01)) = upper(trim( AAPPLLIIDD1EENNVV_contract_group_xref_t2.instance_id))
                        )"""
        print("Job: 'd8_contract_group_xref_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_group_xref_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE
                    TABLE """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld         
                select
                Q1.* 
                From
                """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld  Q1 
                Left Join
                    (
                        select
                            A.contract_group_xref_id,
                            A.instance_id 
                        From
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t1 As A 
                            Left Join
                            (
                                SELECT
                                    contract_group_xref_id,
                                    instance_id 
                                FROM
                                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2
                                WHERE
                                    current_record_ind = 'D' 
                            )
                            As B 
                            On A.contract_group_xref_id = b.contract_group_xref_id 
                            And A.instance_id = B.instance_id 
                        Where
                            B.contract_group_xref_id Is NULL 
                            And B.instance_id Is Null 
                    )
                    As Q2 
                    On upper(trim(Q1.contract_group_xref_id)) = upper(trim(Q2.contract_group_xref_id)) 
                    And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id)) 
                Where
                Q2.contract_group_xref_id Is NULL 
                And Q2.instance_id Is Null """
        print("Job: 'd8_contract_group_xref_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld           SELECT
                    contract_group_xref_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    included_contract_group_id,
                    included_contract_header_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    parent_contract_group_id,
                    subclass_code,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT contract_group_xref_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_71 
                        ON (
                            upper(trim(contract_group_xref_id)) = upper(trim(autoAlias_71.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_71.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_71.AUTO_C00 IS  NULL  
                    AND autoAlias_71.AUTO_C01 IS  NULL"""
        print("Job: 'd8_contract_group_xref_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_group_xref_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t1"""
        print("Job: 'd8_contract_group_xref_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """with qq1 as (
        SELECT
                            contract_group_xref_id,
                            instance_id,
                            as_of_date_time,
                            batch_nbr,
                            created_by_name,
                            creation_date_time,
                            included_contract_group_id,
                            included_contract_header_id,
                            last_update_date_time,
                            last_update_login_name,
                            last_updated_by_name,
                            parent_contract_group_id,
                            subclass_code,
                            tran_code,
                            ROW_NUMBER() OVER(PARTITION BY contract_group_xref_id,instance_id ORDER BY 
                            (CONCAT (CAST (as_of_date_time AS CHAR(26)),batch_nbr)) DESC) AS RNK
                            FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_ml              WHERE
                             upper(trim(tran_code)) = upper(trim('D'))) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t1 
        SELECT
                    edw2.contract_group_xref_id,
                    edw2.instance_id,
                    edw2.as_of_date_time,
                    edw2.batch_nbr,
                    edw2.created_by_name,
                    edw2.creation_date_time,
                    edw2.included_contract_group_id,
                    edw2.included_contract_header_id,
                    edw2.last_update_date_time,
                    edw2.last_update_login_name,
                    edw2.last_updated_by_name,
                    edw2.parent_contract_group_id,
                    edw2.subclass_code,
                    edw2.tran_code 
        FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_ml EDW2 
          INNER JOIN (
            SELECT 
               contract_group_xref_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    included_contract_group_id,
                    included_contract_header_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    parent_contract_group_id,
                    subclass_code,
                    tran_code, 
                    RNK 
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND upper(trim(EDW2.contract_group_xref_id)) =upper(trim(IBVL.contract_group_xref_id)) 
          AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_group_xref_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2"""
        print("Job: 'd8_contract_group_xref_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH CTE AS 
        (
        SELECT t.contract_group_xref_id,t.instance_id, CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) AS iCat,
        t.as_of_date_time,t.batch_nbr,t.tran_code
        FROM """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld    AS l   
        INNER JOIN """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t1    AS t 
        ON l.contract_group_xref_id = t.contract_group_xref_id  
        AND l.instance_id = t.instance_id   
        WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
        )
        INSERT 
        INTO
        TABLE
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2           SELECT
        l.contract_group_xref_id,
        l.instance_id,
        t.as_of_date_time,
        t.batch_nbr,
        l.as_of_date_time,
        l.batch_nbr,
        l.created_by_name,
        l.creation_date_time,
        null,
        l.included_contract_group_id,
        l.included_contract_header_id,
        l.last_update_date_time,
        l.last_update_login_name,
        l.last_updated_by_name,
        l.parent_contract_group_id,
        l.subclass_code,
        t.tran_code  
        FROM
        """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld    AS l   
        INNER JOIN CTE AS t   
        ON upper(trim(l.contract_group_xref_id)) = upper(trim(t.contract_group_xref_id))  
        AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))   
        AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) =  t.iCat"""
        print("Job: 'd8_contract_group_xref_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_group_xref_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE
                    TABLE """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld     
                SELECT
                Q1.* 
                FROM
                """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld AS Q1
                Left Join
                    (
                        SELECT
                            contract_group_xref_id,
                            instance_id 
                        FROM
                             """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2  
                    )
                    As Q2 
                    On upper(trim( Q1.contract_group_xref_id)) = upper(trim( Q2.contract_group_xref_id ))
                    And upper(trim( Q1.instance_id)) = upper(trim( Q2.instance_id)) 
                Where
                Q2.contract_group_xref_id Is NULL 
                And Q2.instance_id Is Null """
        print("Job: 'd8_contract_group_xref_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld           SELECT
                    contract_group_xref_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    included_contract_group_id,
                    included_contract_header_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    parent_contract_group_id,
                    subclass_code,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_group_xref_t2"""
        print("Job: 'd8_contract_group_xref_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractgroupxref01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractgroupxref01inssql").enableHiveSupport().getOrCreate()
    main()
