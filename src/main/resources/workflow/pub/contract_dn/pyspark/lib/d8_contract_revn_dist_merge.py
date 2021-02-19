from pyspark import SparkConf, SparkContext, SQLContext 
from pyspark.sql import SparkSession, HiveContext 
import sys 
import logging 
import db_params

class d8contractrevndistmergesql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = """    use """ + db_params.TTMMPPDDBB1 + """"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1           SELECT 
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    ( SELECT
                        m1.*,
                        ROW_NUMBER() OVER(PARTITION 
                    BY
                        instance_id ,
                        revenue_dist_id 
                    ORDER BY
                        CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                        batch_nbr) DESC) AS r00 
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_ml m1 
                    WHERE
                        upper(trim(tran_code)) <> trim('D')) rcrd 
                WHERE
                    r00=1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """CACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2           SELECT distinct
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_revn_dist_ld 
                INNER JOIN
                    (
                        SELECT
                            instance_id AS auto_c00,
                            revenue_dist_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1 
                    ) AS autoAlias_47 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_47.AUTO_C00)) 
                            AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_47.AUTO_C01)) 
                        )"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """CACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t3"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t3           SELECT 
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2 
                INNER JOIN
                    (
                        SELECT
                             instance_id AS auto_c00,
                            revenue_dist_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1 
                    ) AS autoAlias_49 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_49.AUTO_C00)) 
                            AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_49.AUTO_C01)) 
                        )"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """CACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t3 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t3           SELECT  
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1 
                INNER JOIN
                    (
                        SELECT
                             instance_id AS auto_c00,
                            revenue_dist_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2 
                    ) AS autoAlias_785 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_785.AUTO_C00)) 
                            AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_785.AUTO_C01)) 
                        )"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """
        INSERT OVERWRITE TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t3           SELECT  
                DISTINCT * FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t3"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2 SELECT 
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    (CASE 
                        WHEN(RCRD_UPD=1) THEN 'D' 
                        ELSE current_record_ind 
                    END) AS current_record_ind,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2 
                LEFT OUTER JOIN
                    (
                        SELECT
                            instance_id AS auto_c00,
                            revenue_dist_id AS auto_c01,
                            COUNT(*) AS RCRD_UPD  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2 
                        GROUP BY
                            1,
                            2
                    ) AS autoAlias_7 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                            AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_7.AUTO_C01)) 
                        )"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    use """ + db_params.EEDDWWDDBB1 + """"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                 TABLE """ + db_params.EEDDWWDDBB1 + """.contract_revn_dist_ld 
                 SELECT distinct Q1.* FROM  """ + db_params.EEDDWWDDBB1 + """.contract_revn_dist_ld AS Q1
                 LEFT OUTER JOIN 
                        (
                        SELECT revenue_dist_id, instance_id
                        FROM
                         """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1 AS Q2  
                        LEFT OUTER JOIN 
                            (   SELECT revenue_dist_id AS autoc000 , instance_id as autoc001
                            FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2 
                                WHERE current_record_ind = 'D' ) AS Q3 
                        ON Q2.revenue_dist_id = Q3.autoc000
                        AND Q2.instance_id = Q3.autoc001
                            WHERE Q3.autoc000 IS NULL 
                            AND Q3.autoc001 IS NULL
                        ) AS Q23
                 ON Q1.revenue_dist_id = Q23.revenue_dist_id
                 AND Q1.instance_id = Q23.instance_id
                    WHERE Q23.revenue_dist_id IS NULL 
                    AND Q23.instance_id IS NULL"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_revn_dist_ld           SELECT distinct
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    as_of_date_time,
                    batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT instance_id AS auto_c00,
                            revenue_dist_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_55 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_55.AUTO_C00)) 
                            AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_55.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_55.AUTO_C00 IS  NULL  
                    AND autoAlias_55.AUTO_C01 IS  NULL"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1           SELECT distinct
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    ( SELECT
                        m1.*,
                        ROW_NUMBER() OVER(PARTITION 
                    BY
                        instance_id ,
                        revenue_dist_id 
                    ORDER BY
                        CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                        batch_nbr) DESC) AS r00 
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_ml m1 
                    WHERE
                        upper(trim(tran_code)) = trim('D')) rcrd 
                WHERE
                    r00=1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """CACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """
            INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2           SELECT distinct
                    l.instance_id,
                    l.revenue_dist_id,
                    l.account_class_code,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.contract_header_id,
                    l.contract_line_id,
                    l.created_by_name,
                    l.creation_date_time,
                    null,
                    l.distribution_pct,
                    l.gl_code_combination_id,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    t.tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_revn_dist_ld    AS l 
                INNER JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t1    AS t   
                        ON l.instance_id = t.instance_id  
                        AND l.revenue_dist_id = t.revenue_dist_id 
                WHERE
                    CONCAT(CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) < CONCAT(CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """CACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_revn_dist_ld SELECT distinct
                    revn_ld.* 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_revn_dist_ld revn_ld 
                LEFT JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2 revn_t2 
                        on revn_ld.instance_id = revn_t2.instance_id 
                        AND revn_ld.revenue_dist_id = revn_t2.revenue_dist_id 
                WHERE
                    revn_t2.revenue_dist_id IS NULL 
                    AND revn_t2.instance_id IS NULL"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_revn_dist_ld SELECT distinct
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_revn_dist_t2"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_revn_dist SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contract_revn_dist_ld"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)


def main():
    m = d8contractrevndistmergesql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractrevndistmergesql").enableHiveSupport().getOrCreate()
    main()
