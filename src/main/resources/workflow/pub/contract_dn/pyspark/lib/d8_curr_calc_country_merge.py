from pyspark import SparkConf, SparkContext, SQLContext 
from pyspark.sql import SparkSession, HiveContext 
import sys 
import logging 
import db_params

class d8currcalccountrymergesql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """use """ + db_params.TTMMPPDDBB1 + """"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
                INTO
                    TABLE
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_as_of_date           
                    SELECT
                        '99',
                        instance_id,
                        MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml      
                    GROUP BY
                        '99' ,
                        instance_id
                UNION ALL 
                    SELECT
                        '99',
                        instance_id,
                        MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_cover_timezone_ml      
                    GROUP BY
                        '99' ,
                        instance_id
                UNION ALL
                    SELECT
                        '99',
                        instance_id,
                        MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_coverage_time_ml      
                    GROUP BY
                        '99' ,
                        instance_id
                UNION ALL 
                    SELECT
                        '99',
                        instance_id,
                        MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_ml      
                    GROUP BY
                        '99' ,
                        instance_id
                UNION ALL
                    SELECT
                        '99',
                        instance_id,
                        MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_action_time_ml      
                    GROUP BY
                        '99' ,
                        instance_id
                UNION ALL
                    SELECT
                        '99',
                        instance_id,
                        MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_ml      
                    GROUP BY
                        '99' ,
                        instance_id
                UNION ALL
                    SELECT
                        '99',
                        instance_id,
                        MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_ml      
                    GROUP BY
                        '99' ,
                        instance_id
                UNION ALL
                    SELECT
                        author_oper_unit_country_code,
                        instance_id,
                        MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t4      
                    GROUP BY
                        author_oper_unit_country_code ,
                        instance_id"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """
            WITH qq1 AS (    
                SELECT distinct
                    instance_id,
                    batch_nbr  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml
                UNION ALL
                SELECT distinct
                    instance_id,
                    batch_nbr  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_action_time_ml
                UNION ALL
                SELECT distinct
                    instance_id,
                    batch_nbr  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_ml
                UNION ALL
                SELECT distinct 
                    instance_id,
                    batch_nbr  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_ml 
                UNION ALL
                SELECT distinct 
                    instance_id,
                    batch_nbr  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_cover_timezone_ml
                UNION ALL
                SELECT distinct
                    instance_id,
                    batch_nbr  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_coverage_time_ml 
                UNION ALL
                SELECT distinct
                    instance_id,
                    batch_nbr  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_stream_ml 
                        ) INSERT 
                    INTO
                        TABLE
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_batch_nbr_trig       
                        SELECT
                        DISTINCT qq1.* 
                        FROM
                            qq1
                LEFT OUTER JOIN
                    (SELECT
                        instance_id AS auto_c00,
                        batch_nbr AS auto_c01  
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_batch_nbr_trig ) AS autoAlias_7 
                        ON ( upper(trim(qq1.instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                        AND qq1.batch_nbr = autoAlias_7.AUTO_C01 ) 
                WHERE
                    autoAlias_7.AUTO_C00 IS  NULL  
                    AND autoAlias_7.AUTO_C01 IS  NULL"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    use """ + db_params.EEDDWWDDBB + """"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB + """.cfs_curr_calc_country"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """      WITH qq1 AS (    SELECT distinct
                    EDW1.acc_date,
                    EDW1.translate_from_curr_code,
                    EDW1.translate_to_curr_code,
                    CASE WHEN EDW3.currency_code IS NOT NULL AND trim(EDW3.data_source_code) = 'COS' THEN EDW3.country_code ELSE EDW2.country_code END AS country_code,
                    CASE WHEN EDW3.currency_code IS NOT NULL AND trim(EDW3.data_source_code) = 'COS' THEN EDW3.currency_code ELSE '    ' END  AS currency_code,
                    EDW1.us_average_rate,
                    EDW1.us_end_of_period_rate,
                    EDW1.local_average_rate,
                    EDW1.local_end_of_period_rate,
                    EDW1.euro_average_rate,
                    EDW1.euro_end_of_period_rate  
                FROM
                    """ + db_params.EEDDWWDDBB + """.curr_calc    AS EDW1   ,
                   (SELECT author_oper_unit_country_code AS  country_code
                    FROM """ + db_params.EEDDWWDDBB1 + """.contract_header_ld      
                    GROUP BY author_oper_unit_country_code)   AS EDW2 
                LEFT OUTER JOIN """ + db_params.EEDDWWDDBB + """.currency_xref    AS EDW3   
                            ON trim(EDW1.translate_to_curr_code) = trim(EDW3.currency_code)
                WHERE
                    upper(trim(EDW1.translate_from_curr_code)) = upper(trim(EDW1.translate_to_curr_code))  
                    AND upper(trim(EDW1.translate_to_curr_code)) <> 'EUR') INSERT 
                    INTO
                        TABLE
                        """ + db_params.EEDDWWDDBB + """.cfs_curr_calc_country       SELECT
                            * 
                        FROM
                            qq1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB + """.cfs_curr_calc_country SELECT distinct
                    EDW1.acc_date AS acc_date ,
                    EDW1.translate_from_curr_code AS translate_from_curr_code ,
                    EDW1.translate_to_curr_code AS translate_to_curr_code ,
                    EDW1.country_code AS country_code ,
                    COALESCE(EDW4.currency_code,
                    EDW1.func_currency_code) AS func_currency_code ,
                    EDW1.us_average_rate AS us_average_rate ,
                    EDW1.us_end_of_period_rate AS us_end_of_period_rate ,
                    COALESCE((EDW4.average_rate * EDW1.us_average_rate),
                    EDW1.local_average_rate) AS local_average_rate ,
                    COALESCE((EDW4.end_of_period_rate * EDW1.us_end_of_period_rate),
                    EDW1.local_end_of_period_rate) AS local_end_of_period_rate ,
                    EDW1.euro_average_rate AS euro_average_rate ,
                    EDW1.euro_end_of_period_rate AS euro_end_of_period_rate 
                FROM
                    """ + db_params.EEDDWWDDBB + """.cfs_curr_calc_country AS EDW1 
                LEFT JOIN
                    (
                        SELECT distinct
                            EDW2.average_rate,
                            EDW2.end_of_period_rate,
                            EDW3.currency_code,
                            EDW3.country_code 
                        FROM
                            """ + db_params.EEDDWWDDBB + """.translation_rates AS EDW2 
                        INNER JOIN
                            """ + db_params.EEDDWWDDBB + """.currency_xref AS EDW3 
                                ON upper(trim(EDW3.data_source_code)) = upper(trim('COS')) 
                                AND upper(trim(EDW3.currency_code )) = upper(trim(EDW2.translate_to_curr_code)) 
                                AND upper(trim(EDW2.translate_to_curr_code)) <> upper(trim('USD')) 
                                AND upper(trim(EDW2.translate_from_curr_code)) = upper(trim('USD')) 
                        ) AS EDW4 
                            ON upper(trim(EDW4.country_code )) = upper(trim(EDW1.country_code )) 
                    WHERE
                        upper(trim(EDW1.translate_from_curr_code )) = upper(trim(EDW1.translate_to_curr_code )) 
                        AND upper(trim(EDW4.currency_code )) <> upper(trim(EDW1.translate_to_curr_code )) 
                        AND EDW1.local_average_rate = 1 
                        AND EDW1.local_end_of_period_rate = 1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB + """.cfs_curr_calc_country SELECT distinct
                    EDW1.acc_date AS acc_date ,
                    EDW1.translate_from_curr_code AS translate_from_curr_code ,
                    EDW1.translate_to_curr_code AS translate_to_curr_code ,
                    EDW1.country_code AS country_code ,
                    COALESCE( EDW3.currency_code ,
                    EDW1.func_currency_code ) AS func_currency_code ,
                    EDW1.us_average_rate AS us_average_rate ,
                    EDW1.us_end_of_period_rate AS us_end_of_period_rate ,
                    COALESCE( EDW2.average_rate ,
                    EDW1.local_average_rate ) AS local_average_rate ,
                    COALESCE( EDW2.end_of_period_rate ,
                    EDW1.local_end_of_period_rate ) AS local_end_of_period_rate ,
                    EDW1.euro_average_rate AS euro_average_rate ,
                    EDW1.euro_end_of_period_rate AS euro_end_of_period_rate  
                FROM
                    """ + db_params.EEDDWWDDBB + """.translation_rates AS EDW2  ,
                    """ + db_params.EEDDWWDDBB + """.currency_xref AS EDW3  
                RIGHT OUTER JOIN
                    """ + db_params.EEDDWWDDBB + """.cfs_curr_calc_country AS EDW1 
                        ON upper(trim(EDW3.data_source_code)) = upper(trim('COS'))  
                        AND upper(trim(EDW3.country_code)) = upper(trim(EDW1.country_code))   
                        AND upper(trim(EDW3.currency_code)) = upper(trim(EDW2.translate_to_curr_code))   
                        AND upper(trim(EDW2.translate_to_curr_code)) = upper(trim('USD'))   
                        AND upper(trim(EDW2.translate_from_curr_code)) <> upper(trim('USD'))   
                        AND upper(trim(EDW1.translate_from_curr_code)) <> upper(trim('USD'))   
                        AND upper(trim(EDW1.translate_from_curr_code)) = upper(trim(EDW1.translate_to_curr_code))   
                        AND upper(trim(EDW2.translate_from_curr_code)) = upper(trim(EDW1.translate_to_curr_code))   
                        AND EDW1.local_average_rate = 1   
                        AND EDW1.local_end_of_period_rate = 1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)


def main():
    m = d8currcalccountrymergesql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8currcalccountrymergesql").enableHiveSupport().getOrCreate()
    main()
