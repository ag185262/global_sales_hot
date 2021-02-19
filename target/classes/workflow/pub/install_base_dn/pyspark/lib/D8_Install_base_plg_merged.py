from pyspark.sql import SparkSession 
import sys 
from datetime import datetime
import db_params

class D8Installbaseplgmergedpy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB + """"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t01"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t02"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t02           SELECT
                    EDW1.instance_id,
                    EDW1.inventory_item_id,
                    EDW1.inventory_source_code,
                    EDW1.category_id,
                    EDW1.category_set_id,
                    EDW1.source_country_code,
                    EDW2.item_category_code,
                    EDW2.item_category_desc,
                    COALESCE( EDW3.product_line_nbr ,
                    0 ) AS auto_c08,
                    COALESCE( EDW3.corp_product_type_code ,
                    ' ' ) AS auto_c09,
                    CASE 
                        WHEN upper(trim(EDW1.tran_code)) = upper(trim('D'))  
                        OR upper(trim(EDW2.tran_code)) = upper(trim('D'))   THEN 'D'  
                        ELSE 'U'  
                    END AS best_fit_tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.mtl_item_category_xref    AS EDW1   ,
                    """ + db_params.EEDDWWDDBB2 + """.mtl_item_category    AS EDW2
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.fml_product_service_xref    AS EDW3    
                     ON EDW2.item_category_code = EDW3.corp_fml_prod_service_code        
                WHERE
                    EDW1.instance_id = EDW2.instance_id  
                    AND EDW1.category_id = EDW2.category_id   
                    AND EDW1.category_set_id = 6"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t03"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t03           SELECT
                    EDW1.product_line_nbr,
                    EDW1.product_group_nbr,
                    EDW1.prod_line_cat_code,
                    EDW1.product_line_name,
                    EDW2.product_group_name  
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.corporate_product_line    AS EDW1   ,
                    """ + db_params.EEDDWWDDBB2 + """.product_group    AS EDW2   
                WHERE
                    EDW1.product_group_nbr = EDW2.product_group_nbr"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t04"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t05"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t05           SELECT
                    EDW1.instance_id,
                    EDW1.inventory_item_id,
                    EDW1.inventory_source_code,
                    EDW1.category_id,
                    EDW1.category_set_id,
                    EDW1.source_country_code,
                    EDW1.item_category_code,
                    EDW1.item_category_desc,
                    EDW1.product_line_nbr,
                    EDW1.corp_product_type_code,
                    COALESCE( EDW3.product_type_name ,
                    ' ' ) AS auto_c010,
                    EDW2.product_group_nbr,
                    EDW2.prod_line_cat_code,
                    EDW2.product_line_name,
                    EDW2.product_group_name,
                    EDW1.best_fit_tran_code  
                FROM """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t02    AS EDW1   ,
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t03    AS EDW2   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.product_type    AS EDW3    
                        ON EDW1.corp_product_type_code = EDW3.product_type_code
                WHERE
                    EDW1.product_line_nbr = EDW2.product_line_nbr"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t06"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t06           SELECT
                    EDW1.instance_id,
                    EDW1.inventory_item_id,
                    EDW1.inventory_source_code,
                    EDW1.category_id,
                    EDW1.category_set_id,
                    EDW1.source_country_code,
                    EDW2.item_category_code,
                    EDW2.item_category_desc,
                    CASE 
                        WHEN upper(trim(EDW1.tran_code)) = 'D'  
                        OR upper(trim(EDW2.tran_code)) = 'D'   THEN 'D'  
                        ELSE 'U'  
                    END AS auto_c08  
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.mtl_item_category_xref    AS EDW1   ,
                    """ + db_params.EEDDWWDDBB2 + """.mtl_item_category    AS EDW2   
                WHERE
                    EDW1.instance_id = EDW2.instance_id 
                    AND EDW1.category_id = EDW2.category_id   
                    AND EDW1.category_set_id = 7"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t07_b"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
         TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t07"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t07           SELECT
                    EDW1.instance_id,
                    EDW1.inventory_item_id,
                    EDW1.inventory_source_code,
                    EDW1.category_id,
                    EDW1.category_set_id,
                    EDW1.source_country_code,
                    EDW1.item_category_code,
                    EDW1.item_category_desc,
                    EDW1.product_line_nbr,
                    EDW1.corp_product_type_code,
                    EDW1.product_type_name,
                    EDW1.product_group_nbr,
                    EDW1.prod_line_cat_code,
                    EDW1.product_line_name,
                    EDW1.product_group_name,
                    EDW1.best_fit_tran_code,
                    COALESCE( EDW2.item_category_code ,
                    ' ' ) AS auto_c016,
                    COALESCE( EDW2.item_category_desc ,
                    ' ' ) AS auto_c017,
                    COALESCE( EDW2.best_fit_tran_code ,
                    ' ' ) AS auto_c018  
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t05    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t06    AS EDW2    
                        ON EDW1.instance_id = EDW2.instance_id  
                        AND EDW1.inventory_item_id = EDW2.inventory_item_id   
                        AND EDW1.inventory_source_code = EDW2.inventory_source_code"""
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ INSERT 
                INTO
                    TABLE
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t07   
           SELECT
                EDW1.instance_id,
                EDW1.inventory_item_id,
                EDW1.inventory_source_code,
                0 AS auto_c03,
                0 AS auto_c04,
                ' ' AS auto_c05,
                ' ' AS auto_c06,
                ' ' AS auto_c07,
                0 AS auto_c08,
                ' ' AS auto_c09,
                ' ' AS auto_c010,
                0 AS auto_c011,
                ' ' AS auto_c012,
                ' ' AS auto_c013,
                ' ' AS auto_c014,
                ' ' AS auto_c015,
                EDW1.item_category_code,
                EDW1.item_category_desc,
                EDW1.best_fit_tran_code  
            FROM
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t06    AS EDW1 
            LEFT OUTER JOIN
                 """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t07  AS autoAlias_88 
                    ON ( EDW1.instance_id = autoAlias_88.instance_id 
                    AND EDW1.inventory_item_id = autoAlias_88.inventory_item_id 
                    AND EDW1.inventory_source_code = autoAlias_88.inventory_source_code ) 
            WHERE
                autoAlias_88.instance_id IS  NULL  
                AND autoAlias_88.inventory_item_id IS  NULL  
                AND autoAlias_88.inventory_source_code IS  NULL    """
        print("Job 'D8_Install_base_plg_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = D8Installbaseplgmergedpy(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print(e)
        print("Error in D8Installbaseplgmergedpy")
        sparkSession.stop()
        sys.exit(1)


if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8Installbaseplgmergedpy").enableHiveSupport().getOrCreate()
    main()
    sparkSession.stop()
    sys.exit(0)
