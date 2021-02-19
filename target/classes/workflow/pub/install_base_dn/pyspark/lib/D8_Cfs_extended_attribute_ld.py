from pyspark.sql import SparkSession 
import sys 
from datetime import datetime
import db_params

class D8Cfsextendedattributeldpy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """"""
        print("Job 'D8_Cfs_extended_attribute_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB1 + """.cfs_extended_attribute_ld"""
        print("Job 'D8_Cfs_extended_attribute_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.cfs_extended_attribute_ld           SELECT
                  A.*  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.cfs_extended_attribute A"""
        print("Job 'D8_Cfs_extended_attribute_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Cfs_extended_attribute_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_extended_attribute_t1"""
        print("Job 'D8_Cfs_extended_attribute_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_extended_attribute_t1           SELECT
                    a.attribute_id,
                    a.instance_id,
                    a.active_end_date_time,
                    a.active_start_date_time,
                    a.as_of_date_time,
                    a.attribute_category_code,
                    a.attribute_code_name,
                    a.attribute_desc,
                    a.attribute_level_name,
                    a.attribute_name,
                    a.batch_nbr,
                    a.created_by_name,
                    a.creation_date_time,
                    a.inventory_org_id,
                    a.inventory_item_id,
                    a.item_category_id,
                    a.last_update_date_time,
                    a.last_update_login_name,
                    a.last_updated_by_name,
                    a.tran_code  
                FROM (
                           SELECT
                                m1.*,
                                ROW_NUMBER() OVER(PARTITION 
                            BY
                                attribute_id ,
                                instance_id 
                            ORDER BY
                                CAST (as_of_date_time AS CHAR(26)) DESC) AS r00 
                            FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_extended_attribute_ml  m1    
                    ) AS a 
                       WHERE
                            r00=1"""
        print("Job 'D8_Cfs_extended_attribute_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.cfs_extended_attribute_ld SELECT
                    a.* 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.cfs_extended_attribute_ld    AS a  
                left outer join
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_extended_attribute_t1 b 
                        on   (
                            a.attribute_id = b.attribute_id  
                            AND a.instance_id = b.instance_id  
                        )  
                where
                    b.attribute_id IS NULL   
                    and  b.instance_id IS NULL"""
        print("Job 'D8_Cfs_extended_attribute_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.cfs_extended_attribute_ld           SELECT
                    attribute_id,
                    instance_id,
                    active_end_date_time,
                    active_start_date_time,
                    as_of_date_time,
                    attribute_category_code,
                    attribute_code_name,
                    attribute_desc,
                    attribute_level_name,
                    attribute_name,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    inventory_org_id,
                    inventory_item_id,
                    item_category_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_extended_attribute_t1     
                WHERE
                    upper(trim(tran_code)) <> trim('D')"""
        print("Job 'D8_Cfs_extended_attribute_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = D8Cfsextendedattributeldpy(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print(e)
        print("Error in D8Cfsextendedattributeldpy")
        sparkSession.stop()
        sys.exit(1)

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8Cfsextendedattributeldpy").enableHiveSupport().getOrCreate()
    main()