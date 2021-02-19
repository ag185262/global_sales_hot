from pyspark.sql import SparkSession
from datetime import datetime  
import sys 
import db_params

class D8Cfsattributevaluemergedpy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """"""
        print("Job 'D8_Cfs_attribute_value_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB1 + """.cfs_attribute_value_ld"""
        print("Job 'D8_Cfs_attribute_value_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.cfs_attribute_value_ld           SELECT
                    cfs_attribute_value.*  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.cfs_attribute_value"""
        print("Job 'D8_Cfs_attribute_value_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Cfs_attribute_value_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_attribute_value_t1"""
        print("Job 'D8_Cfs_attribute_value_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_attribute_value_t1           SELECT
                    a.attribute_value_id,
                    a.instance_id,
                    a.active_end_date_time,
                    a.active_start_date_time,
                    a.as_of_date_time,
                    a.attribute_id,
                    a.attribute_value,
                    a.batch_nbr,
                    a.created_by_name,
                    a.creation_date_time,
                    a.item_instance_id,
                    a.last_update_date_time,
                    a.last_update_login_name,
                    a.last_updated_by_name,
                    a.tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_attribute_value_ml    AS a """
        print("Job 'D8_Cfs_attribute_value_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.cfs_attribute_value_ld SELECT
                    a.* 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.cfs_attribute_value_ld    AS a  
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_attribute_value_t1 b 
                        on    a.attribute_value_id = b.attribute_value_id  
                        AND a.instance_id = b.instance_id    
                where
                    b.attribute_value_id IS   NULL  
                    and  b.instance_id IS  NULL"""
        print("Job 'D8_Cfs_attribute_value_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.cfs_attribute_value_ld           SELECT
                    attribute_value_id,
                    instance_id,
                    active_end_date_time,
                    active_start_date_time,
                    as_of_date_time,
                    attribute_id,
                    attribute_value,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    item_instance_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_attribute_value_t1     
                WHERE
                    upper(trim(tran_code)) <> upper(trim('D'))"""
        print("Job 'D8_Cfs_attribute_value_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = D8Cfsattributevaluemergedpy(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print(e)
        print("Error in D8Cfsattributevaluemergedpy")
        sparkSession.stop()
        sys.exit(1)

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8Cfsattributevaluemergedpy").enableHiveSupport().getOrCreate()
    main()
    sparkSession.stop()
    sys.exit(0)
