from pyspark.sql import SparkSession 
import sys 
from datetime import datetime
import db_params

class D8Esslmclassmergedpy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB2 + """"""
        print("Job 'D8_Es_slm_class_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE  """ + db_params.EEDDWWDDBB2 + """.es_slm_class_ld"""
        print("Job 'D8_Es_slm_class_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB2 + """.es_slm_class_ld          SELECT
                    A.*  
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.es_slm_class A"""
        print("Job 'D8_Es_slm_class_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_es_slm_class_t1"""
        print("Job 'D8_Es_slm_class_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_es_slm_class_t1           SELECT
                    EDW1.instance_id,
                    EDW1.text_01_value,
                    CAST (CAST (EDW1.alt_as_of_date_time AS VARCHAR(19)) AS timestamp),
                    CAST (CAST (EDW1.as_of_date_time AS VARCHAR(19)) AS timestamp),
                    EDW1.tran_code,
                    CURRENT_TIMESTAMP() AS auto_c05  
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.edw_custom_lookup    AS EDW1   CROSS 
                JOIN
                    """ + db_params.EEDDWWDDBB2 + """.calendar_dates    AS EDW2   
                WHERE
                    upper(trim(EDW1.phantom_table_name)) = upper(trim('es_slm_class_lup'))  
                    AND upper(trim(EDW1.data_application_id)) = upper(trim('D8'))   
                    AND upper(trim(EDW1.tran_code)) = upper(trim('U'))   
                    AND  (
                        CAST (DATE_SUB( CURRENT_DATE() , 28 )  AS DATE) BETWEEN EDW2.begin_period_date AND EDW2.end_period_date 
                    )   
                    AND upper(trim(EDW2.type_period)) = upper(trim('C'))   
                GROUP BY
                    EDW1.instance_id ,
                    EDW1.text_01_value ,
                    CAST (CAST (EDW1.alt_as_of_date_time AS VARCHAR(19)) AS timestamp) ,
                    CAST (CAST (EDW1.as_of_date_time AS VARCHAR(19)) AS timestamp) ,
                    EDW1.tran_code ,
                    CURRENT_TIMESTAMP()"""
        print("Job 'D8_Es_slm_class_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """TRUNCATE TABLE """ + db_params.EEDDWWDDBB2 + """.es_slm_class_ld """
        print("Job 'D8_Es_slm_class_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT INTO """ + db_params.EEDDWWDDBB2 + """.es_slm_class_ld
        SELECT
         EDW1.instance_id
        ,EDW1.prod_class_code
        ,EDW1.alt_as_of_date_time
        ,EDW1.as_of_date_time
        ,EDW1.tran_code
        ,EDW1.update_date_time
        FROM """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_es_slm_class_t1 EDW1
        """
        print("Job 'D8_Es_slm_class_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """  ALTER TABLE es_slm_class RENAME TO es_slm_class_hd"""
        print("Job 'D8_Es_slm_class_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE es_slm_class_ld RENAME TO es_slm_class"""
        print("Job 'D8_Es_slm_class_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE es_slm_class_hd RENAME TO es_slm_class_ld"""
        print("Job 'D8_Es_slm_class_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = D8Esslmclassmergedpy(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print(e)
        print("Error in D8Esslmclassmergedpy")
        sparkSession.stop()
        sys.exit(1)


if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8Esslmclassmergedpy").enableHiveSupport().getOrCreate()
    main()
    sparkSession.stop()
    sys.exit(0)

