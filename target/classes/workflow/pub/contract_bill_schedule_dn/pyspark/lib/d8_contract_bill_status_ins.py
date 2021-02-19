from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractbillstatusinssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB + """ """
        print("Job: 'd8_contract_bill_status'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB + """.contract_bill_status"""
        print("Job: 'd8_contract_bill_status'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB + """.contract_bill_status           SELECT
                    COALESCE( status_code ,
                    ' ' ) AS auto_c00,
                    update_date_time  
                FROM
                    """ + db_params.EEDDWWDDBB + """.contract_bill_sched_dn_ld      
                GROUP BY
                    COALESCE( status_code ,
                    ' ' ) ,
                    update_date_time"""
        print("Job: 'd8_contract_bill_status'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB + """ """
        print("Job: 'd8_contract_bill_country'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB + """.contract_bill_country"""
        print("Job: 'd8_contract_bill_country'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB + """.contract_bill_country           SELECT
                    COALESCE( country_code ,
                    ' ' ) AS auto_c00,
                    COALESCE( country_desc ,
                    ' ' ) AS auto_c01,
                    update_date_time  
                FROM
                    """ + db_params.EEDDWWDDBB + """.contract_bill_sched_dn_ld      
                GROUP BY
                    COALESCE( country_code ,
                    ' ' ) ,
                    COALESCE( country_desc ,
                    ' ' ) ,
                    update_date_time"""
        print("Job: 'd8_contract_bill_country'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB + """ """
        print("Job: 'd8_contract_bill_group'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB + """.contract_bill_group"""
        print("Job: 'd8_contract_bill_group'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB + """.contract_bill_group           SELECT
                    COALESCE( contract_group_name ,
                    ' ' ) AS auto_c00,
                    update_date_time  
                FROM
                    """ + db_params.EEDDWWDDBB + """.contract_bill_sched_dn_ld      
                GROUP BY
                    COALESCE( contract_group_name ,
                    ' ' ) ,
                    update_date_time"""
        print("Job: 'd8_contract_bill_group'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractbillstatusinssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractbillstatusinssql").enableHiveSupport().getOrCreate()
    main()
