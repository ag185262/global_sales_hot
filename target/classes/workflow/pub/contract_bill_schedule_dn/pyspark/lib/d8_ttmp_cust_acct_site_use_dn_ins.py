from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8ttmpcustacctsiteusedninssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.ttmp_cust_acct_site_use_dn"""
        print("Job: 'd8_ttmp_cust_acct_site_use_dn'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.ttmp_cust_acct_site_use_dn           SELECT
                    cust_acct_site_use_id,
                    instance_id,
                    cust_sales_channel_code,
                    naics_code,
                    naics_desc,
                    cust_acct_site_use_code  
                FROM
                    """ + db_params.EEDDWWDDBB + """.cust_acct_site_use_dn"""
        print("Job: 'd8_ttmp_cust_acct_site_use_dn'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.ttmp_cust_acct_site_use_dn COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_ttmp_cust_acct_site_use_dn'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.ttmp_cust_acct_site_use_dn COMPUTE STATISTICS  FOR COLUMNS cust_acct_site_use_id"""
        print("Job: 'd8_ttmp_cust_acct_site_use_dn'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.ttmp_cust_acct_site_use_dn"""
        print("Job: 'd8_ttmp_cust_acct_site_use_dn'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.ttmp_cust_acct_site_use_dn           SELECT
                    cust_acct_site_use_id,
                    instance_id,
                    cust_sales_channel_code,
                    naics_code,
                    naics_desc,
                    cust_acct_site_use_code  
                FROM
                    """ + db_params.EEDDWWDDBB + """.cust_acct_site_use_dn"""
        print("Job: 'd8_ttmp_cust_acct_site_use_dn'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.ttmp_cust_acct_site_use_dn COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_ttmp_cust_acct_site_use_dn'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.ttmp_cust_acct_site_use_dn COMPUTE STATISTICS  FOR COLUMNS cust_acct_site_use_id"""
        print("Job: 'd8_ttmp_cust_acct_site_use_dn'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8ttmpcustacctsiteusedninssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8ttmpcustacctsiteusedninssql").enableHiveSupport().getOrCreate()
    main()
