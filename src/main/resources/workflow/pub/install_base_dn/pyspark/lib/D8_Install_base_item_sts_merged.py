from pyspark.sql import SparkSession 
from datetime import datetime
import sys
import db_params

class D8Installbaseitemstsmergedpy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts_ld"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts_ld          
                SELECT
                    X.*  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts X"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """Truncate Table """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT INTO """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1
        SELECT 
        instance_id,
        item_instance_status_id,
        active_end_date_time,
        active_start_date_time,
        as_of_date_time,
        batch_nbr,
        created_by_name,
        creation_date_time,
        incident_allowed_flag,
        item_instance_status_desc,
        item_instance_status_name,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        migrated_flag,
        seed_status_update_flag,
        seeded_flag,
        service_order_allowed_flag,
        status_change_allowed_flag,
        terminated_flag,
        tran_code,
        update_flag
        FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_ml

                    WHERE
                        upper(trim(tran_code)) <> upper(trim('D'))"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    CACHE TABLE   """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """Truncate Table """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT INTO """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2
        SELECT 
        X.instance_id,
        X.item_instance_status_id,
        active_end_date_time,
        active_start_date_time,
        alt_as_of_date_time,
        alt_batch_nbr,
        as_of_date_time,
        batch_nbr,
        created_by_name,
        creation_date_time,
        'Y',
        incident_allowed_flag,
        item_instance_status_desc,
        item_instance_status_name,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        migrated_flag,
        seed_status_update_flag,
        seeded_flag,
        service_order_allowed_flag,
        status_change_allowed_flag,
        terminated_flag,
        tran_code,
        update_flag
        FROM """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts_ld X
        INNER JOIN
                    (
        SELECT Distinct
            instance_id ,
            item_instance_status_id 
            FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1 
                    ) AS autoAlias_52 
        on 
        X.instance_id =  autoAlias_52.instance_id and
        X.item_instance_status_id = autoAlias_52.item_instance_status_id"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    CACHE TABLE   """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        TRUNCATE TABLE """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t3"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT INTO """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t3
        SELECT
        A.instance_id,
        A.item_instance_status_id,
        active_end_date_time,
        active_start_date_time,
        created_by_name,
        creation_date_time,
        incident_allowed_flag,
        item_instance_status_desc,
        item_instance_status_name,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        migrated_flag,
        seed_status_update_flag,
        seeded_flag,
        service_order_allowed_flag,
        status_change_allowed_flag,
        terminated_flag,
        tran_code,
        update_flag
        FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2 A"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        use  """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT INTO """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t3
        SELECT
        A.instance_id,
        A.item_instance_status_id,
        active_end_date_time,
        active_start_date_time,
        created_by_name,
        creation_date_time,
        incident_allowed_flag,
        item_instance_status_desc,
        item_instance_status_name,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        migrated_flag,
        seed_status_update_flag,
        seeded_flag,
        service_order_allowed_flag,
        status_change_allowed_flag,
        terminated_flag,
        tran_code,
        update_flag
        FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1 A
        INNER JOIN
         (SELECT  DISTINCT
            instance_id,
            item_instance_status_id 
            FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2
            ) X
        on A.instance_id = X.instance_id and
        A.item_instance_status_id = X.item_instance_status_id
        """
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT OVERWRITE TABLE """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t3 SELECT DISTINCT * FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t3"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        use  """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT OVERWRITE TABLE """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2
        SELECT
               A.instance_id AS instance_id ,
                    A.item_instance_status_id AS item_instance_status_id ,
                    A.active_end_date_time AS active_end_date_time ,
                    A.active_start_date_time AS active_start_date_time ,
                    A.alt_as_of_date_time AS alt_as_of_date_time ,
                    A.alt_batch_nbr AS alt_batch_nbr ,
                    A.as_of_date_time AS as_of_date_time ,
                    A.batch_nbr AS batch_nbr ,
                    A.created_by_name AS created_by_name ,
                    A.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN X.autoc0 IS not null THEN 'D' 
                        ELSE A.current_record_ind 
                    END AS current_record_ind ,
        A.incident_allowed_flag         
        ,A.item_instance_status_desc     
        ,A.item_instance_status_name     
        ,A.last_update_date_time         
        ,A.last_update_login_name        
        ,A.last_updated_by_name          
        ,A.migrated_flag                 
        ,A.seed_status_update_flag       
        ,A.seeded_flag                   
        ,A.service_order_allowed_flag    
        ,A.status_change_allowed_flag    
        ,A.terminated_flag               
        ,A.tran_code                     
        ,A.update_flag                   
        FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2 A
        LEFT OUTER JOIN (SELECT instance_id as autoc0, item_instance_status_id as autoc1
            FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t3
            GROUP BY instance_id, item_instance_status_id
            HAVING COUNT(*) = 1
            ) X
        on
        A.instance_id = X.autoc0 and
        A.item_instance_status_id = X.autoc1
        """
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """with qq1 as (SELECT distinct instance_id, item_instance_status_id FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2  WHERE upper(trim(current_record_ind)) = 'D' ),
        qq2 as (SELECT DISTINCT A.instance_id,A.item_instance_status_id from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1 A left outer join qq1 B on   A.instance_id = B.instance_id and A.item_instance_status_id = B.item_instance_status_id where B.instance_id is null and B.item_instance_status_id is null)
            INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts_ld SELECT
                    X.* 
                FROM  """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts_ld X
                    left outer join 
                     qq2 Y
                on X.instance_id = Y.instance_id AND
                   X.item_instance_status_id = Y.item_instance_status_id
                where Y.instance_id is null and Y.item_instance_status_id is null"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts_ld           SELECT
         instance_id,
         item_instance_status_id,
         active_end_date_time,
         active_start_date_time,
         as_of_date_time,
         batch_nbr,
         as_of_date_time,
         batch_nbr,
         created_by_name,
         creation_date_time,
         incident_allowed_flag,
         item_instance_status_desc,
         item_instance_status_name,
         last_update_date_time,
         last_update_login_name,
         last_updated_by_name,
         migrated_flag,
         seed_status_update_flag,
         seeded_flag,
         service_order_allowed_flag,
         status_change_allowed_flag,
         terminated_flag,
         tran_code,
         update_flag
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT instance_id AS auto_c00,
                            item_instance_status_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_13 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_13.AUTO_C00)) 
                            AND item_instance_status_id = autoAlias_13.AUTO_C01 
                        ) 
                WHERE
                    autoAlias_13.AUTO_C00 IS  NULL  
                    AND autoAlias_13.AUTO_C01 IS  NULL"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1           
        SELECT 
        instance_id,
        item_instance_status_id,
        active_end_date_time,
        active_start_date_time,
        as_of_date_time,
        batch_nbr,
        created_by_name,
        creation_date_time,
        incident_allowed_flag,
        item_instance_status_desc,
        item_instance_status_name,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        migrated_flag,
        seed_status_update_flag,
        seeded_flag,
        service_order_allowed_flag,
        status_change_allowed_flag,
        terminated_flag,
        tran_code,
        update_flag
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_ml
                    WHERE
                        upper(trim(tran_code)) = upper(trim('D'))"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2           SELECT
        l.instance_id                   
        ,l.item_instance_status_id       
        ,l.active_end_date_time          
        ,l.active_start_date_time        
        ,l.alt_as_of_date_time           
        ,l.alt_batch_nbr                 
        ,l.as_of_date_time               
        ,l.batch_nbr                     
        ,l.created_by_name               
        ,l.creation_date_time            
        ,null            
        ,l.incident_allowed_flag         
        ,l.item_instance_status_desc     
        ,l.item_instance_status_name     
        ,l.last_update_date_time         
        ,l.last_update_login_name        
        ,l.last_updated_by_name          
        ,l.migrated_flag                 
        ,l.seed_status_update_flag       
        ,l.seeded_flag                   
        ,l.service_order_allowed_flag    
        ,l.status_change_allowed_flag    
        ,l.terminated_flag               
        ,l.tran_code                     
        ,l.update_flag                   
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts_ld    AS l   ,
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1    AS t   
                WHERE
                    l.instance_id = t.instance_id  
                    AND l.item_instance_status_id = t.item_instance_status_id   
                    AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts_ld SELECT
                    A.* 
                FROM  """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts_ld A Left outer join
            ( select distinct instance_id, item_instance_status_id from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2) B
        on A.instance_id = B.instance_id and
        A.item_instance_status_id = B.item_instance_status_id
        where B.instance_id is null and B.item_instance_status_id is null"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.install_base_item_sts_ld           
        SELECT 
        instance_id,
        item_instance_status_id,
        active_end_date_time,
        active_start_date_time,
        alt_as_of_date_time,
        alt_batch_nbr,
        as_of_date_time,
        batch_nbr,
        created_by_name,
        creation_date_time,
        incident_allowed_flag,
        item_instance_status_desc,
        item_instance_status_name,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        migrated_flag,
        seed_status_update_flag,
        seeded_flag,
        service_order_allowed_flag,
        status_change_allowed_flag,
        terminated_flag,
        tran_code,
        update_flag  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    UNCACHE TABLE   """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    UNCACHE TABLE   """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2"""
        print("Job 'D8_Install_base_item_sts_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = D8Installbaseitemstsmergedpy(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print(e)
        print("Error in D8Installbaseitemstsmergedpy")
        sparkSession.stop()
        sys.exit(1)


if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8Installbaseitemstsmergedpy").enableHiveSupport().getOrCreate()
    main()
    sparkSession.stop()
    sys.exit(0)
