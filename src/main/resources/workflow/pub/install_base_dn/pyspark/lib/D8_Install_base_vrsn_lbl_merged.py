from pyspark.sql import SparkSession
from datetime import datetime 
import sys 
import db_params

class D8Installbasevrsnlblmergedpy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl_ld"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl_ld          
                SELECT
                    a.*  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl a"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """Truncate Table """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT INTO """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1
        SELECT 
        instance_id,
        version_label_id,
        active_end_date_time,
        active_start_date_time,
        as_of_date_time,
        batch_nbr,
        created_by_name,
        creation_date_time,
        item_instance_id,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        migrated_flag,
        tran_code,
        version_label_date_time,
        version_label_desc,
        version_label_name
        FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_ml
                    WHERE
                        upper(trim(tran_code)) <> upper(trim('D'))"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    CACHE TABLE   """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """Truncate Table """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT INTO """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2
        SELECT 
        instance_id,
        version_label_id,
        active_end_date_time,
        active_start_date_time,
        alt_as_of_date_time,
        alt_batch_nbr,
        as_of_date_time,
        batch_nbr,
        created_by_name,
        creation_date_time,
        'Y',
        item_instance_id,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        migrated_flag,
        tran_code,
        version_label_date_time,
        version_label_desc,
        version_label_name
        FROM """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl_ld B
        INNER JOIN
         (SELECT DISTINCT
            instance_id as AUTO_C0,
            version_label_id as AUTO_C1
            FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1
            ) X
        on X.AUTO_C0 = B.instance_id AND
        X.AUTO_C1 = B.version_label_id"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    CACHE TABLE   """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        TRUNCATE TABLE """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t3"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT INTO """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t3
        SELECT
        instance_id,
        version_label_id,
        active_end_date_time,
        active_start_date_time,
        created_by_name,
        creation_date_time,
        item_instance_id,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        migrated_flag,
        tran_code,
        version_label_date_time,
        version_label_desc,
        version_label_name
        FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """use """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT INTO """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t3
        SELECT
        instance_id,
        version_label_id,
        active_end_date_time,
        active_start_date_time,
        created_by_name,
        creation_date_time,
        item_instance_id,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        migrated_flag,
        tran_code,
        version_label_date_time,
        version_label_desc,
        version_label_name
        FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1 A
        INNER JOIN (SELECT Distinct
            instance_id as autoc0,
            version_label_id as autoc1
            FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2
            ) X
        on
        A.instance_id = X.autoc0 and
        A.version_label_id = X.autoc1
        """
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT OVERWRITE TABLE """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t3 SELECT DISTINCT * FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t3"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """use """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT OVERWRITE TABLE """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2
        SELECT
               A.instance_id AS instance_id ,
                    A.version_label_id AS version_label_id ,
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
                    A.item_instance_id AS item_instance_id ,
                    A.last_update_date_time AS last_update_date_time ,
                    A.last_update_login_name AS last_update_login_name ,
                    A.last_updated_by_name AS last_updated_by_name ,
                    A.migrated_flag AS migrated_flag ,
                    A.tran_code AS tran_code ,
                    A.version_label_date_time AS version_label_date_time ,
                    A.version_label_desc AS version_label_desc ,
                    A.version_label_name AS version_label_name 
        FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2 A
        LEFT OUTER JOIN (SELECT instance_id as autoc0, version_label_id as autoc1
            FROM """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t3
            GROUP BY instance_id, version_label_id
            HAVING COUNT(*) = 1
            ) X
        on
        A.instance_id = X.autoc0 and
        A.version_label_id = X.autoc1
        """
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """with qq1 as (SELECT distinct instance_id, version_label_id FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2  WHERE upper(trim(current_record_ind)) = 'D' ),
        qq2 as (SELECT DISTINCT A.instance_id,A.version_label_id from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1 A left outer join qq1 B on   A.instance_id = B.instance_id and A.version_label_id = B.version_label_id where B.instance_id is null and B.version_label_id is null)
            INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl_ld SELECT
                    X.* 
                FROM  """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl_ld X
                    left outer join 
                     qq2 Y
                on X.instance_id = Y.instance_id AND
                   X.version_label_id = Y.version_label_id
                where Y.instance_id is null and Y.version_label_id is null"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl_ld           SELECT
                    instance_id,
                    version_label_id,
                    active_end_date_time,
                    active_start_date_time,
                    as_of_date_time,
                    batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    item_instance_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migrated_flag,
                    tran_code,
                    version_label_date_time,
                    version_label_desc,
                    version_label_name  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT instance_id AS auto_c00,
                            version_label_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_13 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_13.AUTO_C00)) 
                            AND version_label_id = autoAlias_13.AUTO_C01 
                        ) 
                WHERE
                    autoAlias_13.AUTO_C00 IS  NULL  
                    AND autoAlias_13.AUTO_C01 IS  NULL"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1           SELECT
                    instance_id,
                    version_label_id,
                    active_end_date_time,
                    active_start_date_time,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    item_instance_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migrated_flag,
                    tran_code,
                    version_label_date_time,
                    version_label_desc,
                    version_label_name  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_ml 
                    WHERE
                        upper(trim(tran_code)) = upper(trim('D'))"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2           SELECT
                    l.instance_id,
                    l.version_label_id,
                    l.active_end_date_time,
                    l.active_start_date_time,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.created_by_name,
                    l.creation_date_time,
                    null,
                    l.item_instance_id,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    l.migrated_flag,
                    t.tran_code,
                    l.version_label_date_time,
                    l.version_label_desc,
                    l.version_label_name  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl_ld    AS l   ,
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1    AS t   
                WHERE
                    l.instance_id = t.instance_id  
                    AND l.version_label_id = t.version_label_id   
                    AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl_ld SELECT
                    A.* 
                FROM  """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl_ld A Left outer join
            ( select distinct instance_id, version_label_id from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2) B
        on A.instance_id = B.instance_id and
        A.version_label_id = B.version_label_id
        where B.instance_id is null and B.version_label_id is null"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.install_base_vrsn_lbl_ld           SELECT
                    instance_id,
                    version_label_id,
                    active_end_date_time,
                    active_start_date_time,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    item_instance_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migrated_flag,
                    tran_code,
                    version_label_date_time,
                    version_label_desc,
                    version_label_name  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    UNCACHE TABLE   """ + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1"""
        print("Job 'D8_Install_base_vrsn_lbl_merged'. Running Query at {}: {}".format(datetime.now(), query))


def main():
    m = D8Installbasevrsnlblmergedpy(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print(e)
        print("Error in D8Installbasevrsnlblmergedpy")
        sparkSession.stop()
        sys.exit(1)

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8Installbasevrsnlblmergedpy").enableHiveSupport().getOrCreate()
    main()
    sparkSession.stop()
    sys.exit(0)

