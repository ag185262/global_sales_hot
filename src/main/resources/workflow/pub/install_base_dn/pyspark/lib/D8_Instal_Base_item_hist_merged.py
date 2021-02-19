from pyspark.sql import SparkSession
from datetime import datetime 
import sys  
import db_params

class D8InstalBaseitemhistmergedpy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB + """"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB + """.install_base_item_hist_ld"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB + """.install_base_item_hist_ld           SELECT
                    X.*  
                FROM
                    """ + db_params.EEDDWWDDBB + """.install_base_item_hist X"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t1"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t1           SELECT
                    instance_history_id,
                    instance_id,
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
                    transaction_id,
                    new_product_id,
                    old_product_id,
                    CASE 
                        WHEN m.new_product_id  IS NOT NULL  THEN substring ( CAST (m.creation_date_time AS VARCHAR(26)) ,
                        1  ,
                        10  )   
                    END AS auto_c019,
                    CASE 
                        WHEN m.new_product_id  IS NOT NULL  THEN substring ( CAST (m.creation_date_time AS VARCHAR(26)) ,
                        12  ,
                        5  )   
                    END AS auto_c020,
                    new_prev_cust_nbr,
                    old_prev_cust_nbr,
                    new_prev_site_nbr,
                    old_prev_site_nbr,
                    new_service_tier_name,
                    old_service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_ml    AS m 
                        WHERE
                            upper(trim(tran_code)) <> 'D'"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2           SELECT
                    instance_history_id,
                    instance_id,
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
                    transaction_id,
                    new_product_id,
                    old_product_id,
                    product_id_swap_date,
                    product_id_swap_time,
                    new_prev_cust_nbr,
                    old_prev_cust_nbr,
                    new_prev_site_nbr,
                    old_prev_site_nbr,
                    new_service_tier_name,
                    old_service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld 
                INNER JOIN
                    (
                        SELECT
                            instance_history_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t1 
                    ) AS autoAlias_72 
                        ON (
                            instance_history_id = autoAlias_72.AUTO_C00 
                            AND instance_id = autoAlias_72.AUTO_C01 
                        )"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t3"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t3           SELECT
                    instance_history_id,
                    instance_id,
                    created_by_name,
                    creation_date_time,
                    item_instance_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migrated_flag,
                    tran_code,
                    transaction_id,
                    new_product_id,
                    old_product_id,
                    product_id_swap_date,
                    product_id_swap_time,
                    new_prev_cust_nbr,
                    old_prev_cust_nbr,
                    new_prev_site_nbr,
                    old_prev_site_nbr,
                    new_service_tier_name,
                    old_service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t3           SELECT
                    instance_history_id,
                    instance_id,
                    created_by_name,
                    creation_date_time,
                    item_instance_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migrated_flag,
                    tran_code,
                    transaction_id,
                    new_product_id,
                    old_product_id,
                    product_id_swap_date,
                    product_id_swap_time,
                    new_prev_cust_nbr,
                    old_prev_cust_nbr,
                    new_prev_site_nbr,
                    old_prev_site_nbr,
                    new_service_tier_name,
                    old_service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t1 
                INNER JOIN
                    (
                        SELECT
                             instance_history_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2 
                    ) AS autoAlias_76 
                        ON (
                            instance_history_id = autoAlias_76.AUTO_C00 
                            AND instance_id = autoAlias_76.AUTO_C01 
                        )"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT overwrite TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t3  SELECT DISTINCT * FROM  """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t3"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            overwrite
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2
                SELECT
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.instance_history_id AS instance_history_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_78.auto_c00 IS not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_install_base_item_hist_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.item_instance_id AS item_instance_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.migrated_flag AS migrated_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.tran_code AS tran_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.transaction_id AS transaction_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.new_product_id AS new_product_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.old_product_id AS old_product_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.product_id_swap_date AS product_id_swap_date ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.product_id_swap_time AS product_id_swap_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.new_prev_cust_nbr AS new_prev_cust_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.old_prev_cust_nbr AS old_prev_cust_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.new_prev_site_nbr AS new_prev_site_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.old_prev_site_nbr AS old_prev_site_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.new_service_tier_name AS new_service_tier_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.old_service_tier_name AS old_service_tier_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.esd_flag AS esd_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.media_type AS media_type ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.license_model AS license_model ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.license_start_date AS license_start_date ,
                    AAPPLLIIDD1EENNVV_install_base_item_hist_t2.license_end_date AS license_end_date 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2 AS AAPPLLIIDD1EENNVV_install_base_item_hist_t2 
                Left Outer Join
                    (
                        SELECT
                            instance_history_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t3      
                        GROUP BY
                            instance_history_id,
                            instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_78 
                        ON (
                            autoAlias_78.auto_c00 = AAPPLLIIDD1EENNVV_install_base_item_hist_t2.instance_history_id 
                            AND autoAlias_78.auto_c01 = AAPPLLIIDD1EENNVV_install_base_item_hist_t2.instance_id
                        )"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.EEDDWWDDBB1 + """"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    WITH CTE AS 
            (SELECT   T1.instance_id ,T1.instance_history_id FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t1 T1
                LEFT JOIN
                (SELECT  instance_id,instance_history_id,current_record_ind FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2)T2
                ON T1.instance_id=T2.instance_id
                AND T1.instance_history_id=T2.instance_history_id
                AND T2.current_record_ind = 'D'
                WHERE T2.instance_id IS NULL
                AND T2.instance_history_id IS NULL
            )
            INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld
            SELECT A.* from  """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld A
            LEFT OUTER JOIN
            CTE B
            ON  A.instance_id=B.instance_id
            AND A.instance_history_id=B.instance_history_id
            WHERE B.instance_id IS  NULL
            AND B.instance_history_id IS  NULL"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld           SELECT
                    instance_history_id,
                    instance_id,
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
                    transaction_id,
                    new_product_id,
                    old_product_id,
                    product_id_swap_date,
                    product_id_swap_time,
                    new_prev_cust_nbr,
                    old_prev_cust_nbr,
                    new_prev_site_nbr,
                    old_prev_site_nbr,
                    new_service_tier_name,
                    old_service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                             instance_history_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2     
                        WHERE
                            upper(trim(current_record_ind)) = 'D'
                    ) AS autoAlias_82 
                        ON (
                            instance_history_id = autoAlias_82.AUTO_C00 
                            AND instance_id = autoAlias_82.AUTO_C01 
                        ) 
                WHERE
                    autoAlias_82.AUTO_C00 IS  NULL  
                    AND autoAlias_82.AUTO_C01 IS  NULL"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t1"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t1           SELECT
                    instance_history_id,
                    instance_id,
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
                    transaction_id,
                    new_product_id,
                    old_product_id,
                    CASE 
                        WHEN EDW1.new_product_id  IS NOT NULL  THEN substring ( CAST (EDW1.creation_date_time AS VARCHAR(26)) ,
                        1  ,
                        10  )   
                    END AS auto_c019,
                    CASE 
                        WHEN EDW1.new_product_id  IS NOT NULL  THEN substring ( CAST (EDW1.creation_date_time AS VARCHAR(26)) ,
                        12  ,
                        5  )   
                    END AS auto_c020,
                    new_prev_cust_nbr,
                    old_prev_cust_nbr,
                    new_prev_site_nbr,
                    old_prev_site_nbr,
                    new_service_tier_name,
                    old_service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_ml    AS EDW1 
                    WHERE
                        upper(trim(tran_code)) = 'D'"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2           SELECT
                    l.instance_history_id,
                    l.instance_id,
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
                    l.transaction_id,
                    l.new_product_id,
                    l.old_product_id,
                    l.product_id_swap_date,
                    l.product_id_swap_time,
                    l.new_prev_cust_nbr,
                    l.old_prev_cust_nbr,
                    l.new_prev_site_nbr,
                    l.old_prev_site_nbr,
                    l.new_service_tier_name,
                    l.old_service_tier_name,
                    l.esd_flag,
                    l.media_type,
                    l.license_model,
                    l.license_start_date,
                    l.license_end_date  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld    AS l   ,
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t1    AS t   
                WHERE
                    l.instance_history_id = t.instance_history_id  
                    AND l.instance_id = t.instance_id   
                    AND CONCAT(CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) < CONCAT(CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
            INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld SELECT DISTINCT
                    a.*  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld    AS A  
                LEFT OUTER JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_purge_t1    AS B  
                        on  (
                            A.item_instance_id = B.item_instance_id  
                            AND a.instance_id = B.instance_id  
                        )  
                where
                    B.item_instance_id IS NULL  
                    and  B.instance_id IS NULL  """
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld
        SELECT A.* from  """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld A
        LEFT OUTER JOIN
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2 B
        ON  A.instance_id=B.instance_id and
        A.instance_history_id=B.instance_history_id
        WHERE B.instance_id IS NULL
        AND B.instance_history_id IS NULL"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = """ INSERT INTO TABLE """ + db_params.EEDDWWDDBB1 + """.""" + """install_base_item_hist_ld SELECT
            instance_history_id,
            instance_id,
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
            transaction_id,
            new_product_id,
            old_product_id,
            product_id_swap_date,
            product_id_swap_time,
            new_prev_cust_nbr,
            old_prev_cust_nbr,
            new_prev_site_nbr,
            old_prev_site_nbr,
            new_service_tier_name,
            old_service_tier_name,
            esd_flag,
            media_type,
            license_model,
            license_start_date,
            license_end_date  
        FROM
            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_hist_t2 """
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ANALYZE TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld COMPUTE STATISTICS  FOR COLUMNS INSTANCE_HISTORY_ID,INSTANCE_ID,ITEM_INSTANCE_ID"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld COMPUTE STATISTICS  FOR COLUMNS item_instance_id,instance_id"""
        print("Job 'D8_Instal_Base_item_hist_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = D8InstalBaseitemhistmergedpy(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print(e)
        print("Error in D8InstalBaseitemhistmergedpy")
        sparkSession.stop()
        sys.exit(1)


if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8InstalBaseitemhistmergedpy").enableHiveSupport().getOrCreate()
    main()
    sparkSession.stop()
    sys.exit(0)

