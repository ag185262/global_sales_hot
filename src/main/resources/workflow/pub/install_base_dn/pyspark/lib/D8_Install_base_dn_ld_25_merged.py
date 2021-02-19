from pyspark.sql import SparkSession 
from datetime import datetime
import sys 
import db_params

class D8Installbasednld25mergedpy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB + """"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB + """.install_base_dn_ld"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB + """.install_base_dn_ld           SELECT
                    X.*  
                FROM
                    """ + db_params.EEDDWWDDBB + """.install_base_dn X CLUSTER BY X.INSTANCE_ID,X.ITEM_INSTANCE_ID"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB + """"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t01"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t01           SELECT
                    instance_id,
                    item_instance_id,
                    to_date (active_end_date_time ,'yyyy-MM-dd') AS auto_c02,
                    to_date (active_start_date_time ,'yyyy-MM-dd') AS auto_c03,
                    to_date (actual_return_date_time ,'yyyy-MM-dd') AS auto_c04,
                    external_reference_nbr,
                    to_date (install_date_time ,'yyyy-MM-dd') AS auto_c06,
                    install_location_id,
                    install_location_type_code,
                    inventory_item_id,
                    inventory_master_org_id,
                    item_instance_status_id,
                    tran_code,
                    last_order_line_id,
                    last_valid_invtry_org_id,
                    location_id,
                    location_type_code,
                    to_date (return_by_date_time ,'yyyy-MM-dd') AS auto_c017,
                    prev_site_nbr,
                    service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date,
                cit_vendor_code,        
                mdm_product_identifier, 
                mdm_solution_identifier
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.install_base_item_ld"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t02"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t01 COMPUTE STATISTICS  FOR COLUMNS item_instance_status_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t01 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t02           SELECT /*+ BROADCASTJOIN( """ + db_params.EEDDWWDDBB2 + """.install_base_item_sts_ld ) */
                    EDW1.instance_id,
                    EDW1.item_instance_id,
                    EDW1.active_end_date,
                    EDW1.active_start_date,
                    EDW1.actual_return_date,
                    EDW1.external_reference_nbr,
                    EDW1.install_date,
                    EDW1.install_location_id,
                    EDW1.install_location_type_code,
                    EDW1.inventory_item_id,
                    EDW1.inventory_master_org_id,
                    IBIS.item_instance_status_desc,
                    EDW1.item_instance_status_id,
                    IBIS.item_instance_status_name,
                    EDW1.item_instance_tran_code,
                    EDW1.last_order_line_id,
                    EDW1.last_valid_invtry_org_id,
                    EDW1.location_id,
                    EDW1.location_type_code,
                    EDW1.return_by_date,
                    IBIS.service_order_allowed_flag,
                    IBIS.tran_code,
                    IBIS.terminated_flag,
                    EDW1.prev_site_nbr,
                    EDW1.service_tier_name,
                    EDW1.esd_flag,
                    EDW1.media_type,
                    EDW1.license_model,
                    EDW1.license_start_date,
                    EDW1.license_end_date,
                    EDW1.cit_vendor_code,        
                EDW1.mdm_product_identifier, 
                EDW1.mdm_solution_identifier  
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t01    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.install_base_item_sts_ld    AS IBIS    
                        ON EDW1.instance_id = IBIS.instance_id  
                        AND EDW1.item_instance_status_id = IBIS.item_instance_status_id CLUSTER BY EDW1.INSTANCE_ID,EDW1.ITEM_INSTANCE_ID"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """TRUNCATE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t03"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        with qq1 as (SELECT version_label_date_time, version_label_desc, version_label_name, tran_code, instance_id, item_instance_id, version_label_id, rank() over (partition by instance_id, item_instance_id order by version_label_id DESC) AS RNK FROM """ + db_params.EEDDWWDDBB2 + """.""" + """install_base_vrsn_lbl_ld) INSERT 
                        INTO
                            """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t03
                            SELECT
                                EDW2.instance_id,
                                EDW2.item_instance_id,
                                EDW2.active_end_date,
                                EDW2.active_start_date,
                                EDW2.actual_return_date,
                                EDW2.external_reference_nbr,
                                EDW2.install_date,
                                EDW2.install_location_id,
                                EDW2.install_location_type_code,
                                EDW2.inventory_item_id,
                                EDW2.inventory_master_org_id,
                                EDW2.item_instance_status_desc,
                                EDW2.item_instance_status_id,
                                EDW2.item_instance_status_name,
                                EDW2.item_instance_tran_code,
                                EDW2.last_order_line_id,
                                EDW2.last_valid_invtry_org_id,
                                EDW2.location_id,
                                EDW2.location_type_code,
                                EDW2.return_by_date,
                                EDW2.service_order_allowed_flag,
                                EDW2.status_tran_code,
                                EDW2.terminated_flag,
                                to_date(IBVL.version_label_date_time ,'yyyy-MM-dd'),
                                IBVL.version_label_desc,
                                IBVL.version_label_name,
                                IBVL.tran_code,
                                EDW2.prev_site_nbr,
                                EDW2.service_tier_name,
                                EDW2.esd_flag,
                                EDW2.media_type,
                                EDW2.license_model,
                                EDW2.license_start_date,
                                EDW2.license_end_date,
                            EDW2.cit_vendor_code,        
                            EDW2.mdm_product_identifier, 
                            EDW2.mdm_solution_identifier
 
                            FROM
                                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t02 EDW2 
                            LEFT OUTER JOIN
                                (
                                    SELECT
                                        version_label_date_time,
                                        version_label_desc,
                                        version_label_name,
                                        tran_code,
                                        instance_id,
                                        item_instance_id,
                                        version_label_id,
                                        RNK 
                                    FROM
                                        qq1 
                                    where
                                        RNK=1 
                                ) IBVL  
                                    ON EDW2.instance_id = IBVL.instance_id 
                                    AND EDW2.item_instance_id = IBVL.item_instance_id """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT OVERWRITE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t03 SELECT DISTINCT * FROM """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t03"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t04"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t03 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t03 COMPUTE STATISTICS  FOR COLUMNS install_location_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t03 COMPUTE STATISTICS  FOR COLUMNS instance_id,install_location_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ INSERT  
                      INTO
                        TABLE
                        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t04           SELECT
                            EDW3.instance_id,
                            EDW3.item_instance_id,
                            EDW3.active_end_date,
                            EDW3.active_start_date,
                            EDW3.actual_return_date,
                            EDW3.external_reference_nbr,
                            EDW3.install_date,
                            (case when EDW3.install_location_id is not null then CAS.operating_unit_id else NULL end),
                            EDW3.install_location_id,
                            EDW3.install_location_type_code,
                            EDW3.inventory_item_id,
                            EDW3.inventory_master_org_id,
                            EDW3.item_instance_status_desc,
                            EDW3.item_instance_status_id,
                            EDW3.item_instance_status_name,
                            EDW3.item_instance_tran_code,
                            EDW3.last_order_line_id,
                            EDW3.last_valid_invtry_org_id,
                            EDW3.location_id,
                            EDW3.location_type_code,
                            EDW3.return_by_date,
                            EDW3.service_order_allowed_flag,
                            EDW3.status_tran_code,
                            EDW3.terminated_flag,
                            EDW3.version_label_date,
                            EDW3.version_label_desc,
                            EDW3.version_label_name,
                            EDW3.vrsn_lbl_tran_code,
                            EDW3.prev_site_nbr,
                            EDW3.service_tier_name,
                            EDW3.esd_flag,
                            EDW3.media_type,
                            EDW3.license_model,
                            EDW3.license_start_date,
                            EDW3.license_end_date,
                        EDW3.cit_vendor_code,        
                        EDW3.mdm_product_identifier, 
                        EDW3.mdm_solution_identifier
                        FROM
                            """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t03 AS EDW3  
                        LEFT OUTER  JOIN
                            """ + db_params.EEDDWWDDBB1 + """.cust_account_site    AS CAS  
                                ON EDW3.instance_id = CAS.instance_id  
                                AND EDW3.install_location_id = CAS.party_site_id AND trim(upper(install_location_type_code)) = 'HZ_PARTY_SITES' CLUSTER BY EDW3.INSTANCE_ID,EDW3.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t05"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ INSERT  
                    INTO
                        TABLE
                        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t05           SELECT
                            EDW4.instance_id,
                            EDW4.item_instance_id,
                            EDW4.active_end_date,
                            EDW4.active_start_date,
                            EDW4.actual_return_date,
                            CAS.oper_unit_country_code,
                            CAS.cs_fml_organization_code,
                            EDW4.external_reference_nbr,
                            EDW4.install_date,
                            EDW4.install_loc_oper_unit_id,
                            EDW4.install_location_id,
                            EDW4.install_location_type_code,
                            EDW4.inventory_item_id,
                            EDW4.inventory_master_org_id,
                            EDW4.item_instance_status_desc,
                            EDW4.item_instance_status_id,
                            EDW4.item_instance_status_name,
                            EDW4.item_instance_tran_code,
                            EDW4.last_order_line_id,
                            EDW4.last_valid_invtry_org_id,
                            EDW4.location_id,
                            EDW4.location_type_code,
                            EDW4.return_by_date,
                            EDW4.service_order_allowed_flag,
                            EDW4.status_tran_code,
                            EDW4.terminated_flag,
                            EDW4.version_label_date,
                            EDW4.version_label_desc,
                            EDW4.version_label_name,
                            EDW4.vrsn_lbl_tran_code,
                            EDW4.prev_site_nbr,
                            EDW4.service_tier_name,
                            EDW4.esd_flag,
                            EDW4.media_type,
                            EDW4.license_model,
                            EDW4.license_start_date,
                            EDW4.license_end_date,
                        EDW4.cit_vendor_code,
                        EDW4.mdm_product_identifier,
                        EDW4.mdm_solution_identifier
                        FROM
                            """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t04 AS EDW4  
                        LEFT OUTER  JOIN
                            """ + db_params.EEDDWWDDBB1 + """.cust_account_site    AS CAS  
                                ON EDW4.instance_id = CAS.instance_id  
                                AND EDW4.location_id = CAS.party_site_id AND UPPER(TRIM(location_type_code)) = 'HZ_PARTY_SITES' CLUSTER BY EDW4.INSTANCE_ID,EDW4.ITEM_INSTANCE_ID"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t06"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t06           SELECT /*+ BROADCASTJOIN( """ + db_params.EEDDWWDDBB1 + """.sales_order_sys_parm ) */
                    EDW5.instance_id,
                    EDW5.item_instance_id,
                    EDW5.active_end_date,
                    EDW5.active_start_date,
                    EDW5.actual_return_date,
                    EDW5.country_code,
                    EDW5.crnt_loc_cs_fml_org_code,
                    EDW5.external_reference_nbr,
                    EDW5.install_date,
                    EDW5.install_loc_oper_unit_id,
                    EDW5.install_location_id,
                    EDW5.install_location_type_code,
                    EDW5.inventory_item_id,
                    EDW5.inventory_master_org_id,
                    EDW5.item_instance_status_desc,
                    EDW5.item_instance_status_id,
                    EDW5.item_instance_status_name,
                    EDW5.item_instance_tran_code,
                    EDW5.last_order_line_id,
                    EDW5.last_valid_invtry_org_id,
                    EDW5.location_id,
                    EDW5.location_type_code,
                    EDW5.return_by_date,
                    EDW5.service_order_allowed_flag,
                    EDW5.status_tran_code,
                    EDW5.terminated_flag,
                    EDW5.version_label_date,
                    EDW5.version_label_desc,
                    EDW5.version_label_name,
                    SOSP.inventory_org_id,
                    EDW5.vrsn_lbl_tran_code,
                    EDW5.prev_site_nbr,
                    EDW5.service_tier_name,
                    EDW5.esd_flag,
                    EDW5.media_type,
                    EDW5.license_model,
                    EDW5.license_start_date,
                    EDW5.license_end_date,
                    EDW5.cit_vendor_code,
                    EDW5.mdm_product_identifier,
                    EDW5.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t05    AS EDW5   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB1 + """.sales_order_sys_parm    AS SOSP    
                        ON EDW5.instance_id = SOSP.instance_id  
                        AND EDW5.install_loc_oper_unit_id = SOSP.operating_unit_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t07"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t06 COMPUTE STATISTICS  FOR COLUMNS last_order_line_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t06 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t06 COMPUTE STATISTICS"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """                INSERT  
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t07           SELECT /*+ BROADCASTJOIN( """ + db_params.EEDDWWDDBB1 + """.sales_order_line ) */
                    EDW6.instance_id,
                    EDW6.item_instance_id,
                    EDW6.active_end_date,
                    EDW6.active_start_date,
                    EDW6.actual_return_date,
                    to_date (SOL.actual_ship_date_time ,'yyyy-MM-dd') AS auto_c05,
                    SOL.invoice_to_site_use_id,
                    EDW6.country_code,
                    EDW6.crnt_loc_cs_fml_org_code,
                    EDW6.external_reference_nbr,
                    EDW6.install_date,
                    EDW6.install_loc_oper_unit_id,
                    EDW6.install_location_id,
                    EDW6.install_location_type_code,
                    EDW6.inventory_item_id,
                    EDW6.inventory_master_org_id,
                    EDW6.item_instance_status_desc,
                    EDW6.item_instance_status_id,
                    EDW6.item_instance_status_name,
                    EDW6.item_instance_tran_code,
                    EDW6.last_order_line_id,
                    EDW6.last_valid_invtry_org_id,
                    EDW6.location_id,
                    EDW6.location_type_code,
                    SOL.order_header_id,
                    SOL.order_line_nbr,
                    EDW6.return_by_date,
                    EDW6.service_order_allowed_flag,
                    SOL.ship_to_site_use_id,
                    EDW6.status_tran_code,
                    EDW6.terminated_flag,
                    EDW6.version_label_date,
                    EDW6.version_label_desc,
                    EDW6.version_label_name,
                    EDW6.vldtn_inventory_org_id,
                    EDW6.vrsn_lbl_tran_code,
                    EDW6.prev_site_nbr,
                    EDW6.service_tier_name,
                    EDW6.esd_flag,
                    EDW6.media_type,
                    EDW6.license_model,
                    EDW6.license_start_date,
                    EDW6.license_end_date,
                EDW6.cit_vendor_code,        
                EDW6.mdm_product_identifier, 
                EDW6.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t06    AS EDW6   ,
                    """ + db_params.EEDDWWDDBB1 + """.sales_order_line    AS SOL  
                WHERE
                    EDW6.instance_id = SOL.instance_id  
                    AND EDW6.last_order_line_id = SOL.order_line_id  
                    AND EDW6.last_order_line_id is not null    CLUSTER BY EDW6.INSTANCE_ID,EDW6.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """             INSERT  
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t07           SELECT
                    EDW6.instance_id,
                    EDW6.item_instance_id,
                    EDW6.active_end_date,
                    EDW6.active_start_date,
                    EDW6.actual_return_date,
                    Null,
                    Null,
                    EDW6.country_code,
                    EDW6.crnt_loc_cs_fml_org_code,
                    EDW6.external_reference_nbr,
                    EDW6.install_date,
                    EDW6.install_loc_oper_unit_id,
                    EDW6.install_location_id,
                    EDW6.install_location_type_code,
                    EDW6.inventory_item_id,
                    EDW6.inventory_master_org_id,
                    EDW6.item_instance_status_desc,
                    EDW6.item_instance_status_id,
                    EDW6.item_instance_status_name,
                    EDW6.item_instance_tran_code,
                    EDW6.last_order_line_id,
                    EDW6.last_valid_invtry_org_id,
                    EDW6.location_id,
                    EDW6.location_type_code,
                    Null,
                    Null,
                    EDW6.return_by_date,
                    EDW6.service_order_allowed_flag,
                    Null,
                    EDW6.status_tran_code,
                    EDW6.terminated_flag,
                    EDW6.version_label_date,
                    EDW6.version_label_desc,
                    EDW6.version_label_name,
                    EDW6.vldtn_inventory_org_id,
                    EDW6.vrsn_lbl_tran_code,
                    EDW6.prev_site_nbr,
                    EDW6.service_tier_name,
                    EDW6.esd_flag,
                    EDW6.media_type,
                    EDW6.license_model,
                    EDW6.license_start_date,
                    EDW6.license_end_date,
                EDW6.cit_vendor_code,        
                EDW6.mdm_product_identifier, 
                EDW6.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t06    AS EDW6  
                WHERE
                    EDW6.last_order_line_id is null    """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ INSERT   INTO TABLE
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t07          SELECT
                EDW6.instance_id,
                EDW6.item_instance_id,
                EDW6.active_end_date,
                EDW6.active_start_date,
                EDW6.actual_return_date,
                Null,
                Null,
                EDW6.country_code,
                EDW6.crnt_loc_cs_fml_org_code,
                EDW6.external_reference_nbr,
                EDW6.install_date,
                EDW6.install_loc_oper_unit_id,
                EDW6.install_location_id,
                EDW6.install_location_type_code,
                EDW6.inventory_item_id,
                EDW6.inventory_master_org_id,
                EDW6.item_instance_status_desc,
                EDW6.item_instance_status_id,
                EDW6.item_instance_status_name,
                EDW6.item_instance_tran_code,
                EDW6.last_order_line_id,
                EDW6.last_valid_invtry_org_id,
                EDW6.location_id,
                EDW6.location_type_code,
                Null,
                Null,
                EDW6.return_by_date,
                EDW6.service_order_allowed_flag,
                Null,
                EDW6.status_tran_code,
                EDW6.terminated_flag,
                EDW6.version_label_date,
                EDW6.version_label_desc,
                EDW6.version_label_name,
                EDW6.vldtn_inventory_org_id,
                EDW6.vrsn_lbl_tran_code,
                EDW6.prev_site_nbr,
                EDW6.service_tier_name,
                EDW6.esd_flag,
                EDW6.media_type,
                EDW6.license_model,
                EDW6.license_start_date,
                EDW6.license_end_date,
                EDW6.cit_vendor_code,        
                EDW6.mdm_product_identifier, 
                EDW6.mdm_solution_identifier
 
            FROM
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t06    AS EDW6  
            LEFT OUTER JOIN
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t07  AS autoAlias_40  
                    ON ( upper(trim(EDW6.instance_id)) = upper(trim(autoAlias_40.instance_id))  
                    AND EDW6.item_instance_id = autoAlias_40.item_instance_id )  
            WHERE
                autoAlias_40.instance_id IS  NULL  
                AND autoAlias_40.item_instance_id IS  NULL    CLUSTER BY EDW6.INSTANCE_ID,EDW6.ITEM_INSTANCE_ID     """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t08"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t07 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t07 COMPUTE STATISTICS  FOR COLUMNS order_header_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """             INSERT  
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t08           SELECT
                    EDW7.instance_id,
                    EDW7.item_instance_id,
                    EDW7.active_end_date,
                    EDW7.active_start_date,
                    EDW7.actual_return_date,
                    EDW7.actual_ship_date,
                    EDW7.bill_to_site_use_id,
                    EDW7.country_code,
                    EDW7.crnt_loc_cs_fml_org_code,
                    null,
                    null,
                    EDW7.external_reference_nbr,
                    EDW7.install_date,
                    EDW7.install_loc_oper_unit_id,
                    EDW7.install_location_id,
                    EDW7.install_location_type_code,
                    EDW7.inventory_item_id,
                    EDW7.inventory_master_org_id,
                    EDW7.item_instance_status_desc,
                    EDW7.item_instance_status_id,
                    EDW7.item_instance_status_name,
                    EDW7.item_instance_tran_code,
                    EDW7.last_order_line_id,
                    EDW7.last_valid_invtry_org_id,
                    EDW7.location_id,
                    EDW7.location_type_code,
                    null,
                    EDW7.order_header_id,
                    EDW7.order_line_nbr,
                    null,
                    EDW7.return_by_date,
                    EDW7.service_order_allowed_flag,
                    EDW7.ship_to_site_use_id,
                    EDW7.status_tran_code,
                    EDW7.terminated_flag,
                    EDW7.version_label_date,
                    EDW7.version_label_desc,
                    EDW7.version_label_name,
                    EDW7.vldtn_inventory_org_id,
                    EDW7.vrsn_lbl_tran_code,
                    EDW7.prev_site_nbr,
                    EDW7.service_tier_name,
                    EDW7.esd_flag,
                    EDW7.media_type,
                    EDW7.license_model,
                    EDW7.license_start_date,
                    EDW7.license_end_date,
                EDW7.cit_vendor_code,        
                EDW7.mdm_product_identifier, 
                EDW7.mdm_solution_identifier
 
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t07    AS EDW7  
                WHERE
                    EDW7.order_header_id  IS NULL  CLUSTER BY EDW7.INSTANCE_ID,EDW7.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """                         INSERT  
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t08           SELECT /*+ BROADCASTJOIN( sales_order ) */
                    EDW7.instance_id,
                    EDW7.item_instance_id,
                    EDW7.active_end_date,
                    EDW7.active_start_date,
                    EDW7.actual_return_date,
                    EDW7.actual_ship_date,
                    EDW7.bill_to_site_use_id,
                    EDW7.country_code,
                    EDW7.crnt_loc_cs_fml_org_code,
                    SO.customer_po_date,
                    SO.customer_po_nbr,
                    EDW7.external_reference_nbr,
                    EDW7.install_date,
                    EDW7.install_loc_oper_unit_id,
                    EDW7.install_location_id,
                    EDW7.install_location_type_code,
                    EDW7.inventory_item_id,
                    EDW7.inventory_master_org_id,
                    EDW7.item_instance_status_desc,
                    EDW7.item_instance_status_id,
                    EDW7.item_instance_status_name,
                    EDW7.item_instance_tran_code,
                    EDW7.last_order_line_id,
                    EDW7.last_valid_invtry_org_id,
                    EDW7.location_id,
                    EDW7.location_type_code,
                    to_date (SO.order_date_time ,'yyyy-MM-dd') AS auto_c026,
                    EDW7.order_header_id,
                    EDW7.order_line_nbr,
                    SO.order_nbr,
                    EDW7.return_by_date,
                    EDW7.service_order_allowed_flag,
                    EDW7.ship_to_site_use_id,
                    EDW7.status_tran_code,
                    EDW7.terminated_flag,
                    EDW7.version_label_date,
                    EDW7.version_label_desc,
                    EDW7.version_label_name,
                    EDW7.vldtn_inventory_org_id,
                    EDW7.vrsn_lbl_tran_code,
                    EDW7.prev_site_nbr,
                    EDW7.service_tier_name,
                    EDW7.esd_flag,
                    EDW7.media_type,
                    EDW7.license_model,
                    EDW7.license_start_date,
                    EDW7.license_end_date,
                EDW7.cit_vendor_code,        
                EDW7.mdm_product_identifier, 
                EDW7.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t07    AS EDW7  
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB1 + """.sales_order    AS SO  
                        ON EDW7.instance_id = SO.instance_id  
                        AND EDW7.order_header_id = SO.order_header_id  
                        AND EDW7.order_header_id  IS NOT NULL  
                LEFt OUTER JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t08 EDW8  
                        on EDW7.instance_id = EDW8.instance_id 
                        AND  EDW7.item_instance_id = EDW8.item_instance_id  
                where
                    EDW8.instance_id is null 
                    and edw8.item_instance_id is null and EDW7.order_header_id is not null  """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t09"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t09           SELECT
                    EDW8.instance_id,
                    EDW8.item_instance_id,
                    EDW8.active_end_date,
                    EDW8.active_start_date,
                    EDW8.actual_return_date,
                    EDW8.actual_ship_date,
                    EDW8.bill_to_site_use_id,
                    II.bom_enabled_flag,
                    EDW8.country_code,
                    EDW8.crnt_loc_cs_fml_org_code,
                    EDW8.customer_po_date,
                    EDW8.customer_po_nbr,
                    EDW8.external_reference_nbr,
                    EDW8.install_date,
                    EDW8.install_loc_oper_unit_id,
                    EDW8.install_location_id,
                    EDW8.install_location_type_code,
                    EDW8.inventory_item_id,
                    EDW8.inventory_master_org_id,
                    II.invtry_item_desc,
                    II.invtry_item_flag,
                    II.invtry_item_status_code,
                    1 AS auto_c022,
                    II.tran_code,
                    II.unit_of_measure_code,
                    EDW8.item_instance_status_desc,
                    EDW8.item_instance_status_id,
                    EDW8.item_instance_status_name,
                    EDW8.item_instance_tran_code,
                    II.item_type_name,
                    EDW8.last_order_line_id,
                    EDW8.last_valid_invtry_org_id,
                    EDW8.location_id,
                    EDW8.location_type_code,
                    II.offering_acctg_type_code,
                    EDW8.order_date,
                    EDW8.order_header_id,
                    EDW8.order_line_nbr,
                    EDW8.order_nbr,
                    SUBSTR( II.offering_id ,
                    1 ,
                    4 ) AS auto_c039,
                    SUBSTR( II.offering_id ,
                    1 ,
                    8 ) AS auto_c040,
                    II.offering_id,
                    II.offering_id_hyphenated,
                    SUBSTR( II.offering_id ,
                    5 ,
                    4 ) AS auto_c043,
                    SUBSTR( II.offering_id ,
                    9 ,
                    4 ) AS auto_c044,
                    EDW8.return_by_date,
                    II.serial_nbr_control_code,
                    EDW8.service_order_allowed_flag,
                    EDW8.ship_to_site_use_id,
                    II.shippable_item_flag,
                    EDW8.status_tran_code,
                    EDW8.terminated_flag,
                    EDW8.version_label_date,
                    EDW8.version_label_desc,
                    EDW8.version_label_name,
                    EDW8.vldtn_inventory_org_id,
                    EDW8.vrsn_lbl_tran_code,
                    COALESCE( II.invtry_item_desc_unc ,
                    ' ' ) AS auto_c057,
                    EDW8.prev_site_nbr,
                    EDW8.service_tier_name,
                    EDW8.esd_flag,
                    EDW8.media_type,
                    EDW8.license_model,
                    EDW8.license_start_date,
                    EDW8.license_end_date,
                EDW8.cit_vendor_code,        
                EDW8.mdm_product_identifier, 
                EDW8.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t08    AS EDW8   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB1 + """.invtry_item    AS II    
                        ON EDW8.instance_id = II.instance_id  
                        AND EDW8.inventory_item_id = II.inventory_item_id   
                        AND EDW8.last_valid_invtry_org_id = II.inventory_org_id CLUSTER BY EDW8.INSTANCE_ID,EDW8.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t10"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t10           SELECT
                    EDW9.instance_id,
                    EDW9.item_instance_id,
                    EDW9.active_end_date,
                    EDW9.active_start_date,
                    EDW9.actual_return_date,
                    EDW9.actual_ship_date,
                    EDW9.bill_to_site_use_id,
                    EDW9.bom_enabled_flag,
                    EDW9.country_code,
                    EDW9.crnt_loc_cs_fml_org_code,
                    EDW9.customer_po_date,
                    EDW9.customer_po_nbr,
                    EDW9.external_reference_nbr,
                    EDW9.install_date,
                    EDW9.install_loc_oper_unit_id,
                    EDW9.install_location_id,
                    EDW9.install_location_type_code,
                    EDW9.inventory_item_id,
                    EDW9.inventory_master_org_id,
                    EDW9.invtry_item_desc,
                    EDW9.invtry_item_flag,
                    II.invtry_item_sponsor_loc_id,
                    EDW9.invtry_item_status_code,
                    2 AS auto_c023,
                    EDW9.invtry_tran_code,
                    EDW9.invtry_uom_code,
                    EDW9.item_instance_status_desc,
                    EDW9.item_instance_status_id,
                    EDW9.item_instance_status_name,
                    EDW9.item_instance_tran_code,
                    EDW9.item_type_name,
                    EDW9.last_order_line_id,
                    EDW9.last_valid_invtry_org_id,
                    EDW9.location_id,
                    EDW9.location_type_code,
                    EDW9.offering_acctg_type_code,
                    EDW9.order_date,
                    EDW9.order_header_id,
                    EDW9.order_line_nbr,
                    EDW9.order_nbr,
                    EDW9.product_class,
                    EDW9.product_class_model,
                    EDW9.product_id,
                    EDW9.product_id_formatted,
                    EDW9.product_model,
                    II.product_source_org_id,
                    EDW9.product_submodel,
                    EDW9.return_by_date,
                    EDW9.serial_nbr_control_code,
                    EDW9.service_order_allowed_flag,
                    II.serviceable_product_flag,
                    EDW9.ship_to_site_use_id,
                    EDW9.shippable_item_flag,
                    EDW9.status_tran_code,
                    EDW9.terminated_flag,
                    EDW9.version_label_date,
                    EDW9.version_label_desc,
                    EDW9.version_label_name,
                    EDW9.vldtn_inventory_org_id,
                    EDW9.vrsn_lbl_tran_code,
                    EDW9.invtry_item_desc_unc,
                    EDW9.prev_site_nbr,
                    EDW9.service_tier_name,
                    EDW9.esd_flag,
                    EDW9.media_type,
                    EDW9.license_model,
                    EDW9.license_start_date,
                    EDW9.license_end_date,
                EDW9.cit_vendor_code,        
                EDW9.mdm_product_identifier, 
                EDW9.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t09    AS EDW9   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB1 + """.invtry_item    AS II    
                        ON EDW9.instance_id = II.instance_id  
                        AND EDW9.inventory_item_id = II.inventory_item_id   
                        AND EDW9.vldtn_inventory_org_id = II.inventory_org_id CLUSTER BY EDW9.INSTANCE_ID,EDW9.ITEM_INSTANCE_ID"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t11"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        df1_qq2=sparkSession.sql("""select II.nl_trackable_flag,II.instance_id,II.inventory_item_id from """ + db_params.EEDDWWDDBB1 + """.invtry_item    AS II INNER JOIN (select * from """ + db_params.EEDDWWDDBB2 + """.invtry_organization  where  upper(trim(invtry_org_name))  = 'MASTER ORGANIZATION')   AS IVO                 
           ON upper(trim(II.instance_id)) = upper(trim(IVO.instance_id))  AND II.inventory_org_id = IVO.inventory_org_id  """)
        df1_qq2.registerTempTable("NLFLAG"); 
        query = """INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t11           SELECT /*+ BROADCASTJOIN( NLFLAG ) */
                    EDW10.instance_id,
                    EDW10.item_instance_id,
                    EDW10.active_end_date,
                    EDW10.active_start_date,
                    EDW10.actual_return_date,
                    EDW10.actual_ship_date,
                    EDW10.bill_to_site_use_id,
                    EDW10.bom_enabled_flag,
                    EDW10.country_code,
                    EDW10.crnt_loc_cs_fml_org_code,
                    EDW10.customer_po_date,
                    EDW10.customer_po_nbr,
                    EDW10.external_reference_nbr,
                    EDW10.install_date,
                    EDW10.install_loc_oper_unit_id,
                    EDW10.install_location_id,
                    EDW10.install_location_type_code,
                    EDW10.inventory_item_id,
                    EDW10.inventory_master_org_id,
                    EDW10.invtry_item_desc,
                    EDW10.invtry_item_flag,
                    EDW10.invtry_item_sponsor_loc_id,
                    EDW10.invtry_item_status_code,
                    EDW10.invtry_pass_nbr,
                    EDW10.invtry_tran_code,
                    EDW10.invtry_uom_code,
                    EDW10.item_instance_status_desc,
                    EDW10.item_instance_status_id,
                    EDW10.item_instance_status_name,
                    EDW10.item_instance_tran_code,
                    EDW10.item_type_name,
                    EDW10.last_order_line_id,
                    EDW10.last_valid_invtry_org_id,
                    EDW10.location_id,
                    EDW10.location_type_code,
                    A.nl_trackable_flag,
                    EDW10.offering_acctg_type_code,
                    EDW10.order_date,
                    EDW10.order_header_id,
                    EDW10.order_line_nbr,
                    EDW10.order_nbr,
                    EDW10.product_class,
                    EDW10.product_class_model,
                    EDW10.product_id,
                    EDW10.product_id_formatted,
                    EDW10.product_model,
                    EDW10.product_source_org_id,
                    EDW10.product_submodel,
                    EDW10.return_by_date,
                    EDW10.serial_nbr_control_code,
                    EDW10.service_order_allowed_flag,
                    EDW10.serviceable_product_flag,
                    EDW10.ship_to_site_use_id,
                    EDW10.shippable_item_flag,
                    EDW10.status_tran_code,
                    EDW10.terminated_flag,
                    EDW10.version_label_date,
                    EDW10.version_label_desc,
                    EDW10.version_label_name,
                    EDW10.vldtn_inventory_org_id,
                    EDW10.vrsn_lbl_tran_code,
                    EDW10.invtry_item_desc_unc,
                    EDW10.prev_site_nbr,
                    EDW10.service_tier_name,
                    EDW10.esd_flag,
                    EDW10.media_type,
                    EDW10.license_model,
                    EDW10.license_start_date,
                    EDW10.license_end_date,
                    EDW10.cit_vendor_code,        
                    EDW10.mdm_product_identifier, 
                    EDW10.mdm_solution_identifier
                FROM
         """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t10    AS EDW10
         LEFt OUTER JOIN
         NLFLAG as A
                         ON EDW10.instance_id = A.instance_id  
                        AND EDW10.inventory_item_id = A.inventory_item_id   """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t12"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   CACHE TABLE INVTRY select * from """ + db_params.EEDDWWDDBB2 + """.invtry_organization where upper(trim(invtry_org_name)) = 'MASTER ORGANIZATION' """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t12           SELECT /*+ BROADCASTJOIN( INVTRY ) */
                    EDW11.instance_id,
                    EDW11.item_instance_id,
                    EDW11.active_end_date,
                    EDW11.active_start_date,
                    EDW11.actual_return_date,
                    EDW11.actual_ship_date,
                    EDW11.bill_to_site_use_id,
                    EDW11.bom_enabled_flag,
                    EDW11.country_code,
                    EDW11.crnt_loc_cs_fml_org_code,
                    EDW11.customer_po_date,
                    EDW11.customer_po_nbr,
                    EDW11.external_reference_nbr,
                    EDW11.install_date,
                    EDW11.install_loc_oper_unit_id,
                    EDW11.install_location_id,
                    EDW11.install_location_type_code,
                    EDW11.inventory_item_id,
                    EDW11.inventory_master_org_id,
                    IVO.inventory_org_id,
                    EDW11.invtry_item_desc,
                    EDW11.invtry_item_flag,
                    EDW11.invtry_item_sponsor_loc_id,
                    EDW11.invtry_item_status_code,
                    IVO.tran_code,
                    EDW11.invtry_pass_nbr,
                    EDW11.invtry_tran_code,
                    EDW11.invtry_uom_code,
                    EDW11.item_instance_status_desc,
                    EDW11.item_instance_status_id,
                    EDW11.item_instance_status_name,
                    EDW11.item_instance_tran_code,
                    EDW11.item_type_name,
                    EDW11.last_order_line_id,
                    EDW11.last_valid_invtry_org_id,
                    EDW11.location_id,
                    EDW11.location_type_code,
                    EDW11.nl_trackable_flag,
                    EDW11.offering_acctg_type_code,
                    EDW11.order_date,
                    EDW11.order_header_id,
                    EDW11.order_line_nbr,
                    EDW11.order_nbr,
                    EDW11.product_class,
                    EDW11.product_class_model,
                    EDW11.product_id,
                    EDW11.product_id_formatted,
                    EDW11.product_model,
                    EDW11.product_source_org_id,
                    EDW11.product_submodel,
                    EDW11.return_by_date,
                    EDW11.serial_nbr_control_code,
                    EDW11.service_order_allowed_flag,
                    EDW11.serviceable_product_flag,
                    EDW11.ship_to_site_use_id,
                    EDW11.shippable_item_flag,
                    EDW11.status_tran_code,
                    EDW11.terminated_flag,
                    EDW11.version_label_date,
                    EDW11.version_label_desc,
                    EDW11.version_label_name,
                    EDW11.vldtn_inventory_org_id,
                    EDW11.vrsn_lbl_tran_code,
                    EDW11.invtry_item_desc_unc,
                    EDW11.prev_site_nbr,
                    EDW11.service_tier_name,
                    EDW11.esd_flag,
                    EDW11.media_type,
                    EDW11.license_model,
                    EDW11.license_start_date,
                    EDW11.license_end_date,
                    EDW11.cit_vendor_code,        
                    EDW11.mdm_product_identifier, 
                    EDW11.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t11    AS EDW11   
                LEFT OUTER  JOIN
                    INVTRY    AS IVO  
                      ON EDW11.instance_id = IVO.instance_id CLUSTER BY EDW11.instance_id,EDW11.item_instance_id """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   UNCACHE TABLE INVTRY """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t13"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t13           SELECT
                    EDW12.instance_id,
                    EDW12.item_instance_id,
                    EDW12.active_end_date,
                    EDW12.active_start_date,
                    EDW12.actual_return_date,
                    EDW12.actual_ship_date,
                    EDW12.bill_to_site_use_id,
                    EDW12.bom_enabled_flag,
                    EDW12.country_code,
                    EDW12.crnt_loc_cs_fml_org_code,
                    EDW12.customer_po_date,
                    EDW12.customer_po_nbr,
                    EDW12.external_reference_nbr,
                    EDW12.install_date,
                    EDW12.install_loc_oper_unit_id,
                    EDW12.install_location_id,
                    EDW12.install_location_type_code,
                    EDW12.inventory_item_id,
                    EDW12.inventory_master_org_id,
                    EDW12.invtry_item_desc,
                    EDW12.invtry_item_flag,
                    EDW12.invtry_item_sponsor_loc_id,
                    EDW12.invtry_item_status_code,
                    EDW12.invtry_org_tran_code,
                    EDW12.invtry_pass_nbr,
                    EDW12.invtry_tran_code,
                    EDW12.invtry_uom_code,
                    EDW12.item_instance_status_desc,
                    EDW12.item_instance_status_id,
                    EDW12.item_instance_status_name,
                    EDW12.item_instance_tran_code,
                    EDW12.item_type_name,
                    EDW12.last_order_line_id,
                    EDW12.last_valid_invtry_org_id,
                    EDW12.location_id,
                    EDW12.location_type_code,
                    EDW12.nl_trackable_flag,
                    EDW12.offering_acctg_type_code,
                    IVO.operating_unit_id,
                    EDW12.order_date,
                    EDW12.order_header_id,
                    EDW12.order_line_nbr,
                    EDW12.order_nbr,
                    EDW12.product_class,
                    EDW12.product_class_model,
                    EDW12.product_id,
                    EDW12.product_id_formatted,
                    EDW12.product_model,
                    EDW12.product_source_org_id,
                    EDW12.product_submodel,
                    EDW12.return_by_date,
                    EDW12.serial_nbr_control_code,
                    EDW12.service_order_allowed_flag,
                    EDW12.serviceable_product_flag,
                    EDW12.ship_to_site_use_id,
                    EDW12.shippable_item_flag,
                    EDW12.status_tran_code,
                    EDW12.terminated_flag,
                    EDW12.version_label_date,
                    EDW12.version_label_desc,
                    EDW12.version_label_name,
                    EDW12.vldtn_inventory_org_id,
                    EDW12.vrsn_lbl_tran_code,
                    EDW12.invtry_item_desc_unc,
                    EDW12.prev_site_nbr,
                    EDW12.service_tier_name,
                    EDW12.esd_flag,
                    EDW12.media_type,
                    EDW12.license_model,
                    EDW12.license_start_date,
                    EDW12.license_end_date,
                EDW12.cit_vendor_code,        
                EDW12.mdm_product_identifier, 
                EDW12.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t12    AS EDW12   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.invtry_organization    AS IVO    
                        ON EDW12.instance_id = IVO.instance_id  
                        AND EDW12.last_valid_invtry_org_id = IVO.inventory_org_id CLUSTER BY EDW12.instance_id,EDW12.item_instance_id """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t14"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t14           SELECT /*+ BROADCASTJOIN( """ + db_params.EEDDWWDDBB2 + """.erp_operating_unit ) */
                    EDW13.instance_id,
                    EDW13.item_instance_id,
                    EDW13.active_end_date,
                    EDW13.active_start_date,
                    EDW13.actual_return_date,
                    EDW13.actual_ship_date,
                    EDW13.bill_to_site_use_id,
                    EDW13.bom_enabled_flag,
                    EDW13.country_code,
                    EDW13.crnt_loc_cs_fml_org_code,
                    EDW13.customer_po_date,
                    EDW13.customer_po_nbr,
                    EDW13.external_reference_nbr,
                    EDW13.install_date,
                    EDW13.install_loc_oper_unit_id,
                    EDW13.install_location_id,
                    EDW13.install_location_type_code,
                    EDW13.inventory_item_id,
                    EDW13.inventory_master_org_id,
                    EDW13.invtry_item_desc,
                    EDW13.invtry_item_flag,
                    EDW13.invtry_item_sponsor_loc_id,
                    EDW13.invtry_item_status_code,
                    EDW13.invtry_org_tran_code,
                    EDW13.invtry_pass_nbr,
                    EDW13.invtry_tran_code,
                    EDW13.invtry_uom_code,
                    EDW13.item_instance_status_desc,
                    EDW13.item_instance_status_id,
                    EDW13.item_instance_status_name,
                    EDW13.item_instance_tran_code,
                    EDW13.item_type_name,
                    EDW13.last_order_line_id,
                    EDW13.last_valid_invtry_org_id,
                    EDW13.location_id,
                    EDW13.location_type_code,
                    EDW13.nl_trackable_flag,
                    EDW13.offering_acctg_type_code,
                    EOU.oper_unit_country_code,
                    EDW13.operating_unit_id,
                    EOU.operating_unit_name,
                    EDW13.order_date,
                    EDW13.order_header_id,
                    EDW13.order_line_nbr,
                    EDW13.order_nbr,
                    EDW13.product_class,
                    EDW13.product_class_model,
                    EDW13.product_id,
                    EDW13.product_id_formatted,
                    EDW13.product_model,
                    EDW13.product_source_org_id,
                    EDW13.product_submodel,
                    EDW13.return_by_date,
                    EDW13.serial_nbr_control_code,
                    EDW13.service_order_allowed_flag,
                    EDW13.serviceable_product_flag,
                    EDW13.ship_to_site_use_id,
                    EDW13.shippable_item_flag,
                    EDW13.status_tran_code,
                    EDW13.terminated_flag,
                    EDW13.version_label_date,
                    EDW13.version_label_desc,
                    EDW13.version_label_name,
                    EDW13.vldtn_inventory_org_id,
                    EDW13.vrsn_lbl_tran_code,
                    EDW13.invtry_item_desc_unc,
                    EDW13.prev_site_nbr,
                    EDW13.service_tier_name,
                    EDW13.esd_flag,
                    EDW13.media_type,
                    EDW13.license_model,
                    EDW13.license_start_date,
                    EDW13.license_end_date,
                EDW13.cit_vendor_code,        
                EDW13.mdm_product_identifier, 
                EDW13.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t13    AS EDW13   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.erp_operating_unit    AS EOU    
                        ON EDW13.instance_id = EOU.instance_id  
                        AND EDW13.operating_unit_id = EOU.operating_unit_id CLUSTER BY EDW13.instance_id,EDW13.item_instance_id """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t15a"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t15a           SELECT
                    instance_id,
                    inventory_org_id,
                    gsdb_org_code  
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.invtry_mtl_parameter     
                WHERE
                    gsdb_org_code  IS NOT NULL  
                GROUP BY
                    instance_id ,
                    inventory_org_id ,
                    gsdb_org_code"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t15a           SELECT
                    IMP1.instance_id,
                    IMP1.inventory_org_id,
                    IMP2.gsdb_org_code  
                FROM
                    (select * from """ + db_params.EEDDWWDDBB2 + """.invtry_mtl_parameter where gsdb_org_code  IS NULL)    AS IMP1   ,
                    """ + db_params.EEDDWWDDBB2 + """.invtry_mtl_parameter    AS IMP2   
                WHERE
                    upper(trim(IMP1.instance_id)) = upper(trim(IMP2.instance_id))  
                    AND IMP1.inventory_org_id = cast(IMP2.related_gsl_sdc_org_id as decimal(18,0))   
                GROUP BY
                    IMP1.instance_id ,
                    IMP1.inventory_org_id ,
                    IMP2.gsdb_org_code"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t15b"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t15b           SELECT
                    EDW14.instance_id,
                    EDW14.item_instance_id,
                    EDW14.active_end_date,
                    EDW14.active_start_date,
                    EDW14.actual_return_date,
                    EDW14.actual_ship_date,
                    EDW14.bill_to_site_use_id,
                    EDW14.bom_enabled_flag,
                    EDW14.country_code,
                    EDW14.crnt_loc_cs_fml_org_code,
                    EDW14.customer_po_date,
                    EDW14.customer_po_nbr,
                    EDW14.external_reference_nbr,
                    EDW15A.gsdb_org_code,
                    EDW14.install_date,
                    EDW14.install_loc_oper_unit_id,
                    EDW14.install_location_id,
                    EDW14.install_location_type_code,
                    EDW14.inventory_item_id,
                    EDW14.inventory_master_org_id,
                    EDW15a.inventory_org_id,
                    EDW14.invtry_item_desc,
                    EDW14.invtry_item_flag,
                    EDW14.invtry_item_sponsor_loc_id,
                    EDW14.invtry_item_status_code,
                    EDW14.invtry_org_tran_code,
                    EDW14.invtry_pass_nbr,
                    EDW14.invtry_tran_code,
                    EDW14.invtry_uom_code,
                    EDW14.item_instance_status_desc,
                    EDW14.item_instance_status_id,
                    EDW14.item_instance_status_name,
                    EDW14.item_instance_tran_code,
                    EDW14.item_type_name,
                    EDW14.last_order_line_id,
                    EDW14.last_valid_invtry_org_id,
                    EDW14.location_id,
                    EDW14.location_type_code,
                    EDW14.nl_trackable_flag,
                    EDW14.offering_acctg_type_code,
                    EDW14.oper_unit_country_code,
                    EDW14.operating_unit_id,
                    EDW14.operating_unit_name,
                    EDW14.order_date,
                    EDW14.order_header_id,
                    EDW14.order_line_nbr,
                    EDW14.order_nbr,
                    EDW14.product_class,
                    EDW14.product_class_model,
                    EDW14.product_id,
                    EDW14.product_id_formatted,
                    EDW14.product_model,
                    EDW14.product_source_org_id,
                    EDW14.product_submodel,
                    EDW14.return_by_date,
                    EDW14.serial_nbr_control_code,
                    EDW14.service_order_allowed_flag,
                    EDW14.serviceable_product_flag,
                    EDW14.ship_to_site_use_id,
                    EDW14.shippable_item_flag,
                    EDW14.status_tran_code,
                    EDW14.terminated_flag,
                    EDW14.version_label_date,
                    EDW14.version_label_desc,
                    EDW14.version_label_name,
                    EDW14.vldtn_inventory_org_id,
                    EDW14.vrsn_lbl_tran_code,
                    EDW14.invtry_item_desc_unc,
                    EDW14.prev_site_nbr,
                    EDW14.service_tier_name,
                    EDW14.esd_flag,
                    EDW14.media_type,
                    EDW14.license_model,
                    EDW14.license_start_date,
                    EDW14.license_end_date,
                EDW14.cit_vendor_code,        
                EDW14.mdm_product_identifier, 
                EDW14.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t14    AS EDW14   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t15a    AS EDW15A    
                        ON EDW14.instance_id = EDW15A.instance_id  
                        AND EDW14.last_valid_invtry_org_id = EDW15A.inventory_org_id CLUSTER BY EDW14.instance_id,EDW14.item_instance_id """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t16"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t15b COMPUTE STATISTICS  FOR COLUMNS product_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   CACHE TABLE product select * from """ + db_params.EEDDWWDDBB2 + """.product  """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t16           SELECT /*+ BROADCASTJOIN( product ) */  
                    EDW15B.instance_id,
                    EDW15B.item_instance_id,
                    EDW15B.active_end_date,
                    EDW15B.active_start_date,
                    EDW15B.actual_return_date,
                    EDW15B.actual_ship_date,
                    EDW15B.bill_to_site_use_id,
                    EDW15B.bom_enabled_flag,
                    EDW15B.country_code,
                    EDW15B.crnt_loc_cs_fml_org_code,
                    EDW15B.customer_po_date,
                    EDW15B.customer_po_nbr,
                    ( case when EDW15B.product_id = PROD.product_id then PROD.service_product_id else null end),
                    EDW15B.external_reference_nbr,
                    EDW15B.gsdb_org_code,
                    EDW15B.install_date,
                    EDW15B.install_loc_oper_unit_id,
                    EDW15B.install_location_id,
                    EDW15B.install_location_type_code,
                    EDW15B.inventory_item_id,
                    EDW15B.inventory_master_org_id,
                    EDW15B.inventory_org_id,
                    EDW15B.invtry_item_desc,
                    EDW15B.invtry_item_flag,
                    EDW15B.invtry_item_sponsor_loc_id,
                    EDW15B.invtry_item_status_code,
                    EDW15B.invtry_org_tran_code,
                    EDW15B.invtry_pass_nbr,
                    EDW15B.invtry_tran_code,
                    EDW15B.invtry_uom_code,
                    EDW15B.item_instance_status_desc,
                    EDW15B.item_instance_status_id,
                    EDW15B.item_instance_status_name,
                    EDW15B.item_instance_tran_code,
                    EDW15B.item_type_name,
                    EDW15B.last_order_line_id,
                    EDW15B.last_valid_invtry_org_id,
                    EDW15B.location_id,
                    EDW15B.location_type_code,
                    EDW15B.nl_trackable_flag,
                    EDW15B.offering_acctg_type_code,
                    EDW15B.oper_unit_country_code,
                    EDW15B.operating_unit_id,
                    EDW15B.operating_unit_name,
                    EDW15B.order_date,
                    EDW15B.order_header_id,
                    EDW15B.order_line_nbr,
                    EDW15B.order_nbr,
                    EDW15B.product_class,
                    EDW15B.product_class_model,
                    EDW15B.product_id,
                    EDW15B.product_id_formatted,
                    EDW15B.product_model,
                    EDW15B.product_source_org_id,
                    EDW15B.product_submodel,
                    EDW15B.return_by_date,
                    ( case when EDW15B.product_id = PROD.product_id then PROD.sal_code else null end),
                    EDW15B.serial_nbr_control_code,
                    EDW15B.service_order_allowed_flag,
                    EDW15B.serviceable_product_flag,
                    EDW15B.ship_to_site_use_id,
                    EDW15B.shippable_item_flag,
                    EDW15B.status_tran_code,
                    EDW15B.terminated_flag,
                    EDW15B.version_label_date,
                    EDW15B.version_label_desc,
                    EDW15B.version_label_name,
                    EDW15B.vldtn_inventory_org_id,
                    EDW15B.vrsn_lbl_tran_code,
                    EDW15B.invtry_item_desc_unc,
                    EDW15B.prev_site_nbr,
                    EDW15B.service_tier_name,
                    EDW15B.esd_flag,
                    EDW15B.media_type,
                    EDW15B.license_model,
                    EDW15B.license_start_date,
                    EDW15B.license_end_date,
                EDW15B.cit_vendor_code,        
                EDW15B.mdm_product_identifier, 
                EDW15B.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t15b    AS EDW15B   
                LEFT OUTER  JOIN
                    product    AS PROD    
                        ON EDW15B.product_id = PROD.product_id CLUSTER BY EDW15B.instance_id,EDW15B.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t17"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t17           SELECT /*+ BROADCASTJOIN( """ + db_params.EEDDWWDDBB2 + """.product_order_org ) */
                   distinct
                    EDW16.instance_id,
                    EDW16.item_instance_id,
                    EDW16.active_end_date,
                    EDW16.active_start_date,
                    EDW16.actual_return_date,
                    EDW16.actual_ship_date,
                    EDW16.bill_to_site_use_id,
                    EDW16.bom_enabled_flag,
                    EDW16.country_code,
                    EDW16.crnt_loc_cs_fml_org_code,
                    EDW16.customer_po_date,
                    EDW16.customer_po_nbr,
                    EDW16.dflt_service_product_id,
                    ( case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.warranty_coverage_code else null end),
                    (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.warranty_product_id else null end),
                    (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.warranty_term_mth_cnt else null end),
                    EDW16.external_reference_nbr,
                    EDW16.gsdb_org_code,
                    EDW16.install_date,
                    EDW16.install_loc_oper_unit_id,
                    EDW16.install_location_id,
                    EDW16.install_location_type_code,
                    EDW16.inventory_item_id,
                    EDW16.inventory_master_org_id,
                    EDW16.inventory_org_id,
                    EDW16.invtry_item_desc,
                    EDW16.invtry_item_flag,
                    EDW16.invtry_item_sponsor_loc_id,
                    EDW16.invtry_item_status_code,
                    EDW16.invtry_org_tran_code,
                    EDW16.invtry_pass_nbr,
                    EDW16.invtry_tran_code,
                    EDW16.invtry_uom_code,
                    EDW16.item_instance_status_desc,
                    EDW16.item_instance_status_id,
                    EDW16.item_instance_status_name,
                    EDW16.item_instance_tran_code,
                    EDW16.item_type_name,
                    EDW16.last_order_line_id,
                    EDW16.last_valid_invtry_org_id,
                    EDW16.location_id,
                    EDW16.location_type_code,
                    EDW16.nl_trackable_flag,
                    EDW16.offering_acctg_type_code,
                    EDW16.oper_unit_country_code,
                    EDW16.operating_unit_id,
                    EDW16.operating_unit_name,
                    EDW16.order_date,
                    EDW16.order_header_id,
                    EDW16.order_line_nbr,
                    EDW16.order_nbr,
                    (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.actual_order_end_date else null end),
                    (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.actual_order_start_date else null end),
                    (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.actual_svc_end_date else null end),
                    (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.actual_svc_start_date else null end),
                    EDW16.product_class,
                    EDW16.product_class_model,
                    EDW16.product_id,
                    EDW16.product_id_formatted,
                    EDW16.product_model,
                    EDW16.product_source_org_id,
                    EDW16.product_submodel,
                    EDW16.return_by_date,
                    null,
                    EDW16.serial_nbr_control_code,
                    EDW16.service_order_allowed_flag,
                    EDW16.serviceable_product_flag,
                    EDW16.ship_to_site_use_id,
                    EDW16.shippable_item_flag,
                    EDW16.status_tran_code,
                    EDW16.terminated_flag,
                    EDW16.version_label_date,
                    EDW16.version_label_desc,
                    EDW16.version_label_name,
                    EDW16.vldtn_inventory_org_id,
                    EDW16.vrsn_lbl_tran_code,
                    EDW16.invtry_item_desc_unc,
                    EDW16.prev_site_nbr,
                    EDW16.service_tier_name,
                    EDW16.esd_flag,
                    EDW16.media_type,
                    EDW16.license_model,
                    EDW16.license_start_date,
                    EDW16.license_end_date,
                EDW16.cit_vendor_code,
                    EDW16.mdm_product_identifier,
                    EDW16.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t16    AS EDW16 LEFT OUTER JOIN
                    """ + db_params.EEDDWWDDBB2 + """.product_order_org    AS POO   
                on
                    EDW16.gsdb_org_code = POO.ordering_org_code  
                    AND EDW16.product_id = POO.product_id  CLUSTER BY EDW16.INSTANCE_ID,EDW16.ITEM_INSTANCE_ID"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t18"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t17 COMPUTE STATISTICS  FOR COLUMNS gsdb_org_code"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t17 COMPUTE STATISTICS  FOR COLUMNS dflt_service_product_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t18           SELECT /*+ BROADCASTJOIN( """ + db_params.EEDDWWDDBB2 + """.product_order_org ) */ distinct
                    EDW17.instance_id,
                    EDW17.item_instance_id,
                    EDW17.active_end_date,
                    EDW17.active_start_date,
                    EDW17.actual_return_date,
                    EDW17.actual_ship_date,
                    EDW17.bill_to_site_use_id,
                    EDW17.bom_enabled_flag,
                    EDW17.country_code,
                    EDW17.crnt_loc_cs_fml_org_code,
                    EDW17.customer_po_date,
                    EDW17.customer_po_nbr,
                    POO.service_coverage_code AS dflt_service_coverage_code,
                    null,
                    POO.service_duration_cnt AS dflt_service_duration_cnt,
                    EDW17.dflt_service_product_id,
                    EDW17.dflt_wrnty_coverage_code,
                    EDW17.dflt_wrnty_product_id,
                    EDW17.dflt_wrnty_term_mth_cnt,
                    EDW17.external_reference_nbr,
                    EDW17.gsdb_org_code,
                    EDW17.install_date,
                    EDW17.install_loc_oper_unit_id,
                    EDW17.install_location_id,
                    EDW17.install_location_type_code,
                    EDW17.inventory_item_id,
                    EDW17.inventory_master_org_id,
                    EDW17.inventory_org_id,
                    EDW17.invtry_item_desc,
                    EDW17.invtry_item_flag,
                    EDW17.invtry_item_sponsor_loc_id,
                    EDW17.invtry_item_status_code,
                    EDW17.invtry_org_tran_code,
                    EDW17.invtry_pass_nbr,
                    EDW17.invtry_tran_code,
                    EDW17.invtry_uom_code,
                    EDW17.item_instance_status_desc,
                    EDW17.item_instance_status_id,
                    EDW17.item_instance_status_name,
                    EDW17.item_instance_tran_code,
                    EDW17.item_type_name,
                    EDW17.last_order_line_id,
                    EDW17.last_valid_invtry_org_id,
                    EDW17.location_id,
                    EDW17.location_type_code,
                    EDW17.nl_trackable_flag,
                    EDW17.offering_acctg_type_code,
                    EDW17.oper_unit_country_code,
                    EDW17.operating_unit_id,
                    EDW17.operating_unit_name,
                    EDW17.order_date,
                    EDW17.order_header_id,
                    EDW17.order_line_nbr,
                    EDW17.order_nbr,
                    EDW17.prod_act_order_end_date,
                    EDW17.prod_act_order_start_date,
                    EDW17.prod_act_svc_end_date,
                    EDW17.prod_act_svc_start_date,
                    EDW17.product_class,
                    EDW17.product_class_model,
                    EDW17.product_id,
                    EDW17.product_id_formatted,
                    EDW17.product_model,
                    EDW17.product_source_org_id,
                    EDW17.product_submodel,
                    EDW17.return_by_date,
                    null,
                    EDW17.serial_nbr_control_code,
                    EDW17.service_order_allowed_flag,
                    EDW17.serviceable_product_flag,
                    EDW17.ship_to_site_use_id,
                    EDW17.shippable_item_flag,
                    EDW17.status_tran_code,
                    POO.actual_order_end_date AS svc_act_order_end_date,
                    POO.actual_order_start_date AS svc_act_order_start_date,
                    EDW17.terminated_flag,
                    EDW17.version_label_date,
                    EDW17.version_label_desc,
                    EDW17.version_label_name,
                    EDW17.vldtn_inventory_org_id,
                    EDW17.vrsn_lbl_tran_code,
                    EDW17.invtry_item_desc_unc,
                    EDW17.prev_site_nbr,
                    EDW17.service_tier_name,
                    EDW17.esd_flag,
                    EDW17.media_type,
                    EDW17.license_model,
                    EDW17.license_start_date,
                    EDW17.license_end_date,
                    EDW17.cit_vendor_code,
                    EDW17.mdm_product_identifier,
                    EDW17.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t17    AS EDW17   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.product_order_org    AS POO    
                        ON EDW17.gsdb_org_code = POO.ordering_org_code  
                        AND EDW17.dflt_service_product_id = POO.product_id Where EDW17.gsdb_org_code is not null and EDW17.dflt_service_product_id is not null CLUSTER BY EDW17.INSTANCE_ID,EDW17.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t18           SELECT 
                    EDW17.instance_id,
                    EDW17.item_instance_id,
                    EDW17.active_end_date,
                    EDW17.active_start_date,
                    EDW17.actual_return_date,
                    EDW17.actual_ship_date,
                    EDW17.bill_to_site_use_id,
                    EDW17.bom_enabled_flag,
                    EDW17.country_code,
                    EDW17.crnt_loc_cs_fml_org_code,
                    EDW17.customer_po_date,
                    EDW17.customer_po_nbr,
                    null AS dflt_service_coverage_code,
                    null,
                    null AS dflt_service_duration_cnt,
                    EDW17.dflt_service_product_id,
                    EDW17.dflt_wrnty_coverage_code,
                    EDW17.dflt_wrnty_product_id,
                    EDW17.dflt_wrnty_term_mth_cnt,
                    EDW17.external_reference_nbr,
                    EDW17.gsdb_org_code,
                    EDW17.install_date,
                    EDW17.install_loc_oper_unit_id,
                    EDW17.install_location_id,
                    EDW17.install_location_type_code,
                    EDW17.inventory_item_id,
                    EDW17.inventory_master_org_id,
                    EDW17.inventory_org_id,
                    EDW17.invtry_item_desc,
                    EDW17.invtry_item_flag,
                    EDW17.invtry_item_sponsor_loc_id,
                    EDW17.invtry_item_status_code,
                    EDW17.invtry_org_tran_code,
                    EDW17.invtry_pass_nbr,
                    EDW17.invtry_tran_code,
                    EDW17.invtry_uom_code,
                    EDW17.item_instance_status_desc,
                    EDW17.item_instance_status_id,
                    EDW17.item_instance_status_name,
                    EDW17.item_instance_tran_code,
                    EDW17.item_type_name,
                    EDW17.last_order_line_id,
                    EDW17.last_valid_invtry_org_id,
                    EDW17.location_id,
                    EDW17.location_type_code,
                    EDW17.nl_trackable_flag,
                    EDW17.offering_acctg_type_code,
                    EDW17.oper_unit_country_code,
                    EDW17.operating_unit_id,
                    EDW17.operating_unit_name,
                    EDW17.order_date,
                    EDW17.order_header_id,
                    EDW17.order_line_nbr,
                    EDW17.order_nbr,
                    EDW17.prod_act_order_end_date,
                    EDW17.prod_act_order_start_date,
                    EDW17.prod_act_svc_end_date,
                    EDW17.prod_act_svc_start_date,
                    EDW17.product_class,
                    EDW17.product_class_model,
                    EDW17.product_id,
                    EDW17.product_id_formatted,
                    EDW17.product_model,
                    EDW17.product_source_org_id,
                    EDW17.product_submodel,
                    EDW17.return_by_date,
                    null,
                    EDW17.serial_nbr_control_code,
                    EDW17.service_order_allowed_flag,
                    EDW17.serviceable_product_flag,
                    EDW17.ship_to_site_use_id,
                    EDW17.shippable_item_flag,
                    EDW17.status_tran_code,
                    null AS svc_act_order_end_date,
                    null AS svc_act_order_start_date,
                    EDW17.terminated_flag,
                    EDW17.version_label_date,
                    EDW17.version_label_desc,
                    EDW17.version_label_name,
                    EDW17.vldtn_inventory_org_id,
                    EDW17.vrsn_lbl_tran_code,
                    EDW17.invtry_item_desc_unc,
                    EDW17.prev_site_nbr,
                    EDW17.service_tier_name,
                    EDW17.esd_flag,
                    EDW17.media_type,
                    EDW17.license_model,
                    EDW17.license_start_date,
                    EDW17.license_end_date,
                    EDW17.cit_vendor_code,
                    EDW17.mdm_product_identifier,
                    EDW17.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t17    AS EDW17   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t18    AS EDW18    
                        ON EDW17.ITEM_INSTANCE_ID = EDW18.ITEM_INSTANCE_ID  
                        AND EDW17.INSTANCE_ID = EDW18.INSTANCE_ID Where EDW18.ITEM_INSTANCE_ID is  null and EDW18.INSTANCE_ID is null CLUSTER BY EDW17.instance_id,EDW17.item_instance_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t19"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t18 COMPUTE STATISTICS  FOR COLUMNS dflt_wrnty_product_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        
        
def main():
    m = D8Installbasednld25mergedpy(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print (e)
        print 'Error in D8Installbasednld25mergedpy'
        sparkSession.stop()
        sys.exit(1)



if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8Installbasednld25mergedpy").enableHiveSupport().getOrCreate()
    main()
    sparkSession.stop()
    sys.exit(0)








