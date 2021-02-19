from pyspark.sql import SparkSession
from datetime import datetime 
import sys 
import db_params

class D8Installbasedn26ldpy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t26"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   CACHE TABLE GEO select * from """ + db_params.EEDDWWDDBB2 + """.geo_rollup_xref where upper(trim(business_unit_code)) = 'B4' """
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
            INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t26           SELECT /*+ BROADCASTJOIN( GEO ) */
                    EDW25.instance_id,
                    EDW25.item_instance_id,
                    EDW25.active_end_date,
                    EDW25.active_start_date,
                    EDW25.actual_return_date,
                    EDW25.actual_ship_date,
                    EDW25.area_code,
                    EDW25.area_desc,
                    EDW25.bill_to_site_use_id,
                    EDW25.bom_enabled_flag,
                    EDW7.cg_tran_code,
                    EDW25.country_code,
                    EDW25.country_desc,
                    EDW25.crnt_loc_cs_fml_org_code,
                    EDW25.customer_po_date,
                    EDW25.customer_po_nbr,
                    EDW25.dflt_service_coverage_code,
                    EDW25.dflt_service_duration,
                    EDW25.dflt_service_duration_cnt,
                    EDW25.dflt_service_product_id,
                    EDW25.dflt_wrnty_coverage_code,
                    EDW25.dflt_wrnty_product_desc,
                    EDW25.dflt_wrnty_product_id,
                    EDW25.dflt_wrnty_term_mth_cnt,
                    EDW25.external_reference_nbr,
                    EDW25.func_curr_code,
                    EDW25.gsdb_org_code,
                    EDW25.install_date,
                    EDW25.install_loc_oper_unit_id,
                    EDW25.install_location_id,
                    EDW25.install_location_type_code,
                    EDW25.inventory_item_id,
                    EDW25.inventory_master_org_id,
                    EDW25.inventory_org_id,
                    EDW25.invtry_item_desc,
                    EDW25.invtry_item_flag,
                    EDW25.invtry_item_sponsor_loc_id,
                    EDW25.invtry_item_status_code,
                    EDW25.invtry_org_tran_code,
                    EDW25.invtry_pass_nbr,
                    EDW25.invtry_tran_code,
                    EDW25.invtry_uom_code,
                    EDW7.item_category_code,
                    EDW7.item_category_desc,
                    EDW25.item_instance_status_desc,
                    EDW25.item_instance_status_id,
                    EDW25.item_instance_status_name,
                    EDW25.item_instance_tran_code,
                    EDW25.item_type_name,
                    EDW25.last_order_line_id,
                    EDW25.last_valid_invtry_org_id,
                    EDW25.location_id,
                    EDW25.location_type_code,
                    EDW25.nl_trackable_flag,
                    EDW25.offering_acctg_type_code,
                    EDW25.oper_unit_country_code,
                    EDW25.operating_unit_id,
                    EDW25.operating_unit_name,
                    EDW25.order_date,
                    EDW25.order_header_id,
                    EDW25.order_line_nbr,
                    EDW25.order_nbr,
                    EDW25.prod_act_order_end_date,
                    EDW25.prod_act_order_start_date,
                    EDW25.prod_act_svc_end_date,
                    EDW25.prod_act_svc_start_date,
                    EDW7.pl_tran_code,
                    EDW7.product_category_code,
                    EDW7.product_category_desc,
                    EDW25.product_class,
                    EDW25.product_class_model,
                    EDW7.product_group_name,
                    EDW7.product_group_nbr,
                    EDW25.product_id,
                    EDW25.product_id_formatted,
                    EDW7.product_line_name,
                    EDW7.product_line_nbr,
                    EDW25.product_model,
                    EDW25.product_source_org_id,
                    EDW25.product_submodel,
                    EDW7.product_type_code,
                    EDW7.product_type_name,
                    EDW25.region_code,
                    EDW25.region_desc,
                    EDW25.return_by_date,
                    null,
                    EDW25.serial_nbr_control_code,
                    EDW25.service_order_allowed_flag,
                    EDW25.serviceable_product_flag,
                    EDW25.ship_to_site_use_id,
                    EDW25.shippable_item_flag,
                    EDW25.status_tran_code,
                    EDW25.svc_act_order_end_date,
                    EDW25.svc_act_order_start_date,
                    EDW25.svc_bmr_amt_ent,
                    EDW25.svc_bmr_amt_euro,
                    EDW25.svc_bmr_amt_func,
                    EDW25.svc_bmr_amt_us,
                    EDW25.svc_bmr_curr_code,
                    EDW25.terminated_flag,
                    EDW25.version_label_date,
                    EDW25.version_label_desc,
                    EDW25.version_label_name,
                    EDW25.vldtn_inventory_org_id,
                    EDW25.vrsn_lbl_tran_code,
                    EDW8.sub_region_code,
                    EDW8.sub_region_name,
                    EDW25.invtry_item_desc_unc,
                    EDW25.prev_site_nbr,
                    EDW25.service_tier_name,
                    EDW25.esd_flag,
                    EDW25.media_type,
                    EDW25.license_model,
                    EDW25.license_start_date,
                    EDW25.license_end_date,
                EDW25.cit_vendor_code,        
                EDW25.mdm_product_identifier, 
                EDW25.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t25    AS EDW25   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_plg_dn_t07    AS EDW7    
                        ON EDW25.instance_id = EDW7.instance_id  
                        AND EDW25.inventory_item_id = EDW7.inventory_item_id   
                        AND EDW25.last_valid_invtry_org_id = EDW7.inventory_source_code    
                LEFT OUTER  JOIN
                    GEO    AS EDW8    
                        ON EDW25.country_code = EDW8.country_code  
                        CLUSTER BY EDW25.INSTANCE_ID,EDW25.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   UNCACHE TABLE GEO  """
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """     TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t27"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """DROP TABLE IF EXISTS """ + db_params.TTMMPPDDBB1 + """.d8t_install_base_HISTTTMP"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """  create table """ + db_params.TTMMPPDDBB1 + """.d8t_install_base_HISTTTMP as select    item_instance_id,instance_id,instance_history_id,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name from (select item_instance_id,instance_id,instance_history_id,row_number() over (partition by instance_id,item_instance_id order by instance_history_id desc) as row_num,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name from  """ +   db_params.EEDDWWDDBB1 + """.install_base_item_hist_ld )X  where  x.row_num =1 """
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t27 SELECT  DISTINCT    EDW26.instance_id ,
               EDW26.item_instance_id ,
               EDW26.active_end_date ,
               EDW26.active_start_date ,
               EDW26.actual_return_date ,
               EDW26.actual_ship_date ,
               COALESCE(EDW26.area_code, '  ') ,
               COALESCE(EDW26.area_desc, ' ') ,
               COALESCE(EDW26.bill_to_site_use_id, 0) ,
               COALESCE(EDW26.bom_enabled_flag, ' ') ,
               COALESCE(EDW26.cat_tran_code, ' ') ,
               COALESCE(EDW26.country_code, '  ') ,
               COALESCE(EDW26.country_desc, ' ') ,
               COALESCE(EDW26.crnt_loc_cs_fml_org_code, ' ') ,
               COALESCE(EDW26.customer_po_date, ' ') ,
               COALESCE(EDW26.customer_po_nbr, ' ') ,
               COALESCE(EDW26.dflt_service_coverage_code, ' ') ,
               COALESCE(EDW26.dflt_service_duration, ' ') ,
               COALESCE(EDW26.dflt_service_duration_cnt, 0) ,
               COALESCE(EDW26.dflt_service_product_id, ' ') ,
               COALESCE(EDW26.dflt_wrnty_coverage_code, ' ') ,
               COALESCE(EDW26.dflt_wrnty_product_desc, ' ') ,
               COALESCE(EDW26.dflt_wrnty_product_id, ' ') ,
               COALESCE(EDW26.dflt_wrnty_term_mth_cnt, 0) ,
               CASE
                   WHEN SUBSTR(EDW26.product_id, 5, 1) IN ('F','K') THEN 0
                   ELSE 1
               END ,
               CASE
                   WHEN ESC.prod_class_code IS NOT NULL THEN 1
                   ELSE 0
               END ,
               COALESCE(EDW26.external_reference_nbr, ' ') ,
               COALESCE(EDW26.func_curr_code, '    ') ,
               IBI.future_move_concat_value ,
               COALESCE(EDW26.gsdb_org_code, ' ') ,
               EDW26.install_date ,
               COALESCE(EDW26.install_loc_oper_unit_id, 0) ,
               COALESCE(EDW26.install_location_id, 0) ,
               COALESCE(EDW26.install_location_type_code, ' ') ,
               COALESCE(EDW26.inventory_item_id, 0) ,
               COALESCE(EDW26.inventory_master_org_id, 0) ,
               COALESCE(EDW26.inventory_org_id, 0) ,
               COALESCE(EDW26.invtry_item_desc, ' ') ,
               COALESCE(EDW26.invtry_item_flag, ' ') ,
               COALESCE(EDW26.invtry_item_sponsor_loc_id, ' ') ,
               COALESCE(EDW26.invtry_item_status_code, ' ') ,
               COALESCE(EDW26.invtry_org_tran_code, ' ') ,
               COALESCE(EDW26.invtry_pass_nbr, 0) ,
               COALESCE(EDW26.invtry_tran_code, ' ') ,
               COALESCE(EDW26.invtry_uom_code, ' ') ,
               COALESCE(EDW26.item_category_code, ' ') ,
               COALESCE(EDW26.item_category_desc, ' ') ,
               COALESCE(EDW26.item_instance_status_desc, ' ') ,
               COALESCE(EDW26.item_instance_status_id, 0) ,
               COALESCE(EDW26.item_instance_status_name, ' ') ,
               COALESCE(EDW26.item_instance_tran_code, ' ') ,
               COALESCE(EDW26.item_type_name, ' ') ,
               COALESCE(EDW26.last_order_line_id, 0) ,
               COALESCE(EDW26.last_valid_invtry_org_id, 0) ,
               COALESCE(EDW26.location_id, 0) ,
               COALESCE(EDW26.location_type_code, ' ') ,
               COALESCE(EDW26.nl_trackable_flag, ' ') ,
               COALESCE(EDW26.offering_acctg_type_code, ' ') ,
               IBI.om_invoice_date ,
               COALESCE(EDW26.oper_unit_country_code, ' ') ,
               COALESCE(EDW26.operating_unit_id, 0) ,
               COALESCE(EDW26.operating_unit_name, ' ') ,
               EDW26.order_date ,
               COALESCE(EDW26.order_header_id, 0) ,
               COALESCE(EDW26.order_line_nbr, 0) ,
               COALESCE(EDW26.order_nbr, 0) ,
               IBI.previous_prod_id ,
               IBI.previous_prod_id_formatted ,
               EDW26.prod_act_order_end_date ,
               EDW26.prod_act_order_start_date ,
               EDW26.prod_act_svc_end_date ,
               EDW26.prod_act_svc_start_date ,
               COALESCE(EDW26.prod_line_tran_code, ' ') ,
               COALESCE(EDW26.product_category_code, ' ') ,
               COALESCE(EDW26.product_category_desc, ' ') ,
               COALESCE(EDW26.product_class, ' ') ,
               COALESCE(EDW26.product_class_model, ' ') ,
               COALESCE(EDW26.product_group_name, ' ') ,
               COALESCE(EDW26.product_group_nbr, 0) ,
               COALESCE(EDW26.product_id, ' ') ,
               COALESCE(EDW26.product_id_formatted, ' ') ,
               IBIH.product_id_swap_date ,
               IBIH.product_id_swap_time ,
               COALESCE(EDW26.product_line_name, ' ') ,
               COALESCE(EDW26.product_line_nbr, 0) ,
               COALESCE(EDW26.product_model, ' ') ,
               COALESCE(EDW26.product_source_org_id, ' ') ,
               COALESCE(EDW26.product_submodel, ' ') ,
               COALESCE(EDW26.product_type_code, ' ') ,
               COALESCE(EDW26.product_type_name, ' ') ,
               COALESCE(EDW26.region_code, '  ') ,
               COALESCE(EDW26.region_desc, ' ') ,
               EDW26.return_by_date ,
               COALESCE(EDW26.sal_code, ' ') ,
               COALESCE(EDW26.serial_nbr_control_code, 0) ,
               COALESCE(EDW26.service_order_allowed_flag, ' ') ,
               COALESCE(EDW26.serviceable_product_flag, ' ') ,
               COALESCE(EDW26.ship_to_site_use_id, 0) ,
               COALESCE(EDW26.shippable_item_flag, ' ') ,
               COALESCE(EDW26.status_tran_code, ' ') ,
               EDW26.svc_act_order_end_date ,
               EDW26.svc_act_order_start_date ,
               COALESCE(EDW26.svc_bmr_amt_ent, 0) ,
               COALESCE(EDW26.svc_bmr_amt_func, 0) ,
               COALESCE(EDW26.svc_bmr_amt_euro, 0) ,
               COALESCE(EDW26.svc_bmr_amt_us, 0) ,
               COALESCE(EDW26.svc_bmr_curr_code, ' ') ,
               COALESCE(EDW26.terminated_flag, ' ') ,
               EDW26.version_label_date ,
               COALESCE(EDW26.version_label_desc, ' ') ,
               COALESCE(EDW26.version_label_name, ' ') ,
               COALESCE(EDW26.vldtn_inventory_org_id, 0) ,
               COALESCE(EDW26.vrsn_lbl_tran_code, ' ') ,
               COALESCE(EDW26.invtry_item_desc_unc, ' ') ,
               null,
               null,
               COALESCE(EDW26.prev_site_nbr, '') ,
               COALESCE(EDW26.service_tier_name, ' ') ,
               COALESCE(IBIH.new_service_tier_name, ' ') ,
               COALESCE(IBIH.old_service_tier_name, ' ') ,
               EDW26.esd_flag ,
               EDW26.media_type ,
               EDW26.license_model ,
               EDW26.license_start_date ,
               EDW26.license_end_date,
                EDW26.cit_vendor_code,        
            EDW26.mdm_product_identifier, 
            EDW26.mdm_solution_identifier
        from
         """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t26 EDW26
        LEFT OUTER JOIN
              """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld              IBI
        ON    EDW26.instance_id                             = IBI.instance_id
        AND   EDW26.item_instance_id                        = IBI.item_instance_id
        LEFT OUTER JOIN 
        """ + db_params.TTMMPPDDBB1 + """.d8t_install_base_HISTTTMP IBIH
        ON   EDW26.item_instance_id                       = IBIH.item_instance_id 
        AND  EDW26.instance_id                            = IBIH.instance_id 
        LEFT OUTER JOIN
            """ + db_params.EEDDWWDDBB1 + """.es_slm_class                    ESC
        ON  EDW26.product_class = ESC.prod_class_code CLUSTER BY EDW26.INSTANCE_ID,EDW26.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t28"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   CACHE TABLE CFS_EXT SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.cfs_extended_attribute_ld """
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t28           SELECT /*+ BROADCASTJOIN( CFS_EXT ) */
                    EDW1.item_instance_id,
                    EDW1.instance_id,
                    EDW1.attribute_value,
                    EDW1.attribute_id,
                    EDW2.attribute_category_code,
                    null,
                    null,
                    null,
                    null,
                    null  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.cfs_attribute_value_ld    AS EDW1   
                INNER JOIN
                    CFS_EXT    AS EDW2    
                        ON upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))  
                        AND EDW1.attribute_id = EDW2.attribute_id  CLUSTER BY EDW1.INSTANCE_ID,EDW1.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t29"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t29           SELECT /*+ BROADCASTJOIN( CFS_EXT ) */
                    EDW1.item_instance_id,
                    EDW1.instance_id,
                    EDW1.attribute_value,
                    EDW1.attribute_id,
                    EDW2.attribute_category_code,
                    null,
                    null,
                    null,
                    null,
                    null  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.cfs_attribute_value_ld    AS EDW1   
                INNER JOIN
                    CFS_EXT   AS EDW2    
                        ON upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))  
                        AND EDW1.attribute_id = EDW2.attribute_id   
                        AND upper(trim(EDW2.attribute_category_code)) = upper(trim('HSR')) CLUSTER BY EDW1.INSTANCE_ID,EDW1.ITEM_INSTANCE_ID"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   UNCACHE TABLE CFS_EXT  """
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB2 + """.install_base_dn_ld"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """DROp TABLE IF EXISTS """ + db_params.TTMMPPDDBB1 + """.d8t_install_base_dnttmp"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """  
        create table """ + db_params.TTMMPPDDBB1 + """.d8t_install_base_dnttmp as SELECT
                            EDW1.item_instance_id,
                            EDW1.instance_id,
                            MAX( CASE 
                                WHEN EDW1.attribute_id = '10040'  THEN EDW2.attribute_value  
                                ELSE NULL  
                            END )  AS hsr_lgcy_product_id,
                            MAX( CASE 
                                WHEN EDW1.attribute_id = '10041'  THEN EDW2.attribute_value  
                                ELSE NULL  
                            END )  AS hsr_lgcy_party_nbr,
                            MAX( CASE 
                                WHEN EDW1.attribute_id = '10042'  THEN EDW2.attribute_value  
                                ELSE NULL  
                            END )  AS hsr_lgcy_site_nbr,
                            MAX( CASE 
                                WHEN EDW1.attribute_id = '10043'  THEN EDW2.attribute_value  
                                ELSE NULL  
                            END )  AS sw_key_nbr,
                            MAX( CASE 
                                WHEN EDW1.attribute_id = '10044'  THEN from_unixtime(unix_timestamp(EDW2.attribute_value,'dd-MMM-yyyy'),'yyyy-MM-dd')  
                                ELSE NULL  
                            END )  AS sw_effective_date,
                            MAX( CASE 
                                WHEN EDW1.attribute_id = '10045'  THEN from_unixtime(unix_timestamp(EDW2.attribute_value,'dd-MMM-yyyy'),'yyyy-MM-dd')  
                                ELSE NULL  
                            END )  AS sw_key_exp_date,
                            MAX( CASE 
                                WHEN EDW1.attribute_id = '10046'  THEN from_unixtime(unix_timestamp(EDW2.attribute_value,'dd-MMM-yyyy'),'yyyy-MM-dd') 
                                ELSE NULL  
                            END )  AS entitlement_exp_date,
                            MAX( CASE 
                                WHEN EDW1.attribute_id = '10047'  THEN EDW2.attribute_value  
                                ELSE NULL  
                            END )  AS rqs_id  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t29    AS EDW1   
                        INNER JOIN
                            (
                                SELECT
                                    item_instance_id,
                                    attribute_id,
                                    attribute_value  
                                FROM
                                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t28    AS EDW1   
                                WHERE upper(trim(EDW1.attribute_category_code)) = 'HSR'
                                GROUP BY
                                    item_instance_id ,
                                    attribute_id ,
                                    attribute_value      
                            )    AS EDW2    
                                ON EDW1.item_instance_id = EDW2.item_instance_id  
                                AND EDW1.attribute_id = EDW2.attribute_id     
                        GROUP BY
                            EDW1.item_instance_id ,
                            EDW1.instance_id CLUSTER BY EDW1.INSTANCE_ID,EDW1.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB2 + """.install_base_dn_ld           SELECT distinct
                    EDW27.instance_id,
                    EDW27.item_instance_id,
                    EDW27.active_end_date,
                    EDW27.active_start_date,
                    EDW27.actual_return_date,
                    EDW27.actual_ship_date,
                    EDW27.area_code,
                    EDW27.area_desc,
                    EDW27.bom_enabled_flag,
                    EDW27.cat_tran_code,
                    EDW27.country_code,
                    EDW27.country_desc,
                    EDW27.crnt_loc_cs_fml_org_code,
                    EDW27.customer_po_date,
                    EDW27.customer_po_nbr,
                    EDW27.dflt_service_coverage_code,
                    EDW27.dflt_service_duration,
                    EDW27.dflt_service_duration_cnt,
                    EDW27.dflt_service_product_id,
                    EDW27.equip_not_feature_kit_ind,
                    EDW27.equip_tracked_product_ind,
                    EDW27.external_reference_nbr,
                    EDW27.func_curr_code,
                    EDW27.gsdb_org_code,
                    EDW27.install_date,
                    EDW27.inventory_item_id,
                    EDW27.inventory_master_org_id,
                    EDW27.inventory_org_id,
                    EDW27.bill_to_site_use_id,
                    EDW27.invtry_item_desc,
                    EDW27.invtry_item_flag,
                    EDW27.invtry_item_sponsor_loc_id,
                    EDW27.invtry_item_status_code,
                    EDW27.invtry_org_tran_code,
                    EDW27.invtry_pass_nbr,
                    EDW27.invtry_tran_code,
                    EDW27.item_category_code,
                    EDW27.item_category_desc,
                    EDW27.item_instance_status_desc,
                    EDW27.item_instance_status_name,
                    EDW27.item_type_name,
                    EDW27.last_order_line_id,
                    EDW27.last_valid_invtry_org_id,
                    EDW27.nl_trackable_flag,
                    EDW27.offering_acctg_type_code,
                    EDW27.oper_unit_country_code,
                    EDW27.operating_unit_id,
                    EDW27.operating_unit_name,
                    EDW27.order_date,
                    EDW27.order_header_id,
                    EDW27.order_line_nbr,
                    EDW27.order_nbr,
                    EDW27.prod_line_tran_code,
                    EDW27.prod_act_order_end_date,
                    EDW27.prod_act_order_start_date,
                    EDW27.prod_act_svc_end_date,
                    EDW27.prod_act_svc_start_date,
                    EDW27.product_category_code,
                    EDW27.product_category_desc,
                    EDW27.product_class,
                    EDW27.product_class_model,
                    EDW27.product_group_name,
                    EDW27.product_group_nbr,
                    EDW27.product_id,
                    EDW27.product_id_formatted,
                    EDW27.product_line_name,
                    EDW27.product_line_nbr,
                    EDW27.product_model,
                    EDW27.product_source_org_id,
                    EDW27.product_submodel,
                    EDW27.product_type_code,
                    EDW27.product_type_name,
                    EDW27.region_code,
                    EDW27.region_desc,
                    EDW27.return_by_date,
                    EDW27.sal_code,
                    EDW27.serial_nbr_control_code,
                    EDW27.service_order_allowed_flag,
                    EDW27.serviceable_product_flag,
                    EDW27.ship_to_site_use_id,
                    EDW27.shippable_item_flag,
                    EDW27.status_tran_code,
                    EDW27.svc_act_order_end_date,
                    EDW27.svc_act_order_start_date,
                    EDW27.svc_bmr_amt_ent,
                    EDW27.svc_bmr_amt_func,
                    EDW27.svc_bmr_amt_euro,
                    EDW27.svc_bmr_amt_us,
                    EDW27.svc_bmr_curr_code,
                    EDW27.terminated_flag,
                    EDW27.invtry_uom_code,
                    EDW27.version_label_date,
                    EDW27.version_label_desc,
                    EDW27.version_label_name,
                    EDW27.vrsn_lbl_tran_code,
                    EDW27.vldtn_inventory_org_id,
                    EDW27.dflt_wrnty_coverage_code,
                    EDW27.dflt_wrnty_product_id,
                    EDW27.dflt_wrnty_term_mth_cnt,
                    EDW27.dflt_wrnty_product_desc,
                    CURRENT_TIMESTAMP() AS auto_c0100,
                    EDW27.install_location_id,
                    EDW27.install_location_type_code,
                    EDW27.install_loc_oper_unit_id,
                    EDW27.location_id,
                    EDW27.location_type_code,
                    EDW27.item_instance_status_id,
                    EDW27.item_instance_tran_code,
                    EDW27.previous_prod_id,
                    EDW27.previous_prod_id_formatted,
                    EDW27.product_id_swap_date,
                    EDW27.product_id_swap_time,
                    EDW27.future_move_concat_value,
                    EDW27.om_invoice_date,
                    EDW27.sub_region_code,
                    EDW27.sub_region_name,
                    EDW28.Attribute_value,
                    EDW27.invtry_item_desc_unc,
                    EDW29.entitlement_exp_date AS entitlement_exp_date,
                    EDW29.hsr_lgcy_party_nbr AS hsr_lgcy_party_nbr,
                    EDW29.hsr_lgcy_product_id AS hsr_lgcy_product_id,
                    EDW29.hsr_lgcy_site_nbr AS hsr_lgcy_site_nbr,
                    EDW29.rqs_id AS rqs_id,
                    EDW29.sw_effective_date sw_effective_date,
                    EDW29.sw_key_exp_date AS sw_key_exp_date,
                    EDW29.sw_key_nbr AS sw_key_nbr,
                    EDW27.prev_site_nbr,
                    EDW27.service_tier_name,
                    EDW27.new_service_tier_name,
                    EDW27.old_service_tier_name,
                    EDW27.esd_flag,
                    EDW27.media_type,
                    EDW27.license_model,
                    EDW27.license_start_date,
                    EDW27.license_end_date,
                EDW27.cit_vendor_code,        
                EDW27.mdm_product_identifier, 
                EDW27.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t27    AS EDW27   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t28    AS EDW28
                        ON EDW27.instance_id = EDW28.instance_id  
                        AND EDW27.item_instance_id = EDW28.item_instance_id  
                        AND upper(trim(EDW28.attribute_category_code)) = 'RETROFIT HISTORY'
                LEFT OUTER  JOIN
                     """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dnttmp    AS EDW29    
                                ON EDW27.instance_id = EDW29.instance_id  
                                AND EDW27.item_instance_id = EDW29.item_instance_id CLUSTER BY EDW27.INSTANCE_ID,EDW27.ITEM_INSTANCE_ID"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_purge_t1"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        #query = """    INSERT 
        #    INTO
        #        TABLE
        #        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_purge_t1           SELECT
        #            a.item_instance_id,
        #            a.instance_id  
        #        FROM
        #            """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld a 
        #        LEFT OUTER JOIN
        #            (
        #                SELECT
        #                    DISTINCT cast (object1_id1 as decimal(18,
        #                    0)) AS auto_c00  
        #                FROM
        #                    """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld     
        #                WHERE
        #                    upper(trim(object1_code)) =  'OKX_CUSTPROD' 
        #            ) AS autoAlias_90 
        #                ON (
        #                    a.item_instance_id = autoAlias_90.AUTO_C00 
        #                )  
        #        WHERE
        #            upper(trim(a.tran_code)) = 'D'  
        #            AND autoAlias_90.AUTO_C00 IS  NULL   
        #            AND (
        #                CAST (a.alt_as_of_date_time AS date) < (
        #                    CAST (DATE_SUB( CURRENT_DATE() , CAST (""" + db_params.PPDDAATTEE + """ as int) )  AS DATE)
        #                ) 
        #            ) 
        #        UNION
        #        SELECT
        #            a.item_instance_id,
        #            a.instance_id  
        #        FROM
        #            """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld a 
        #        LEFT OUTER JOIN
        #            (
        #                SELECT
        #                    DISTINCT cast (object1_id1 as decimal(18,
        #                    0)) AS auto_c00  
        #                FROM
        #                    """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld     
        #                WHERE
        #                    upper(trim(object1_code)) =  upper(trim('OKX_PARTYSITE')) 
        #            ) AS autoAlias_90 
        #                ON (
        #                    a.install_location_id = autoAlias_90.AUTO_C00 
        #                )  
        #        WHERE
        #            upper(trim(a.tran_code)) = upper(trim('D'))  
        #            AND autoAlias_90.AUTO_C00 IS  NULL   
        #            AND (
        #                CAST (a.alt_as_of_date_time AS date) < (
        #                    CAST (DATE_SUB( CURRENT_DATE() , CAST (""" + db_params.PPDDAATTEE + """ as int) )  AS DATE)
        #                ) 
        #            )"""
        #print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        #sparkSession.sql(query)
        query = """    use """ + db_params.EEDDWWDDBB1 + """"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld SELECT
                    a.* 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld    AS A   
                LEFt OUTER JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_purge_t1    AS B   
                        on  (
                            A.item_instance_id = B.item_instance_id  
                            AND a.instance_id = B.instance_id  
                        )  
                where
                    B.item_instance_id IS NULL  
                    and  B.instance_id IS NULL CLUSTER BY A.instance_id,A.ITEM_INSTANCE_ID"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.EEDDWWDDBB + """"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """  ALTER TABLE install_base_dn RENAME TO install_base_dn_hd"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_dn_ld RENAME TO install_base_dn"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_dn_hd RENAME TO install_base_dn_ld"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        use """ + db_params.EEDDWWDDBB1 + """"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_item RENAME TO install_base_item_hd"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_item_ld RENAME TO install_base_item"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_item_hd RENAME TO install_base_item_ld"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
            use """ + db_params.EEDDWWDDBB1 + """"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE cfs_attribute_value RENAME TO cfs_attribute_value_hd"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE cfs_attribute_value_ld RENAME TO cfs_attribute_value"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE cfs_attribute_value_hd RENAME TO cfs_attribute_value_ld"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ALTER TABLE cfs_extended_attribute RENAME TO cfs_extended_attribute_hd"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE cfs_extended_attribute_ld RENAME TO cfs_extended_attribute"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE cfs_extended_attribute_hd RENAME TO cfs_extended_attribute_ld"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        use """ + db_params.EEDDWWDDBB1 + """"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_item_hist RENAME TO install_base_item_hist_hd"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_item_hist_ld RENAME TO install_base_item_hist"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_item_hist_hd RENAME TO install_base_item_hist_ld"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        use """ + db_params.EEDDWWDDBB1 + """"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_vrsn_lbl RENAME TO install_base_vrsn_lbl_hd"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_vrsn_lbl_ld RENAME TO install_base_vrsn_lbl"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_vrsn_lbl_hd RENAME TO install_base_vrsn_lbl_ld"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        use """ + db_params.EEDDWWDDBB1 + """"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_item_sts RENAME TO install_base_item_sts_hd"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_item_sts_ld RENAME TO install_base_item_sts"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        ALTER TABLE install_base_item_sts_hd RENAME TO install_base_item_sts_ld"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_ml"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t1"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t2"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_sts_t3"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_ml"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t1"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t2"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_vrsn_lbl_t3"""
        print("Job 'D8_Install_base_dn_26_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = D8Installbasedn26ldpy(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print(e)
        print("Error in D8Installbasedn26ldpy")
        sparkSession.stop()
        sys.exit(1)

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8Installbasedn26ldpy").enableHiveSupport().getOrCreate()
    main()
