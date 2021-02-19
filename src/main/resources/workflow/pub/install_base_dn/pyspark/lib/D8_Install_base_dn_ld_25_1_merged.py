from pyspark.sql import SparkSession 
from datetime import datetime
import sys 
import db_params

class D8Installbasednld25merged1py:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t19"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t18 COMPUTE STATISTICS  FOR COLUMNS dflt_wrnty_product_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.EEDDWWDDBB + """"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   CACHE TABLE product_language select * from """ + db_params.EEDDWWDDBB2 + """.product_language  where upper(trim(primary_flag)) = 'Y' """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t19           SELECT /*+ BROADCASTJOIN(    product_language ) */
                    EDW18.instance_id,
                    EDW18.item_instance_id,
                    EDW18.active_end_date,
                    EDW18.active_start_date,
                    EDW18.actual_return_date,
                    EDW18.actual_ship_date,
                    EDW18.bill_to_site_use_id,
                    EDW18.bom_enabled_flag,
                    EDW18.country_code,
                    EDW18.crnt_loc_cs_fml_org_code,
                    EDW18.customer_po_date,
                    EDW18.customer_po_nbr,
                    EDW18.dflt_service_coverage_code,
                    null,
                    EDW18.dflt_service_duration_cnt,
                    EDW18.dflt_service_product_id,
                    EDW18.dflt_wrnty_coverage_code,
                    PRL.product_desc,
                    EDW18.dflt_wrnty_product_id,
                    EDW18.dflt_wrnty_term_mth_cnt,
                    EDW18.external_reference_nbr,
                    EDW18.gsdb_org_code,
                    EDW18.install_date,
                    EDW18.install_loc_oper_unit_id,
                    EDW18.install_location_id,
                    EDW18.install_location_type_code,
                    EDW18.inventory_item_id,
                    EDW18.inventory_master_org_id,
                    EDW18.inventory_org_id,
                    EDW18.invtry_item_desc,
                    EDW18.invtry_item_flag,
                    EDW18.invtry_item_sponsor_loc_id,
                    EDW18.invtry_item_status_code,
                    EDW18.invtry_org_tran_code,
                    EDW18.invtry_pass_nbr,
                    EDW18.invtry_tran_code,
                    EDW18.invtry_uom_code,
                    EDW18.item_instance_status_desc,
                    EDW18.item_instance_status_id,
                    EDW18.item_instance_status_name,
                    EDW18.item_instance_tran_code,
                    EDW18.item_type_name,
                    EDW18.last_order_line_id,
                    EDW18.last_valid_invtry_org_id,
                    EDW18.location_id,
                    EDW18.location_type_code,
                    EDW18.nl_trackable_flag,
                    EDW18.offering_acctg_type_code,
                    EDW18.oper_unit_country_code,
                    EDW18.operating_unit_id,
                    EDW18.operating_unit_name,
                    EDW18.order_date,
                    EDW18.order_header_id,
                    EDW18.order_line_nbr,
                    EDW18.order_nbr,
                    EDW18.prod_act_order_end_date,
                    EDW18.prod_act_order_start_date,
                    EDW18.prod_act_svc_end_date,
                    EDW18.prod_act_svc_start_date,
                    EDW18.product_class,
                    EDW18.product_class_model,
                    EDW18.product_id,
                    EDW18.product_id_formatted,
                    EDW18.product_model,
                    EDW18.product_source_org_id,
                    EDW18.product_submodel,
                    EDW18.return_by_date,
                    null,
                    EDW18.serial_nbr_control_code,
                    EDW18.service_order_allowed_flag,
                    EDW18.serviceable_product_flag,
                    EDW18.ship_to_site_use_id,
                    EDW18.shippable_item_flag,
                    EDW18.status_tran_code,
                    EDW18.svc_act_order_end_date,
                    EDW18.svc_act_order_start_date,
                    EDW18.terminated_flag,
                    EDW18.version_label_date,
                    EDW18.version_label_desc,
                    EDW18.version_label_name,
                    EDW18.vldtn_inventory_org_id,
                    EDW18.vrsn_lbl_tran_code,
                    EDW18.invtry_item_desc_unc,
                    EDW18.prev_site_nbr,
                    EDW18.service_tier_name,
                    EDW18.esd_flag,
                    EDW18.media_type,
                    EDW18.license_model,
                    EDW18.license_start_date,
                    EDW18.license_end_date,
                    EDW18.cit_vendor_code,
                    EDW18.mdm_product_identifier,
                    EDW18.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t18    AS EDW18   
                LEFT OUTER  JOIN
                    product_language  AS PRL  
                                ON EDW18.dflt_wrnty_product_id = PRL.product_id WHERE EDW18.dflt_wrnty_product_id is not null CLUSTER BY EDW18.instance_id,EDW18.item_instance_id   """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t19           SELECT 
                    EDW18.instance_id,
                    EDW18.item_instance_id,
                    EDW18.active_end_date,
                    EDW18.active_start_date,
                    EDW18.actual_return_date,
                    EDW18.actual_ship_date,
                    EDW18.bill_to_site_use_id,
                    EDW18.bom_enabled_flag,
                    EDW18.country_code,
                    EDW18.crnt_loc_cs_fml_org_code,
                    EDW18.customer_po_date,
                    EDW18.customer_po_nbr,
                    EDW18.dflt_service_coverage_code,
                    null,
                    EDW18.dflt_service_duration_cnt,
                    EDW18.dflt_service_product_id,
                    EDW18.dflt_wrnty_coverage_code,
                    null,
                    EDW18.dflt_wrnty_product_id,
                    EDW18.dflt_wrnty_term_mth_cnt,
                    EDW18.external_reference_nbr,
                    EDW18.gsdb_org_code,
                    EDW18.install_date,
                    EDW18.install_loc_oper_unit_id,
                    EDW18.install_location_id,
                    EDW18.install_location_type_code,
                    EDW18.inventory_item_id,
                    EDW18.inventory_master_org_id,
                    EDW18.inventory_org_id,
                    EDW18.invtry_item_desc,
                    EDW18.invtry_item_flag,
                    EDW18.invtry_item_sponsor_loc_id,
                    EDW18.invtry_item_status_code,
                    EDW18.invtry_org_tran_code,
                    EDW18.invtry_pass_nbr,
                    EDW18.invtry_tran_code,
                    EDW18.invtry_uom_code,
                    EDW18.item_instance_status_desc,
                    EDW18.item_instance_status_id,
                    EDW18.item_instance_status_name,
                    EDW18.item_instance_tran_code,
                    EDW18.item_type_name,
                    EDW18.last_order_line_id,
                    EDW18.last_valid_invtry_org_id,
                    EDW18.location_id,
                    EDW18.location_type_code,
                    EDW18.nl_trackable_flag,
                    EDW18.offering_acctg_type_code,
                    EDW18.oper_unit_country_code,
                    EDW18.operating_unit_id,
                    EDW18.operating_unit_name,
                    EDW18.order_date,
                    EDW18.order_header_id,
                    EDW18.order_line_nbr,
                    EDW18.order_nbr,
                    EDW18.prod_act_order_end_date,
                    EDW18.prod_act_order_start_date,
                    EDW18.prod_act_svc_end_date,
                    EDW18.prod_act_svc_start_date,
                    EDW18.product_class,
                    EDW18.product_class_model,
                    EDW18.product_id,
                    EDW18.product_id_formatted,
                    EDW18.product_model,
                    EDW18.product_source_org_id,
                    EDW18.product_submodel,
                    EDW18.return_by_date,
                    null,
                    EDW18.serial_nbr_control_code,
                    EDW18.service_order_allowed_flag,
                    EDW18.serviceable_product_flag,
                    EDW18.ship_to_site_use_id,
                    EDW18.shippable_item_flag,
                    EDW18.status_tran_code,
                    EDW18.svc_act_order_end_date,
                    EDW18.svc_act_order_start_date,
                    EDW18.terminated_flag,
                    EDW18.version_label_date,
                    EDW18.version_label_desc,
                    EDW18.version_label_name,
                    EDW18.vldtn_inventory_org_id,
                    EDW18.vrsn_lbl_tran_code,
                    EDW18.invtry_item_desc_unc,
                    EDW18.prev_site_nbr,
                    EDW18.service_tier_name,
                    EDW18.esd_flag,
                    EDW18.media_type,
                    EDW18.license_model,
                    EDW18.license_start_date,
                    EDW18.license_end_date,
                    EDW18.cit_vendor_code,
                    EDW18.mdm_product_identifier,
                    EDW18.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t18    AS EDW18   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t19 AS   EDW19
                                ON EDW18.instance_id = EDW19.instance_id and edw18.item_instance_id=EDW19.item_instance_id where edw19.item_instance_id is null and edw19.item_instance_id is null cluster by edw18.instance_id,edw18.item_instance_id   """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   UNCACHE TABLE product_language """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t19 COMPUTE STATISTICS  FOR COLUMNS dflt_service_product_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t19 COMPUTE STATISTICS  FOR COLUMNS item_instance_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t19 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t20"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t20           SELECT /*+ BROADCASTJOIN( product ) */
                    EDW19.instance_id,
                    EDW19.item_instance_id,
                    EDW19.active_end_date,
                    EDW19.active_start_date,
                    EDW19.actual_return_date,
                    EDW19.actual_ship_date,
                    EDW19.bill_to_site_use_id,
                    EDW19.bom_enabled_flag,
                    EDW19.country_code,
                    EDW19.crnt_loc_cs_fml_org_code,
                    EDW19.customer_po_date,
                    EDW19.customer_po_nbr,
                    EDW19.dflt_service_coverage_code,
                    PROD.service_duration AS dflt_service_duration,
                    EDW19.dflt_service_duration_cnt,
                    EDW19.dflt_service_product_id,
                    EDW19.dflt_wrnty_coverage_code,
                    EDW19.dflt_wrnty_product_desc,
                    EDW19.dflt_wrnty_product_id,
                    EDW19.dflt_wrnty_term_mth_cnt,
                    EDW19.external_reference_nbr,
                    EDW19.gsdb_org_code,
                    EDW19.install_date,
                    EDW19.install_loc_oper_unit_id,
                    EDW19.install_location_id,
                    EDW19.install_location_type_code,
                    EDW19.inventory_item_id,
                    EDW19.inventory_master_org_id,
                    EDW19.inventory_org_id,
                    EDW19.invtry_item_desc,
                    EDW19.invtry_item_flag,
                    EDW19.invtry_item_sponsor_loc_id,
                    EDW19.invtry_item_status_code,
                    EDW19.invtry_org_tran_code,
                    EDW19.invtry_pass_nbr,
                    EDW19.invtry_tran_code,
                    EDW19.invtry_uom_code,
                    EDW19.item_instance_status_desc,
                    EDW19.item_instance_status_id,
                    EDW19.item_instance_status_name,
                    EDW19.item_instance_tran_code,
                    EDW19.item_type_name,
                    EDW19.last_order_line_id,
                    EDW19.last_valid_invtry_org_id,
                    EDW19.location_id,
                    EDW19.location_type_code,
                    EDW19.nl_trackable_flag,
                    EDW19.offering_acctg_type_code,
                    EDW19.oper_unit_country_code,
                    EDW19.operating_unit_id,
                    EDW19.operating_unit_name,
                    EDW19.order_date,
                    EDW19.order_header_id,
                    EDW19.order_line_nbr,
                    EDW19.order_nbr,
                    EDW19.prod_act_order_end_date,
                    EDW19.prod_act_order_start_date,
                    EDW19.prod_act_svc_end_date,
                    EDW19.prod_act_svc_start_date,
                    EDW19.product_class,
                    EDW19.product_class_model,
                    EDW19.product_id,
                    EDW19.product_id_formatted,
                    EDW19.product_model,
                    EDW19.product_source_org_id,
                    EDW19.product_submodel,
                    EDW19.return_by_date,
                    null,
                    EDW19.serial_nbr_control_code,
                    EDW19.service_order_allowed_flag,
                    EDW19.serviceable_product_flag,
                    EDW19.ship_to_site_use_id,
                    EDW19.shippable_item_flag,
                    EDW19.status_tran_code,
                    EDW19.svc_act_order_end_date,
                    EDW19.svc_act_order_start_date,
                    EDW19.terminated_flag,
                    EDW19.version_label_date,
                    EDW19.version_label_desc,
                    EDW19.version_label_name,
                    EDW19.vldtn_inventory_org_id,
                    EDW19.vrsn_lbl_tran_code,
                    EDW19.invtry_item_desc_unc,
                    EDW19.prev_site_nbr,
                    EDW19.service_tier_name,
                    EDW19.esd_flag,
                    EDW19.media_type,
                    EDW19.license_model,
                    EDW19.license_start_date,
                    EDW19.license_end_date,
                    EDW19.cit_vendor_code,
                    EDW19.mdm_product_identifier,
                    EDW19.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t19    AS EDW19   
                LEFT OUTER  JOIN
                    product    AS PROD    
                        ON EDW19.dflt_service_product_id = PROD.product_id CLUSTER BY EDW19.instance_id,EDW19.item_instance_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   UNCACHE TABLE product  """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t21"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        df1_qq1=sparkSession.sql(""" select
                        wca.country_area_code,
                        country_area_desc,
                        branch_code 
                    from
                        """ + db_params.EEDDWWDDBB2 + """.wcs_country_area wca 
                    INNER JOIN
                        """ + db_params.EEDDWWDDBB2 + """.branch_country_area_xref BCAX 
                            on  WCA.country_area_code = BCAX.country_area_code""")
        df1_qq1.registerTempTable("country"); 
        query = """ INSERT  
                    INTO
                        TABLE
                        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t21           SELECT /*+ BROADCASTJOIN( country ) */
                            EDW20.instance_id,
                            EDW20.item_instance_id,
                            EDW20.active_end_date,
                            EDW20.active_start_date,
                            EDW20.actual_return_date,
                            EDW20.actual_ship_date,
                            WCA.country_area_code,
                            WCA.country_area_desc,
                            EDW20.bill_to_site_use_id,
                            EDW20.bom_enabled_flag,
                            EDW20.country_code,
                            EDW20.crnt_loc_cs_fml_org_code,
                            EDW20.customer_po_date,
                            EDW20.customer_po_nbr,
                            EDW20.dflt_service_coverage_code,
                            EDW20.dflt_service_duration,
                            EDW20.dflt_service_duration_cnt,
                            EDW20.dflt_service_product_id,
                            EDW20.dflt_wrnty_coverage_code,
                            EDW20.dflt_wrnty_product_desc,
                            EDW20.dflt_wrnty_product_id,
                            EDW20.dflt_wrnty_term_mth_cnt,
                            EDW20.external_reference_nbr,
                            EDW20.gsdb_org_code,
                            EDW20.install_date,
                            EDW20.install_loc_oper_unit_id,
                            EDW20.install_location_id,
                            EDW20.install_location_type_code,
                            EDW20.inventory_item_id,
                            EDW20.inventory_master_org_id,
                            EDW20.inventory_org_id,
                            EDW20.invtry_item_desc,
                            EDW20.invtry_item_flag,
                            EDW20.invtry_item_sponsor_loc_id,
                            EDW20.invtry_item_status_code,
                            EDW20.invtry_org_tran_code,
                            EDW20.invtry_pass_nbr,
                            EDW20.invtry_tran_code,
                            EDW20.invtry_uom_code,
                            EDW20.item_instance_status_desc,
                            EDW20.item_instance_status_id,
                            EDW20.item_instance_status_name,
                            EDW20.item_instance_tran_code,
                            EDW20.item_type_name,
                            EDW20.last_order_line_id,
                            EDW20.last_valid_invtry_org_id,
                            EDW20.location_id,
                            EDW20.location_type_code,
                            EDW20.nl_trackable_flag,
                            EDW20.offering_acctg_type_code,
                            EDW20.oper_unit_country_code,
                            EDW20.operating_unit_id,
                            EDW20.operating_unit_name,
                            EDW20.order_date,
                            EDW20.order_header_id,
                            EDW20.order_line_nbr,
                            EDW20.order_nbr,
                            EDW20.prod_act_order_end_date,
                            EDW20.prod_act_order_start_date,
                            EDW20.prod_act_svc_end_date,
                            EDW20.prod_act_svc_start_date,
                            EDW20.product_class,
                            EDW20.product_class_model,
                            EDW20.product_id,
                            EDW20.product_id_formatted,
                            EDW20.product_model,
                            EDW20.product_source_org_id,
                            EDW20.product_submodel,
                            EDW20.return_by_date,
                            null,
                            EDW20.serial_nbr_control_code,
                            EDW20.service_order_allowed_flag,
                            EDW20.serviceable_product_flag,
                            EDW20.ship_to_site_use_id,
                            EDW20.shippable_item_flag,
                            EDW20.status_tran_code,
                            EDW20.svc_act_order_end_date,
                            EDW20.svc_act_order_start_date,
                            EDW20.terminated_flag,
                            EDW20.version_label_date,
                            EDW20.version_label_desc,
                            EDW20.version_label_name,
                            EDW20.vldtn_inventory_org_id,
                            EDW20.vrsn_lbl_tran_code,
                            EDW20.invtry_item_desc_unc,
                            EDW20.prev_site_nbr,
                            EDW20.service_tier_name,
                            EDW20.esd_flag,
                            EDW20.media_type,
                            EDW20.license_model,
                            EDW20.license_start_date,
                            EDW20.license_end_date,
                        EDW20.cit_vendor_code,        
                        EDW20.mdm_product_identifier, 
                        EDW20.mdm_solution_identifier
                        FROM
                            """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t20   AS EDW20  
                        LEFT OUTER  JOIN
                            country as wca  
                                ON wca.branch_code = EDW20.crnt_loc_cs_fml_org_code AND trim(upper(country_code)) = 'US' Where EDW20.crnt_loc_cs_fml_org_code is not null
                    """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ INSERT  
                    INTO
                        TABLE
                        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t21           SELECT
                            EDW20.instance_id,
                            EDW20.item_instance_id,
                            EDW20.active_end_date,
                            EDW20.active_start_date,
                            EDW20.actual_return_date,
                            EDW20.actual_ship_date,
                            null,
                            null,
                            EDW20.bill_to_site_use_id,
                            EDW20.bom_enabled_flag,
                            EDW20.country_code,
                            EDW20.crnt_loc_cs_fml_org_code,
                            EDW20.customer_po_date,
                            EDW20.customer_po_nbr,
                            EDW20.dflt_service_coverage_code,
                            EDW20.dflt_service_duration,
                            EDW20.dflt_service_duration_cnt,
                            EDW20.dflt_service_product_id,
                            EDW20.dflt_wrnty_coverage_code,
                            EDW20.dflt_wrnty_product_desc,
                            EDW20.dflt_wrnty_product_id,
                            EDW20.dflt_wrnty_term_mth_cnt,
                            EDW20.external_reference_nbr,
                            EDW20.gsdb_org_code,
                            EDW20.install_date,
                            EDW20.install_loc_oper_unit_id,
                            EDW20.install_location_id,
                            EDW20.install_location_type_code,
                            EDW20.inventory_item_id,
                            EDW20.inventory_master_org_id,
                            EDW20.inventory_org_id,
                            EDW20.invtry_item_desc,
                            EDW20.invtry_item_flag,
                            EDW20.invtry_item_sponsor_loc_id,
                            EDW20.invtry_item_status_code,
                            EDW20.invtry_org_tran_code,
                            EDW20.invtry_pass_nbr,
                            EDW20.invtry_tran_code,
                            EDW20.invtry_uom_code,
                            EDW20.item_instance_status_desc,
                            EDW20.item_instance_status_id,
                            EDW20.item_instance_status_name,
                            EDW20.item_instance_tran_code,
                            EDW20.item_type_name,
                            EDW20.last_order_line_id,
                            EDW20.last_valid_invtry_org_id,
                            EDW20.location_id,
                            EDW20.location_type_code,
                            EDW20.nl_trackable_flag,
                            EDW20.offering_acctg_type_code,
                            EDW20.oper_unit_country_code,
                            EDW20.operating_unit_id,
                            EDW20.operating_unit_name,
                            EDW20.order_date,
                            EDW20.order_header_id,
                            EDW20.order_line_nbr,
                            EDW20.order_nbr,
                            EDW20.prod_act_order_end_date,
                            EDW20.prod_act_order_start_date,
                            EDW20.prod_act_svc_end_date,
                            EDW20.prod_act_svc_start_date,
                            EDW20.product_class,
                            EDW20.product_class_model,
                            EDW20.product_id,
                            EDW20.product_id_formatted,
                            EDW20.product_model,
                            EDW20.product_source_org_id,
                            EDW20.product_submodel,
                            EDW20.return_by_date,
                            null,
                            EDW20.serial_nbr_control_code,
                            EDW20.service_order_allowed_flag,
                            EDW20.serviceable_product_flag,
                            EDW20.ship_to_site_use_id,
                            EDW20.shippable_item_flag,
                            EDW20.status_tran_code,
                            EDW20.svc_act_order_end_date,
                            EDW20.svc_act_order_start_date,
                            EDW20.terminated_flag,
                            EDW20.version_label_date,
                            EDW20.version_label_desc,
                            EDW20.version_label_name,
                            EDW20.vldtn_inventory_org_id,
                            EDW20.vrsn_lbl_tran_code,
                            EDW20.invtry_item_desc_unc,
                            EDW20.prev_site_nbr,
                            EDW20.service_tier_name,
                            EDW20.esd_flag,
                            EDW20.media_type,
                            EDW20.license_model,
                            EDW20.license_start_date,
                            EDW20.license_end_date,
                        EDW20.cit_vendor_code,        
                        EDW20.mdm_product_identifier, 
                        EDW20.mdm_solution_identifier
                        FROM
                            """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t20   AS EDW20  
                        LEFT OUTER  JOIN
                            """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t21   AS EDW21  
                                ON EDW20.INSTANCE_ID = EDW21.INSTANCE_ID AND EDW20.ITEM_INSTANCE_ID = EDW21.ITEM_INSTANCE_ID Where EDW21.INSTANCE_ID is null and EDW21.ITEM_INSTANCE_ID is null
                    """ 
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t22"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   CACHE TABLE GEO select * from """ + db_params.EEDDWWDDBB1 + """.geo_rollup_xref where upper(trim(business_unit_code)) = 'B4' """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t22           SELECT /*+ BROADCASTJOIN( GEO ) */
                    EDW21.instance_id,
                    EDW21.item_instance_id,
                    EDW21.active_end_date,
                    EDW21.active_start_date,
                    EDW21.actual_return_date,
                    EDW21.actual_ship_date,
                    GRX.area_code,
                    GRX.area_desc,
                    EDW21.bill_to_site_use_id,
                    EDW21.bom_enabled_flag,
                    EDW21.country_code,
                    GRX.country_desc,
                    EDW21.crnt_loc_cs_fml_org_code,
                    EDW21.customer_po_date,
                    EDW21.customer_po_nbr,
                    EDW21.dflt_service_coverage_code,
                    EDW21.dflt_service_duration,
                    EDW21.dflt_service_duration_cnt,
                    EDW21.dflt_service_product_id,
                    EDW21.dflt_wrnty_coverage_code,
                    EDW21.dflt_wrnty_product_desc,
                    EDW21.dflt_wrnty_product_id,
                    EDW21.dflt_wrnty_term_mth_cnt,
                    EDW21.external_reference_nbr,
                    EDW21.gsdb_org_code,
                    EDW21.install_date,
                    EDW21.install_loc_oper_unit_id,
                    EDW21.install_location_id,
                    EDW21.install_location_type_code,
                    EDW21.inventory_item_id,
                    EDW21.inventory_master_org_id,
                    EDW21.inventory_org_id,
                    EDW21.invtry_item_desc,
                    EDW21.invtry_item_flag,
                    EDW21.invtry_item_sponsor_loc_id,
                    EDW21.invtry_item_status_code,
                    EDW21.invtry_org_tran_code,
                    EDW21.invtry_pass_nbr,
                    EDW21.invtry_tran_code,
                    EDW21.invtry_uom_code,
                    EDW21.item_instance_status_desc,
                    EDW21.item_instance_status_id,
                    EDW21.item_instance_status_name,
                    EDW21.item_instance_tran_code,
                    EDW21.item_type_name,
                    EDW21.last_order_line_id,
                    EDW21.last_valid_invtry_org_id,
                    EDW21.location_id,
                    EDW21.location_type_code,
                    EDW21.nl_trackable_flag,
                    EDW21.offering_acctg_type_code,
                    EDW21.oper_unit_country_code,
                    EDW21.operating_unit_id,
                    EDW21.operating_unit_name,
                    EDW21.order_date,
                    EDW21.order_header_id,
                    EDW21.order_line_nbr,
                    EDW21.order_nbr,
                    EDW21.prod_act_order_end_date,
                    EDW21.prod_act_order_start_date,
                    EDW21.prod_act_svc_end_date,
                    EDW21.prod_act_svc_start_date,
                    EDW21.product_class,
                    EDW21.product_class_model,
                    EDW21.product_id,
                    EDW21.product_id_formatted,
                    EDW21.product_model,
                    EDW21.product_source_org_id,
                    EDW21.product_submodel,
                    GRX.region_code,
                    GRX.region_desc,
                    EDW21.return_by_date,
                    null,
                    EDW21.serial_nbr_control_code,
                    EDW21.service_order_allowed_flag,
                    EDW21.serviceable_product_flag,
                    EDW21.ship_to_site_use_id,
                    EDW21.shippable_item_flag,
                    EDW21.status_tran_code,
                    EDW21.svc_act_order_end_date,
                    EDW21.svc_act_order_start_date,
                    EDW21.terminated_flag,
                    EDW21.version_label_date,
                    EDW21.version_label_desc,
                    EDW21.version_label_name,
                    EDW21.vldtn_inventory_org_id,
                    EDW21.vrsn_lbl_tran_code,
                    EDW21.invtry_item_desc_unc,
                    EDW21.prev_site_nbr,
                    EDW21.service_tier_name,
                    EDW21.esd_flag,
                    EDW21.media_type,
                    EDW21.license_model,
                    EDW21.license_start_date,
                    EDW21.license_end_date,
                    EDW21.cit_vendor_code,
                    EDW21.mdm_product_identifier,
                    EDW21.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t21    AS EDW21   
                LEFT OUTER  JOIN
                    GEO    AS GRX  
                                ON EDW21.country_code = GRX.country_code    
                WHERE
                    EDW21.area_code  IS NULL CLUSTER BY EDW21.instance_id,EDW21.item_instance_id """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   INSERT  
                        INTO
                            TABLE
                            """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t22         SELECT /*+ BROADCASTJOIN( GEO ) */
                        EDW21.instance_id,
                        EDW21.item_instance_id,
                        EDW21.active_end_date,
                        EDW21.active_start_date,
                        EDW21.actual_return_date,
                        EDW21.actual_ship_date,
                        EDW21.area_code,
                        EDW21.area_desc,
                        EDW21.bill_to_site_use_id,
                        EDW21.bom_enabled_flag,
                        EDW21.country_code,
                        GRX.country_desc,
                        EDW21.crnt_loc_cs_fml_org_code,
                        EDW21.customer_po_date,
                        EDW21.customer_po_nbr,
                        EDW21.dflt_service_coverage_code,
                        EDW21.dflt_service_duration,
                        EDW21.dflt_service_duration_cnt,
                        EDW21.dflt_service_product_id,
                        EDW21.dflt_wrnty_coverage_code,
                        EDW21.dflt_wrnty_product_desc,
                        EDW21.dflt_wrnty_product_id,
                        EDW21.dflt_wrnty_term_mth_cnt,
                        EDW21.external_reference_nbr,
                        EDW21.gsdb_org_code,
                        EDW21.install_date,
                        EDW21.install_loc_oper_unit_id,
                        EDW21.install_location_id,
                        EDW21.install_location_type_code,
                        EDW21.inventory_item_id,
                        EDW21.inventory_master_org_id,
                        null,
                        EDW21.invtry_item_desc,
                        EDW21.invtry_item_flag,
                        EDW21.invtry_item_sponsor_loc_id,
                        EDW21.invtry_item_status_code,
                        EDW21.invtry_org_tran_code,
                        EDW21.invtry_pass_nbr,
                        EDW21.invtry_tran_code,
                        EDW21.invtry_uom_code,
                        EDW21.item_instance_status_desc,
                        EDW21.item_instance_status_id,
                        EDW21.item_instance_status_name,
                        EDW21.item_instance_tran_code,
                        EDW21.item_type_name,
                        EDW21.last_order_line_id,
                        EDW21.last_valid_invtry_org_id,
                        EDW21.location_id,
                        EDW21.location_type_code,
                        EDW21.nl_trackable_flag,
                        EDW21.offering_acctg_type_code,
                        EDW21.oper_unit_country_code,
                        EDW21.operating_unit_id,
                        EDW21.operating_unit_name,
                        EDW21.order_date,
                        EDW21.order_header_id,
                        EDW21.order_line_nbr,
                        EDW21.order_nbr,
                        EDW21.prod_act_order_end_date,
                        EDW21.prod_act_order_start_date,
                        EDW21.prod_act_svc_end_date,
                        EDW21.prod_act_svc_start_date,
                        EDW21.product_class,
                        EDW21.product_class_model,
                        EDW21.product_id,
                        EDW21.product_id_formatted,
                        EDW21.product_model,
                        EDW21.product_source_org_id,
                        EDW21.product_submodel,
                        GRX.region_code,
                        GRX.region_desc,
                        EDW21.return_by_date,
                        null,
                        EDW21.serial_nbr_control_code,
                        EDW21.service_order_allowed_flag,
                        EDW21.serviceable_product_flag,
                        EDW21.ship_to_site_use_id,
                        EDW21.shippable_item_flag,
                        EDW21.status_tran_code,
                        EDW21.svc_act_order_end_date,
                        EDW21.svc_act_order_start_date,
                        EDW21.terminated_flag,
                        EDW21.version_label_date,
                        EDW21.version_label_desc,
                        EDW21.version_label_name,
                        EDW21.vldtn_inventory_org_id,
                        EDW21.vrsn_lbl_tran_code,
                        EDW21.invtry_item_desc_unc,
                        EDW21.prev_site_nbr,
                        EDW21.service_tier_name,
                        EDW21.esd_flag,
                        EDW21.media_type,
                        EDW21.license_model,
                        EDW21.license_start_date,
                        EDW21.license_end_date,
                    EDW21.cit_vendor_code,        
                    EDW21.mdm_product_identifier, 
                    EDW21.mdm_solution_identifier
                    FROM
                        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t21    AS EDW21  
                    LEFT OUTER  JOIN
                        GEO  AS GRX  
                            ON EDW21.country_code = GRX.country_code  
                    LEFT OUTER JOIN
                        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t22  AS autoAlias_44  
                            ON ( upper(trim(EDW21.instance_id)) = upper(trim(autoAlias_44.instance_id))  
                            AND EDW21.item_instance_id = autoAlias_44.item_instance_id )  
                    WHERE
                        autoAlias_44.instance_id IS  NULL  
                        AND autoAlias_44.item_instance_id IS  NULL CLUSTER BY EDW21.INSTANCE_ID,EDW21.ITEM_INSTANCE_ID       """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   UNCACHE TABLE GEO  """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t23"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   CACHE TABLE CURR select * from """ + db_params.EEDDWWDDBB1 + """.currency_xref  where upper(trim(data_source_code)) = 'COS' """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT  
                    INTO
                        TABLE
                        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t23           SELECT /*+ BROADCASTJOIN( CURR ) */
                            EDW22.instance_id,
                            EDW22.item_instance_id,
                            EDW22.active_end_date,
                            EDW22.active_start_date,
                            EDW22.actual_return_date,
                            EDW22.actual_ship_date,
                            EDW22.area_code,
                            EDW22.area_desc,
                            EDW22.bill_to_site_use_id,
                            EDW22.bom_enabled_flag,
                            EDW22.country_code,
                            EDW22.country_desc,
                            EDW22.crnt_loc_cs_fml_org_code,
                            EDW22.customer_po_date,
                            EDW22.customer_po_nbr,
                            EDW22.dflt_service_coverage_code,
                            EDW22.dflt_service_duration,
                            EDW22.dflt_service_duration_cnt,
                            EDW22.dflt_service_product_id,
                            EDW22.dflt_wrnty_coverage_code,
                            EDW22.dflt_wrnty_product_desc,
                            EDW22.dflt_wrnty_product_id,
                            EDW22.dflt_wrnty_term_mth_cnt,
                            EDW22.external_reference_nbr,
                            CXREF.currency_code,
                            EDW22.gsdb_org_code,
                            EDW22.install_date,
                            EDW22.install_loc_oper_unit_id,
                            EDW22.install_location_id,
                            EDW22.install_location_type_code,
                            EDW22.inventory_item_id,
                            EDW22.inventory_master_org_id,
                            EDW22.inventory_org_id,
                            EDW22.invtry_item_desc,
                            EDW22.invtry_item_flag,
                            EDW22.invtry_item_sponsor_loc_id,
                            EDW22.invtry_item_status_code,
                            EDW22.invtry_org_tran_code,
                            EDW22.invtry_pass_nbr,
                            EDW22.invtry_tran_code,
                            EDW22.invtry_uom_code,
                            EDW22.item_instance_status_desc,
                            EDW22.item_instance_status_id,
                            EDW22.item_instance_status_name,
                            EDW22.item_instance_tran_code,
                            EDW22.item_type_name,
                            EDW22.last_order_line_id,
                            EDW22.last_valid_invtry_org_id,
                            EDW22.location_id,
                            EDW22.location_type_code,
                            EDW22.nl_trackable_flag,
                            EDW22.offering_acctg_type_code,
                            EDW22.oper_unit_country_code,
                            EDW22.operating_unit_id,
                            EDW22.operating_unit_name,
                            EDW22.order_date,
                            EDW22.order_header_id,
                            EDW22.order_line_nbr,
                            EDW22.order_nbr,
                            EDW22.prod_act_order_end_date,
                            EDW22.prod_act_order_start_date,
                            EDW22.prod_act_svc_end_date,
                            EDW22.prod_act_svc_start_date,
                            EDW22.product_class,
                            EDW22.product_class_model,
                            EDW22.product_id,
                            EDW22.product_id_formatted,
                            EDW22.product_model,
                            EDW22.product_source_org_id,
                            EDW22.product_submodel,
                            EDW22.region_code,
                            EDW22.region_desc,
                            EDW22.return_by_date,
                            null,
                            EDW22.serial_nbr_control_code,
                            EDW22.service_order_allowed_flag,
                            EDW22.serviceable_product_flag,
                            EDW22.ship_to_site_use_id,
                            EDW22.shippable_item_flag,
                            EDW22.status_tran_code,
                            EDW22.svc_act_order_end_date,
                            EDW22.svc_act_order_start_date,
                            EDW22.terminated_flag,
                            EDW22.version_label_date,
                            EDW22.version_label_desc,
                            EDW22.version_label_name,
                            EDW22.vldtn_inventory_org_id,
                            EDW22.vrsn_lbl_tran_code,
                            EDW22.invtry_item_desc_unc,
                            EDW22.prev_site_nbr,
                            EDW22.service_tier_name,
                            EDW22.esd_flag,
                            EDW22.media_type,
                            EDW22.license_model,
                            EDW22.license_start_date,
                            EDW22.license_end_date,
                        EDW22.cit_vendor_code,        
                        EDW22.mdm_product_identifier, 
                        EDW22.mdm_solution_identifier
                        FROM
                            """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t22    AS EDW22  
                        LEFT OUTER  JOIN
                            CURR  AS CXREF  
                                ON EDW22.country_code = CXREF.country_code CLUSTER BY EDW22.INSTANCE_ID,EDW22.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   UNCACHE TABLE CURR """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24a"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   CLEAR CACHE """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   DROP TABLE IF EXISTS  """ + db_params.TTMMPPDDBB + """.PROD_TTMP  """ 
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        df3_qq5=sparkSession.sql(""" select * from """ + db_params.EEDDWWDDBB1 + """.product_price where upper(trim(price_type_desc)) = 'BMR' and  product_price_start_date  IS NOT NULL """)
        df3_qq5.registerTempTable("PRODUCT_PRICE_AGG"); 
        df5_qq1=sparkSession.sql(""" SELECT
                                    gsdb_org_code AS auto_c00,
                                    product_id AS auto_c01,
                                    MAX( product_price_start_date ) AS m_product_price_start_date  
                                FROM
                                    PRODUCT_PRICE_AGG                          
                                GROUP BY
                                    gsdb_org_code ,
                                    product_id  """)
        df5_qq1.registerTempTable("MAX_PRODUCT_PRICE"); 
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """  INSERT  
                    INTO
                        TABLE
                        """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24a           SELECT /*+ BROADCASTJOIN( MAX_PRODUCT_PRICE ) */
                            gsdb_org_code,
                            product_id,
                            prod_value_amt_ent,
                            currency_code  
                        FROM
                            PRODUCT_PRICE_AGG  
                        INNER JOIN
                            MAX_PRODUCT_PRICE AS autoAlias_46  
                                ON (
                                    upper(trim(gsdb_org_code)) = upper(trim(autoAlias_46.AUTO_C00))  
                                    AND upper(trim(product_id)) = upper(trim(autoAlias_46.AUTO_C01))  
                                    AND product_price_start_date = autoAlias_46.m_product_price_start_date  
                                ) CLUSTER BY gsdb_org_code,product_id """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ CLEAR CACHE"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        df1_qq8=sparkSession.sql(""" SELECT
                            gsdb_org_code AS auto_c00,
                            product_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24a  
                        GROUP BY
                            gsdb_org_code ,
                            product_id""")
        df1_qq8.registerTempTable("IB24A"); 
        query = """  INSERT  
                        INTO
                            TABLE
                            """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24a       SELECT /*+ BROADCASTJOIN( IB24A ) */
                        gsdb_org_code,
                        product_id,
                        prod_value_amt_ent,
                        currency_code  
                    FROM
                        """ + db_params.EEDDWWDDBB1 + """.product_price  
                    LEFT OUTER JOIN
                        IB24A AS autoAlias_48  
                            ON ( upper(trim(gsdb_org_code)) = upper(trim(autoAlias_48.AUTO_C00))  
                            AND upper(trim(product_id)) = upper(trim(autoAlias_48.AUTO_C01)) )  
                    WHERE
                        upper(trim(price_type_desc)) = 'BMR'  
                        AND product_price_start_date  IS NULL  
                        AND autoAlias_48.AUTO_C00 IS  NULL  
                        AND autoAlias_48.AUTO_C01 IS  NULL       """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t23 COMPUTE STATISTICS  FOR COLUMNS gsdb_org_code"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t23 COMPUTE STATISTICS  FOR COLUMNS dflt_service_product_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24a COMPUTE STATISTICS  FOR COLUMNS gsdb_org_code"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24a COMPUTE STATISTICS  FOR COLUMNS product_id"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24b"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24b           SELECT
                    EDW23.instance_id,
                    EDW23.item_instance_id,
                    EDW23.active_end_date,
                    EDW23.active_start_date,
                    EDW23.actual_return_date,
                    EDW23.actual_ship_date,
                    EDW23.area_code,
                    EDW23.area_desc,
                    EDW23.bill_to_site_use_id,
                    EDW23.bom_enabled_flag,
                    EDW23.country_code,
                    EDW23.country_desc,
                    EDW23.crnt_loc_cs_fml_org_code,
                    EDW23.customer_po_date,
                    EDW23.customer_po_nbr,
                    EDW23.dflt_service_coverage_code,
                    EDW23.dflt_service_duration,
                    EDW23.dflt_service_duration_cnt,
                    EDW23.dflt_service_product_id,
                    EDW23.dflt_wrnty_coverage_code,
                    EDW23.dflt_wrnty_product_desc,
                    EDW23.dflt_wrnty_product_id,
                    EDW23.dflt_wrnty_term_mth_cnt,
                    EDW23.external_reference_nbr,
                    EDW23.func_curr_code,
                    EDW23.gsdb_org_code,
                    EDW23.install_date,
                    EDW23.install_loc_oper_unit_id,
                    EDW23.install_location_id,
                    EDW23.install_location_type_code,
                    EDW23.inventory_item_id,
                    EDW23.inventory_master_org_id,
                    (Case when EDW23.gsdb_org_code = EDW24A.gsdb_org_code and EDW23.dflt_service_product_id = EDW24A.product_id then EDW23.inventory_org_id else null end),
                    EDW23.invtry_item_desc,
                    EDW23.invtry_item_flag,
                    EDW23.invtry_item_sponsor_loc_id,
                    EDW23.invtry_item_status_code,
                    EDW23.invtry_org_tran_code,
                    EDW23.invtry_pass_nbr,
                    EDW23.invtry_tran_code,
                    EDW23.invtry_uom_code,
                    EDW23.item_instance_status_desc,
                    EDW23.item_instance_status_id,
                    EDW23.item_instance_status_name,
                    EDW23.item_instance_tran_code,
                    EDW23.item_type_name,
                    EDW23.last_order_line_id,
                    EDW23.last_valid_invtry_org_id,
                    EDW23.location_id,
                    EDW23.location_type_code,
                    EDW23.nl_trackable_flag,
                    EDW23.offering_acctg_type_code,
                    EDW23.oper_unit_country_code,
                    EDW23.operating_unit_id,
                    EDW23.operating_unit_name,
                    EDW23.order_date,
                    EDW23.order_header_id,
                    EDW23.order_line_nbr,
                    EDW23.order_nbr,
                    EDW23.prod_act_order_end_date,
                    EDW23.prod_act_order_start_date,
                    EDW23.prod_act_svc_end_date,
                    EDW23.prod_act_svc_start_date,
                    EDW23.product_class,
                    EDW23.product_class_model,
                    EDW23.product_id,
                    EDW23.product_id_formatted,
                    EDW23.product_model,
                    EDW23.product_source_org_id,
                    EDW23.product_submodel,
                    EDW23.region_code,
                    EDW23.region_desc,
                    EDW23.return_by_date,
                    null,
                    EDW23.serial_nbr_control_code,
                    EDW23.service_order_allowed_flag,
                    EDW23.serviceable_product_flag,
                    EDW23.ship_to_site_use_id,
                    EDW23.shippable_item_flag,
                    EDW23.status_tran_code,
                    EDW23.svc_act_order_end_date,
                    EDW23.svc_act_order_start_date,
                    (Case when EDW23.gsdb_org_code = EDW24A.gsdb_org_code and EDW23.dflt_service_product_id = EDW24A.product_id then EDW24A.svc_bmr_amt_ent else null end),
                    (Case when EDW23.gsdb_org_code = EDW24A.gsdb_org_code and EDW23.dflt_service_product_id = EDW24A.product_id then EDW24A.svc_bmr_curr_code else null end),
                    EDW23.terminated_flag,
                    EDW23.version_label_date,
                    EDW23.version_label_desc,
                    EDW23.version_label_name,
                    EDW23.vldtn_inventory_org_id,
                    EDW23.vrsn_lbl_tran_code,
                    EDW23.invtry_item_desc_unc,
                    EDW23.prev_site_nbr,
                    EDW23.service_tier_name,
                    EDW23.esd_flag,
                    EDW23.media_type,
                    EDW23.license_model,
                    EDW23.license_start_date,
                    EDW23.license_end_date,
                    EDW23.cit_vendor_code,
                    EDW23.mdm_product_identifier,
                    EDW23.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t23    AS EDW23   LEFT OUTER JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24a    AS EDW24A   
                on
                    EDW23.gsdb_org_code = EDW24A.gsdb_org_code  
                    AND EDW23.dflt_service_product_id = EDW24A.product_id CLUSTER BY EDW23.INSTANCE_ID,EDW23.ITEM_INSTANCE_ID """
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t25"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24b COMPUTE STATISTICS  FOR COLUMNS country_code"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24b COMPUTE STATISTICS  FOR COLUMNS svc_bmr_curr_code"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t25           SELECT /*+ BROADCASTJOIN( """ + db_params.EEDDWWDDBB1 + """.cfs_curr_calc_country_bkp ) */
                    EDW24B.instance_id,
                    EDW24B.item_instance_id,
                    EDW24B.active_end_date,
                    EDW24B.active_start_date,
                    EDW24B.actual_return_date,
                    EDW24B.actual_ship_date,
                    EDW24B.area_code,
                    EDW24B.area_desc,
                    EDW24B.bill_to_site_use_id,
                    EDW24B.bom_enabled_flag,
                    EDW24B.country_code,
                    EDW24B.country_desc,
                    EDW24B.crnt_loc_cs_fml_org_code,
                    EDW24B.customer_po_date,
                    EDW24B.customer_po_nbr,
                    EDW24B.dflt_service_coverage_code,
                    EDW24B.dflt_service_duration,
                    EDW24B.dflt_service_duration_cnt,
                    EDW24B.dflt_service_product_id,
                    EDW24B.dflt_wrnty_coverage_code,
                    EDW24B.dflt_wrnty_product_desc,
                    EDW24B.dflt_wrnty_product_id,
                    EDW24B.dflt_wrnty_term_mth_cnt,
                    EDW24B.external_reference_nbr,
                    EDW24B.func_curr_code,
                    EDW24B.gsdb_org_code,
                    EDW24B.install_date,
                    EDW24B.install_loc_oper_unit_id,
                    EDW24B.install_location_id,
                    EDW24B.install_location_type_code,
                    EDW24B.inventory_item_id,
                    EDW24B.inventory_master_org_id,
                    EDW24B.inventory_org_id,
                    EDW24B.invtry_item_desc,
                    EDW24B.invtry_item_flag,
                    EDW24B.invtry_item_sponsor_loc_id,
                    EDW24B.invtry_item_status_code,
                    EDW24B.invtry_org_tran_code,
                    EDW24B.invtry_pass_nbr,
                    EDW24B.invtry_tran_code,
                    EDW24B.invtry_uom_code,
                    EDW24B.item_instance_status_desc,
                    EDW24B.item_instance_status_id,
                    EDW24B.item_instance_status_name,
                    EDW24B.item_instance_tran_code,
                    EDW24B.item_type_name,
                    EDW24B.last_order_line_id,
                    EDW24B.last_valid_invtry_org_id,
                    EDW24B.location_id,
                    EDW24B.location_type_code,
                    EDW24B.nl_trackable_flag,
                    EDW24B.offering_acctg_type_code,
                    EDW24B.oper_unit_country_code,
                    EDW24B.operating_unit_id,
                    EDW24B.operating_unit_name,
                    EDW24B.order_date,
                    EDW24B.order_header_id,
                    EDW24B.order_line_nbr,
                    EDW24B.order_nbr,
                    EDW24B.prod_act_order_end_date,
                    EDW24B.prod_act_order_start_date,
                    EDW24B.prod_act_svc_end_date,
                    EDW24B.prod_act_svc_start_date,
                    EDW24B.product_class,
                    EDW24B.product_class_model,
                    EDW24B.product_id,
                    EDW24B.product_id_formatted,
                    EDW24B.product_model,
                    EDW24B.product_source_org_id,
                    EDW24B.product_submodel,
                    EDW24B.region_code,
                    EDW24B.region_desc,
                    EDW24B.return_by_date,
                    null,
                    EDW24B.serial_nbr_control_code,
                    EDW24B.service_order_allowed_flag,
                    EDW24B.serviceable_product_flag,
                    EDW24B.ship_to_site_use_id,
                    EDW24B.shippable_item_flag,
                    EDW24B.status_tran_code,
                    EDW24B.svc_act_order_end_date,
                    EDW24B.svc_act_order_start_date,
                    EDW24B.svc_bmr_amt_ent,
                    bround(EDW24B.svc_bmr_amt_ent * CCCC.euro_average_rate,5) AS auto_c082,
                    bround(EDW24B.svc_bmr_amt_ent * CCCC.local_average_rate,5) AS auto_c083,
                    bround(EDW24B.svc_bmr_amt_ent * CCCC.us_average_rate,5) AS auto_c084,
                    EDW24B.svc_bmr_curr_code,
                    EDW24B.terminated_flag,
                    EDW24B.version_label_date,
                    EDW24B.version_label_desc,
                    EDW24B.version_label_name,
                    EDW24B.vldtn_inventory_org_id,
                    EDW24B.vrsn_lbl_tran_code,
                    EDW24B.invtry_item_desc_unc,
                    EDW24B.prev_site_nbr,
                    EDW24B.service_tier_name,
                    EDW24B.esd_flag,
                    EDW24B.media_type,
                    EDW24B.license_model,
                    EDW24B.license_start_date,
                    EDW24B.license_end_date,
                    EDW24B.cit_vendor_code,
                    EDW24B.mdm_product_identifier,
                    EDW24B.mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_dn_t24b    AS EDW24B   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB1 + """.cfs_curr_calc_country_bkp    AS CCCC    
                        ON upper(trim(EDW24B.country_code)) = upper(trim(CCCC.country_code)) 
                        AND upper(trim(EDW24B.svc_bmr_curr_code)) = upper(trim(CCCC.translate_from_curr_code))"""
        print("Job 'D8_Install_base_dn_ld_25_merged_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = D8Installbasednld25merged1py(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print (e)
        print 'Error in D8Installbasednld25merged1py'
        sparkSession.stop()
        sys.exit(1)



if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8Installbasednld25merged1py").enableHiveSupport().getOrCreate()
    main()
    sparkSession.stop()
    sys.exit(0)

