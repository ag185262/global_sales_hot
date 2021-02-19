from pyspark import SparkConf, SparkContext, SQLContext 
from pyspark.sql import SparkSession, HiveContext 
import sys 
import logging 
import db_params

class d8contractlinemergesql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_ml COMPUTE STATISTICS  FOR COLUMNS TRAN_CODE  , STATUS_CODE"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """INSERT INTO TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t1 
        SELECT  contract_line_id, instance_id, alt_contract_header_id, as_of_date_time,
                batch_nbr, config_complete_flag, config_header_id, config_item_id,
                config_item_type_code, config_revision_nbr, config_valid_flag,
                contract_header_id, contract_line_nbr, contract_line_style_id,
                contract_price_type_code, created_by_name, creation_date_time,
                curr_code, display_sequence_nbr, end_date_time, exception_flag,
                hidden_line_flag, invoice_line_level_flag, item_to_price_flag,
                last_update_date_time, last_update_login_name, last_updated_by_name,
                line_list_price_amt_ent, negot_price_amt_ent, negot_renew_price_amt_ent,
                next_day_response_flag, orig_sys_id, orig_sys_ref_id, orig_sys_source_code,
                parent_contract_line_id, price_basis_flag, price_hold_adjust_est_pct,
                price_hold_amt_ent, price_hold_break_code, price_hold_enfrc_list_flag,
                price_hold_intgrtd_flag, price_hold_list_ref_id, price_hold_prchs_min_amt_ent,
                price_hold_prchs_min_qty, price_hold_price_type_code, price_level_flag,
                price_list_header_id, price_list_line_id, pricing_date_time,
                renew_contract_line_id, renew_curr_code, renew_date_time, renew_to_contract_line_id,
                sot_order_sot_line_nbr, start_date_time, status_code, template_used_name,
                termination_code, termination_date_time, termination_reason_code,
                top_model_contract_line_id, tran_code, unit_price_amt_ent, unit_price_pct,
                upg_orig_sys_ref_id, upg_orig_sys_ref_name, warranty_exp_reason_code,
                bill_to_site_use_id, cust_acct_id, invoice_rule_id, line_renewal_type_code,
                ship_to_site_use_id, holiday_coverage_code, orig_serviced_qty,
                CASE 
                    WHEN ( TRIM( orig_serviced_qty ) NOT BETWEEN '0' AND '9999999999999' ) THEN 0 
                    ELSE INT(orig_serviced_qty) 
                END AS auto_c074, reverse_logistics_text, routing_comment_text,
                source_location_name, legacy_d1_severity_code, crb_invoice_nbr_text,
                crb_reference_nbr_text, mdm_product_identifier, mdm_solution_identifier 
        FROM    ( 
            SELECT  m1.*, ROW_NUMBER() OVER(
            PARTITION BY instance_id , contract_line_id 
            ORDER BY as_of_date_time DESC, batch_nbr DESC) AS r00 
            FROM    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_ml m1 
            WHERE   upper(trim(tran_code)) <> 'D') rcrd 
        WHERE   r00=1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query="""    CACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t1 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        #print("Running Query: {}".format(query))
        #sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2           SELECT distinct
                    contract_line_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    config_complete_flag,
                    config_header_id,
                    config_item_id,
                    config_item_type_code,
                    config_revision_nbr,
                    config_valid_flag,
                    contract_header_id,
                    contract_line_nbr,
                    contract_line_style_id,
                    contract_price_type_code,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    'Y',
                    display_sequence_nbr,
                    end_date_time,
                    exception_flag,
                    hidden_line_flag,
                    invoice_line_level_flag,
                    item_to_price_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    line_list_price_amt_ent,
                    negot_price_amt_ent,
                    negot_renew_price_amt_ent,
                    next_day_response_flag,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    parent_contract_line_id,
                    price_basis_flag,
                    price_hold_adjust_est_pct,
                    price_hold_amt_ent,
                    price_hold_break_code,
                    price_hold_enfrc_list_flag,
                    price_hold_intgrtd_flag,
                    price_hold_list_ref_id,
                    price_hold_prchs_min_amt_ent,
                    price_hold_prchs_min_qty,
                    price_hold_price_type_code,
                    price_level_flag,
                    price_list_header_id,
                    price_list_line_id,
                    pricing_date_time,
                    renew_contract_line_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_line_id,
                    sot_order_sot_line_nbr,
                    start_date_time,
                    status_code,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    top_model_contract_line_id,
                    tran_code,
                    unit_price_amt_ent,
                    unit_price_pct,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    warranty_exp_reason_code,
                    bill_to_site_use_id,
                    cust_acct_id,
                    invoice_rule_id,
                    line_renewal_type_code,
                    ship_to_site_use_id,
                    holiday_coverage_code,
                    orig_serviced_qty,
                    serviced_qty,
                    reverse_logistics_text,
                    routing_comment_text,
                    source_location_name,
                    legacy_d1_severity_code,
                    crb_invoice_nbr_text,
                    crb_reference_nbr_text,
                    mdm_product_identifier,
                    mdm_solution_identifier    
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_ld 
                WHERE (upper(trim(contract_line_id)), upper(trim(instance_id))) IN 
                (SELECT      upper(trim(contract_line_id)),     upper(trim(instance_id))  
                FROM """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t1)"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t3"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t3           SELECT 
                    contract_line_id,
                    instance_id,
                    alt_contract_header_id,
                    config_complete_flag,
                    config_header_id,
                    config_item_id,
                    config_item_type_code,
                    config_revision_nbr,
                    config_valid_flag,
                    contract_header_id,
                    contract_line_nbr,
                    contract_line_style_id,
                    contract_price_type_code,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    display_sequence_nbr,
                    end_date_time,
                    exception_flag,
                    hidden_line_flag,
                    invoice_line_level_flag,
                    item_to_price_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    line_list_price_amt_ent,
                    negot_price_amt_ent,
                    negot_renew_price_amt_ent,
                    next_day_response_flag,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    parent_contract_line_id,
                    price_basis_flag,
                    price_hold_adjust_est_pct,
                    price_hold_amt_ent,
                    price_hold_break_code,
                    price_hold_enfrc_list_flag,
                    price_hold_intgrtd_flag,
                    price_hold_list_ref_id,
                    price_hold_prchs_min_amt_ent,
                    price_hold_prchs_min_qty,
                    price_hold_price_type_code,
                    price_level_flag,
                    price_list_header_id,
                    price_list_line_id,
                    pricing_date_time,
                    renew_contract_line_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_line_id,
                    sot_order_sot_line_nbr,
                    start_date_time,
                    status_code,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    top_model_contract_line_id,
                    tran_code,
                    unit_price_amt_ent,
                    unit_price_pct,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    warranty_exp_reason_code,
                    bill_to_site_use_id,
                    cust_acct_id,
                    invoice_rule_id,
                    line_renewal_type_code,
                    ship_to_site_use_id,
                    holiday_coverage_code,
                    orig_serviced_qty,
                    serviced_qty,
                    reverse_logistics_text,
                    routing_comment_text,
                    source_location_name,
                    legacy_d1_severity_code,
                    crb_invoice_nbr_text,
                    crb_reference_nbr_text,
                    mdm_product_identifier,
                    mdm_solution_identifier  
                FROM
                    """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2 COMPUTE STATISTICS  FOR COLUMNS CURRENT_RECORD_IND"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t3           SELECT 
                    contract_line_id,
                    instance_id,
                    alt_contract_header_id,
                    config_complete_flag,
                    config_header_id,
                    config_item_id,
                    config_item_type_code,
                    config_revision_nbr,
                    config_valid_flag,
                    contract_header_id,
                    contract_line_nbr,
                    contract_line_style_id,
                    contract_price_type_code,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    display_sequence_nbr,
                    end_date_time,
                    exception_flag,
                    hidden_line_flag,
                    invoice_line_level_flag,
                    item_to_price_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    line_list_price_amt_ent,
                    negot_price_amt_ent,
                    negot_renew_price_amt_ent,
                    next_day_response_flag,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    parent_contract_line_id,
                    price_basis_flag,
                    price_hold_adjust_est_pct,
                    price_hold_amt_ent,
                    price_hold_break_code,
                    price_hold_enfrc_list_flag,
                    price_hold_intgrtd_flag,
                    price_hold_list_ref_id,
                    price_hold_prchs_min_amt_ent,
                    price_hold_prchs_min_qty,
                    price_hold_price_type_code,
                    price_level_flag,
                    price_list_header_id,
                    price_list_line_id,
                    pricing_date_time,
                    renew_contract_line_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_line_id,
                    sot_order_sot_line_nbr,
                    start_date_time,
                    status_code,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    top_model_contract_line_id,
                    tran_code,
                    unit_price_amt_ent,
                    unit_price_pct,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    warranty_exp_reason_code,
                    bill_to_site_use_id,
                    cust_acct_id,
                    invoice_rule_id,
                    line_renewal_type_code,
                    ship_to_site_use_id,
                    holiday_coverage_code,
                    orig_serviced_qty,
                    serviced_qty,
                    reverse_logistics_text,
                    routing_comment_text,
                    source_location_name,
                    legacy_d1_severity_code,
                    crb_invoice_nbr_text,
                    crb_reference_nbr_text,
                    mdm_product_identifier,
                    mdm_solution_identifier  
                FROM
                    """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t1
                WHERE (upper(trim(contract_line_id)), upper(trim(instance_id)))
                IN
                (SELECT      upper(trim(contract_line_id)),     upper(trim(instance_id))     FROM """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2     )
                    """ 
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """     use """ + db_params.ttmmppddbb1 + """ """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """ INSERT OVERWRITE TABLE """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2 
                SELECT  distinct AAPPLLIIDD1EENNVV_contract_line_t2.contract_line_id AS contract_line_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.instance_id AS instance_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                AAPPLLIIDD1EENNVV_contract_line_t2.alt_batch_nbr AS alt_batch_nbr ,
                AAPPLLIIDD1EENNVV_contract_line_t2.alt_contract_header_id AS alt_contract_header_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.as_of_date_time AS as_of_date_time ,
                AAPPLLIIDD1EENNVV_contract_line_t2.batch_nbr AS batch_nbr , AAPPLLIIDD1EENNVV_contract_line_t2.config_complete_flag AS config_complete_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.config_header_id AS config_header_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.config_item_id AS config_item_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.config_item_type_code AS config_item_type_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.config_revision_nbr AS config_revision_nbr ,
                AAPPLLIIDD1EENNVV_contract_line_t2.config_valid_flag AS config_valid_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.contract_header_id AS contract_header_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.contract_line_nbr AS contract_line_nbr ,
                AAPPLLIIDD1EENNVV_contract_line_t2.contract_line_style_id AS contract_line_style_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.contract_price_type_code AS contract_price_type_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.created_by_name AS created_by_name ,
                AAPPLLIIDD1EENNVV_contract_line_t2.creation_date_time AS creation_date_time ,
                AAPPLLIIDD1EENNVV_contract_line_t2.curr_code AS curr_code , 
                CASE 
                    WHEN autoAlias_119.auto_c00 IS not null THEN 'D' 
                    ELSE AAPPLLIIDD1EENNVV_contract_line_t2.current_record_ind 
                END AS current_record_ind , AAPPLLIIDD1EENNVV_contract_line_t2.display_sequence_nbr AS display_sequence_nbr ,
                AAPPLLIIDD1EENNVV_contract_line_t2.end_date_time AS end_date_time ,
                AAPPLLIIDD1EENNVV_contract_line_t2.exception_flag AS exception_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.hidden_line_flag AS hidden_line_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.invoice_line_level_flag AS invoice_line_level_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.item_to_price_flag AS item_to_price_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.last_update_date_time AS last_update_date_time ,
                AAPPLLIIDD1EENNVV_contract_line_t2.last_update_login_name AS last_update_login_name ,
                AAPPLLIIDD1EENNVV_contract_line_t2.last_updated_by_name AS last_updated_by_name ,
                AAPPLLIIDD1EENNVV_contract_line_t2.line_list_price_amt_ent AS line_list_price_amt_ent ,
                AAPPLLIIDD1EENNVV_contract_line_t2.negot_price_amt_ent AS negot_price_amt_ent ,
                AAPPLLIIDD1EENNVV_contract_line_t2.negot_renew_price_amt_ent AS negot_renew_price_amt_ent ,
                AAPPLLIIDD1EENNVV_contract_line_t2.next_day_response_flag AS next_day_response_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.orig_sys_id AS orig_sys_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.orig_sys_ref_id AS orig_sys_ref_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.orig_sys_source_code AS orig_sys_source_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.parent_contract_line_id AS parent_contract_line_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_basis_flag AS price_basis_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_hold_adjust_est_pct AS price_hold_adjust_est_pct ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_hold_amt_ent AS price_hold_amt_ent ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_hold_break_code AS price_hold_break_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_hold_enfrc_list_flag AS price_hold_enfrc_list_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_hold_intgrtd_flag AS price_hold_intgrtd_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_hold_list_ref_id AS price_hold_list_ref_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_hold_prchs_min_amt_ent AS price_hold_prchs_min_amt_ent ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_hold_prchs_min_qty AS price_hold_prchs_min_qty ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_hold_price_type_code AS price_hold_price_type_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_level_flag AS price_level_flag ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_list_header_id AS price_list_header_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.price_list_line_id AS price_list_line_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.pricing_date_time AS pricing_date_time ,
                AAPPLLIIDD1EENNVV_contract_line_t2.renew_contract_line_id AS renew_contract_line_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.renew_curr_code AS renew_curr_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.renew_date_time AS renew_date_time ,
                AAPPLLIIDD1EENNVV_contract_line_t2.renew_to_contract_line_id AS renew_to_contract_line_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.sot_order_sot_line_nbr AS sot_order_sot_line_nbr ,
                AAPPLLIIDD1EENNVV_contract_line_t2.start_date_time AS start_date_time ,
                AAPPLLIIDD1EENNVV_contract_line_t2.status_code AS status_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.template_used_name AS template_used_name ,
                AAPPLLIIDD1EENNVV_contract_line_t2.termination_code AS termination_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.termination_date_time AS termination_date_time ,
                AAPPLLIIDD1EENNVV_contract_line_t2.termination_reason_code AS termination_reason_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.top_model_contract_line_id AS top_model_contract_line_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.tran_code AS tran_code , AAPPLLIIDD1EENNVV_contract_line_t2.unit_price_amt_ent AS unit_price_amt_ent ,
                AAPPLLIIDD1EENNVV_contract_line_t2.unit_price_pct AS unit_price_pct ,
                AAPPLLIIDD1EENNVV_contract_line_t2.upg_orig_sys_ref_id AS upg_orig_sys_ref_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.upg_orig_sys_ref_name AS upg_orig_sys_ref_name ,
                AAPPLLIIDD1EENNVV_contract_line_t2.warranty_exp_reason_code AS warranty_exp_reason_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.bill_to_site_use_id AS bill_to_site_use_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.cust_acct_id AS cust_acct_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.invoice_rule_id AS invoice_rule_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.line_renewal_type_code AS line_renewal_type_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.ship_to_site_use_id AS ship_to_site_use_id ,
                AAPPLLIIDD1EENNVV_contract_line_t2.holiday_coverage_code AS holiday_coverage_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.orig_serviced_qty AS orig_serviced_qty ,
                AAPPLLIIDD1EENNVV_contract_line_t2.serviced_qty AS serviced_qty ,
                AAPPLLIIDD1EENNVV_contract_line_t2.reverse_logistics_text AS reverse_logistics_text ,
                AAPPLLIIDD1EENNVV_contract_line_t2.routing_comment_text AS routing_comment_text ,
                AAPPLLIIDD1EENNVV_contract_line_t2.source_location_name AS source_location_name ,
                AAPPLLIIDD1EENNVV_contract_line_t2.legacy_d1_severity_code AS legacy_d1_severity_code ,
                AAPPLLIIDD1EENNVV_contract_line_t2.crb_invoice_nbr_text AS crb_invoice_nbr_text ,
                AAPPLLIIDD1EENNVV_contract_line_t2.crb_reference_nbr_text AS crb_reference_nbr_text ,
                AAPPLLIIDD1EENNVV_contract_line_t2.mdm_product_identifier AS mdm_product_identifier ,
                AAPPLLIIDD1EENNVV_contract_line_t2.mdm_solution_identifier AS mdm_solution_identifier 
        FROM    """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2 AS AAPPLLIIDD1EENNVV_contract_line_t2 
        Left Outer Join (
            SELECT   contract_line_id AS auto_c00, instance_id AS auto_c01  
            FROM     """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t3      
            GROUP BY contract_line_id,instance_id 
            HAVING  COUNT(*) = 1)autoAlias_119 
            ON (UPPER(TRIM(autoAlias_119.auto_c00)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contract_line_t2.contract_line_id)) 
            AND UPPER(TRIM(autoAlias_119.auto_c01)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contract_line_t2.instance_id)))"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
                 TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_ld 
                 SELECT distinct Q1.* FROM  """ + db_params.EEDDWWDDBB1 + """.contract_line_ld AS Q1
                 LEFT OUTER JOIN 
                        (
                        SELECT contract_line_id, instance_id
                        FROM
                         """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t1 AS Q2  
                        LEFT OUTER JOIN 
                            (   SELECT contract_line_id AS autoc000 , instance_id as autoc001
                            FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2 
                                WHERE UPPER(TRIM(current_record_ind)) = 'D' ) AS Q3 
                        ON UPPER(TRIM(Q2.contract_line_id)) = UPPER(TRIM(Q3.autoc000))
                        AND UPPER(TRIM(Q2.instance_id)) = UPPER(TRIM(Q3.autoc001))
                            WHERE Q3.autoc000 IS NULL 
                            AND Q3.autoc001 IS NULL
                        ) AS Q23
                 ON UPPER(TRIM(Q1.contract_line_id)) = UPPER(TRIM(Q23.contract_line_id))
                 AND UPPER(TRIM(Q1.instance_id)) = UPPER(TRIM(Q23.instance_id))
                    WHERE Q23.contract_line_id IS NULL 
                    AND Q23.instance_id IS NULL"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_line_ld           SELECT 
                    contract_line_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    config_complete_flag,
                    config_header_id,
                    config_item_id,
                    config_item_type_code,
                    config_revision_nbr,
                    config_valid_flag,
                    contract_header_id,
                    contract_line_nbr,
                    contract_line_style_id,
                    contract_price_type_code,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    display_sequence_nbr,
                    end_date_time,
                    exception_flag,
                    hidden_line_flag,
                    invoice_line_level_flag,
                    item_to_price_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    line_list_price_amt_ent,
                    negot_price_amt_ent,
                    negot_renew_price_amt_ent,
                    next_day_response_flag,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    parent_contract_line_id,
                    price_basis_flag,
                    price_hold_adjust_est_pct,
                    price_hold_amt_ent,
                    price_hold_break_code,
                    price_hold_enfrc_list_flag,
                    price_hold_intgrtd_flag,
                    price_hold_list_ref_id,
                    price_hold_prchs_min_amt_ent,
                    price_hold_prchs_min_qty,
                    price_hold_price_type_code,
                    price_level_flag,
                    price_list_header_id,
                    price_list_line_id,
                    pricing_date_time,
                    renew_contract_line_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_line_id,
                    sot_order_sot_line_nbr,
                    start_date_time,
                    status_code,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    top_model_contract_line_id,
                    tran_code,
                    unit_price_amt_ent,
                    unit_price_pct,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    warranty_exp_reason_code,
                    bill_to_site_use_id,
                    cust_acct_id,
                    invoice_rule_id,
                    line_renewal_type_code,
                    ship_to_site_use_id,
                    holiday_coverage_code,
                    orig_serviced_qty,
                    serviced_qty,
                    routing_comment_text,
                    reverse_logistics_text,
                    source_location_name,
                    legacy_d1_severity_code,
                    crb_invoice_nbr_text,
                    crb_reference_nbr_text,
                    mdm_product_identifier,
                    mdm_solution_identifier  
                FROM
                    """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t1  WHERE ( upper(trim(contract_line_id)), upper(trim(instance_id)) ) NOT IN (SELECT  upper(trim(contract_line_id)), upper(trim(instance_id))     FROM """ + db_params.ttmmppddbb1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2     WHERE upper(trim(current_record_ind)) = 'D'      ) """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query="""    UNCACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_dn_t1 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        #print("Running Query: {}".format(query))
        #sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t1           SELECT distinct
                    contract_line_id,
                    instance_id,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    config_complete_flag,
                    config_header_id,
                    config_item_id,
                    config_item_type_code,
                    config_revision_nbr,
                    config_valid_flag,
                    contract_header_id,
                    contract_line_nbr,
                    contract_line_style_id,
                    contract_price_type_code,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    display_sequence_nbr,
                    end_date_time,
                    exception_flag,
                    hidden_line_flag,
                    invoice_line_level_flag,
                    item_to_price_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    line_list_price_amt_ent,
                    negot_price_amt_ent,
                    negot_renew_price_amt_ent,
                    next_day_response_flag,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    parent_contract_line_id,
                    price_basis_flag,
                    price_hold_adjust_est_pct,
                    price_hold_amt_ent,
                    price_hold_break_code,
                    price_hold_enfrc_list_flag,
                    price_hold_intgrtd_flag,
                    price_hold_list_ref_id,
                    price_hold_prchs_min_amt_ent,
                    price_hold_prchs_min_qty,
                    price_hold_price_type_code,
                    price_level_flag,
                    price_list_header_id,
                    price_list_line_id,
                    pricing_date_time,
                    renew_contract_line_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_line_id,
                    sot_order_sot_line_nbr,
                    start_date_time,
                    status_code,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    top_model_contract_line_id,
                    tran_code,
                    unit_price_amt_ent,
                    unit_price_pct,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    warranty_exp_reason_code,
                    bill_to_site_use_id,
                    cust_acct_id,
                    invoice_rule_id,
                    line_renewal_type_code,
                    ship_to_site_use_id,
                    holiday_coverage_code,
                    orig_serviced_qty,
                    CASE 
                        WHEN  ( TRIM( orig_serviced_qty ) NOT BETWEEN '0' AND '9999999999999' )  THEN 0  
                        ELSE INT (orig_serviced_qty) 
                    END AS auto_c074,
                    reverse_logistics_text,
                    routing_comment_text,
                    source_location_name,
                    legacy_d1_severity_code,
                    crb_invoice_nbr_text,
                    crb_reference_nbr_text,
                    mdm_product_identifier,
                    mdm_solution_identifier  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_ml 
                INNER JOIN
                    (
                        SELECT
                            MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                            batch_nbr) ) AS auto_c00,
                            contract_line_id AS auto_c01,
                            instance_id AS auto_c02  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_ml     
                        WHERE
                            upper(trim(tran_code)) = 'D'
                        GROUP BY
                            contract_line_id ,
                            instance_id 
                    ) AS autoAlias_125 
                        ON (
                            CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                        batch_nbr) = autoAlias_125.AUTO_C00 
                        AND upper(trim(contract_line_id)) = upper(trim(autoAlias_125.AUTO_C01)) 
                        AND upper(trim(instance_id)) = upper(trim(autoAlias_125.AUTO_C02)) ) 
                    WHERE
                        upper(trim(tran_code)) = 'D' """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query="""    CACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_dn_t1 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        #print("Running Query: {}".format(query))
        #sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2           SELECT distinct
                    l.contract_line_id,
                    l.instance_id,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.alt_contract_header_id,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.config_complete_flag,
                    l.config_header_id,
                    l.config_item_id,
                    l.config_item_type_code,
                    l.config_revision_nbr,
                    l.config_valid_flag,
                    l.contract_header_id,
                    l.contract_line_nbr,
                    l.contract_line_style_id,
                    l.contract_price_type_code,
                    l.created_by_name,
                    l.creation_date_time,
                    l.curr_code,
                    null,
                    l.display_sequence_nbr,
                    l.end_date_time,
                    l.exception_flag,
                    l.hidden_line_flag,
                    l.invoice_line_level_flag,
                    l.item_to_price_flag,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    l.line_list_price_amt_ent,
                    l.negot_price_amt_ent,
                    l.negot_renew_price_amt_ent,
                    l.next_day_response_flag,
                    l.orig_sys_id,
                    l.orig_sys_ref_id,
                    l.orig_sys_source_code,
                    l.parent_contract_line_id,
                    l.price_basis_flag,
                    l.price_hold_adjust_est_pct,
                    l.price_hold_amt_ent,
                    l.price_hold_break_code,
                    l.price_hold_enfrc_list_flag,
                    l.price_hold_intgrtd_flag,
                    l.price_hold_list_ref_id,
                    l.price_hold_prchs_min_amt_ent,
                    l.price_hold_prchs_min_qty,
                    l.price_hold_price_type_code,
                    l.price_level_flag,
                    l.price_list_header_id,
                    l.price_list_line_id,
                    l.pricing_date_time,
                    l.renew_contract_line_id,
                    l.renew_curr_code,
                    l.renew_date_time,
                    l.renew_to_contract_line_id,
                    l.sot_order_sot_line_nbr,
                    l.start_date_time,
                    l.status_code,
                    l.template_used_name,
                    l.termination_code,
                    l.termination_date_time,
                    l.termination_reason_code,
                    l.top_model_contract_line_id,
                    t.tran_code,
                    l.unit_price_amt_ent,
                    l.unit_price_pct,
                    l.upg_orig_sys_ref_id,
                    l.upg_orig_sys_ref_name,
                    l.warranty_exp_reason_code,
                    l.bill_to_site_use_id,
                    l.cust_acct_id,
                    l.invoice_rule_id,
                    l.line_renewal_type_code,
                    l.ship_to_site_use_id,
                    l.holiday_coverage_code,
                    l.orig_serviced_qty,
                    l.serviced_qty,
                    t.reverse_logistics_text,
                    t.routing_comment_text,
                    t.source_location_name,
                    t.legacy_d1_severity_code,
                    t.crb_invoice_nbr_text,
                    t.crb_reference_nbr_text,
                    t.mdm_product_identifier,
                    t.mdm_solution_identifier  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_ld    AS l   ,
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t1    AS t   
                WHERE
                    l.contract_line_id = t.contract_line_id  
                    AND l.instance_id = t.instance_id   
                    AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query="""    UNCACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_dn_t1 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query="""    CACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_dn_t2 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        #print("Running Query: {}".format(query))
        #sparkSession.sql(query)
        query = """INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_ld
          SELECT  distinct Q1.* 
         FROM   """ + db_params.EEDDWWDDBB1 + """.contract_line_ld Q1 
                LEFT JOIN (SELECT contract_line_id, instance_id FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2) As Q2  
                       ON ( UPPER(TRIM(Q1.contract_line_id)) = UPPER(TRIM(Q2.contract_line_id))  And UPPER(TRIM(Q1.instance_id))=UPPER(TRIM(Q2.instance_id))) 
         WHERE  Q2.contract_line_id Is Null 
           And Q2.instance_id IS NULL"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_line_ld           SELECT  
                    contract_line_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    config_complete_flag,
                    config_header_id,
                    config_item_id,
                    config_item_type_code,
                    config_revision_nbr,
                    config_valid_flag,
                    contract_header_id,
                    contract_line_nbr,
                    contract_line_style_id,
                    contract_price_type_code,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    display_sequence_nbr,
                    end_date_time,
                    exception_flag,
                    hidden_line_flag,
                    invoice_line_level_flag,
                    item_to_price_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    line_list_price_amt_ent,
                    negot_price_amt_ent,
                    negot_renew_price_amt_ent,
                    next_day_response_flag,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    parent_contract_line_id,
                    price_basis_flag,
                    price_hold_adjust_est_pct,
                    price_hold_amt_ent,
                    price_hold_break_code,
                    price_hold_enfrc_list_flag,
                    price_hold_intgrtd_flag,
                    price_hold_list_ref_id,
                    price_hold_prchs_min_amt_ent,
                    price_hold_prchs_min_qty,
                    price_hold_price_type_code,
                    price_level_flag,
                    price_list_header_id,
                    price_list_line_id,
                    pricing_date_time,
                    renew_contract_line_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_line_id,
                    sot_order_sot_line_nbr,
                    start_date_time,
                    status_code,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    top_model_contract_line_id,
                    tran_code,
                    unit_price_amt_ent,
                    unit_price_pct,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    warranty_exp_reason_code,
                    bill_to_site_use_id,
                    cust_acct_id,
                    invoice_rule_id,
                    line_renewal_type_code,
                    ship_to_site_use_id,
                    holiday_coverage_code,
                    orig_serviced_qty,
                    serviced_qty,
                    routing_comment_text,
                    reverse_logistics_text,
                    source_location_name,
                    legacy_d1_severity_code,
                    crb_invoice_nbr_text,
                    crb_reference_nbr_text,
                    mdm_product_identifier,
                    mdm_solution_identifier  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_t2"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query="""    UNCACHE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_dn_t2 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        #print("Running Query: {}".format(query))
        #sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.EEDDWWDDBB + """.contract_line_ld COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,PARENT_CONTRACT_LINE_ID"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.EEDDWWDDBB + """.contract_line_ld COMPUTE STATISTICS  FOR COLUMNS contract_line_id,instance_id  , contract_line_id  , instance_id"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)


def main():
    m = d8contractlinemergesql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractlinemergesql").enableHiveSupport().getOrCreate()
    main()