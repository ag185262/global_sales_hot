from pyspark import SparkConf, SparkContext, SQLContext 
from pyspark.sql import SparkSession, HiveContext 
import sys 
import logging 
import db_params

class d8contractheadermergepnkjsql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml COMPUTE STATISTICS  FOR COLUMNS TRAN_CODE"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml COMPUTE STATISTICS  FOR COLUMNS STATUS_CODE"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t4"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """INSERT 
        INTO
        TABLE
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t4           
        SELECT distinct
        EDW1.contract_header_id,
        EDW1.instance_id,
        EDW1.as_of_date_time,
        EDW1.batch_nbr,
        EDW1.approved_date_time,
        EDW1.archived_flag,
        EDW1.as_of_date_time,
        EDW1.author_ncr_unit_id,
        EDW2.source_country_code,
        EDW1.author_operating_unit_id,
        EDW1.auto_renew_day_cnt,
        EDW1.award_contract_header_id,
        EDW1.batch_nbr,
        EDW1.cancelled_date_time,
        EDW1.character_type_code,
        EDW1.contract_est_amt_ent,
        EDW1.contract_intent_code,
        EDW1.contract_nbr,
        EDW1.contract_nbr_modifier,
        EDW1.contract_subtype_code,
        EDW1.created_by_name,
        EDW1.creation_date_time,
        EDW1.curr_code,
        EDW1.cust_po_nbr,
        EDW1.cust_po_nbr_rqrd_flag,
        EDW1.deleted_flag,
        EDW1.end_date_time,
        EDW1.est_renew_amt_ent,
        EDW1.gsa_code,
        EDW1.inventory_org_id,
        EDW1.issue_receive_code,
        EDW1.issued_date_time,
        EDW1.keep_on_mail_list_flag,
        EDW1.last_update_date_time,
        EDW1.last_update_login_name,
        EDW1.last_updated_by_name,
        EDW1.migration_contract_nbr,
        EDW1.orig_sys_id,
        EDW1.orig_sys_ref_id,
        EDW1.orig_sys_source_code,
        EDW1.prepay_rqrd_flag,
        EDW1.price_list_header_id,
        EDW1.pricing_date_time,
        EDW1.projtd_close_date_time,
        EDW1.proposed_date_time,
        EDW1.qa_check_list_id,
        EDW1.renew_contract_header_id,
        EDW1.renew_curr_code,
        EDW1.renew_date_time,
        EDW1.renew_to_contract_header_id,
        EDW1.resolved_until_date_time,
        EDW1.resp_contract_header_id,
        EDW1.respond_date_time,
        EDW1.response_rqrd_copy_nbr,
        EDW1.set_aside_pct,
        EDW1.sign_by_date_time,
        EDW1.signed_date_time,
        EDW1.solution_portfolio_id,
        EDW1.start_date_time,
        EDW1.status_code,
        EDW1.subclass_code,
        EDW1.template_flag,
        EDW1.template_used_name,
        EDW1.termination_code,
        EDW1.termination_date_time,
        EDW1.termination_reason_code,
        EDW1.total_list_price_amt_ent,
        EDW1.tran_code,
        EDW1.upg_orig_sys_ref_id,
        EDW1.upg_orig_sys_ref_name,
        EDW1.user_estimated_amt_ent,
        EDW1.agreement_record_id,
        EDW1.final_price_modifier_text,
        EDW1.invoice_zero_amt_flag,
        EDW1.bill_to_site_use_id,
        EDW1.curr_cnvrsn_rate,
        EDW1.curr_cnvrsn_rate_date,
        EDW1.curr_cnvrsn_type_code,
        EDW1.document_id,
        EDW1.invoice_rule_id,
        EDW1.invoice_term_id,
        EDW1.renewal_end_date,
        EDW1.renewal_type_code,
        EDW1.ship_to_site_use_id,
        EDW1.new_for_new_duration,
        EDW1.renewal_approval_type,
        EDW1.suspend_credits_flag,
        EDW1.hold_reason_description,
        EDW1.agreement_start_date,
        EDW1.agreement_end_date  
        FROM
        (
        SELECT  * FROM 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml 
        INNER JOIN
        (
        SELECT
        MAX(CONCAT(CAST(as_of_date_time AS CHAR(26)),
        batch_nbr)) AS auto_c00 ,
        instance_id AS auto_c01 ,
        contract_header_id AS auto_c02 
        FROM
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml 
        WHERE
        upper(trim(tran_code)) <> upper(trim('D')) 
        GROUP BY
        instance_id ,
        contract_header_id 
        ) AS autoAlias_25 
        ON (
        CONCAT(CAST(as_of_date_time AS CHAR(26)),
        batch_nbr) = autoAlias_25.AUTO_C00 
        AND upper(trim(instance_id)) = upper(trim(autoAlias_25.AUTO_C01)) 
        AND upper(trim(contract_header_id)) = upper(trim(autoAlias_25.AUTO_C02)) ) 
        WHERE
        upper(trim(tran_code)) <> 'D'
        ) AS EDW1   
        LEFT OUTER  JOIN
        """ + db_params.EEDDWWDDBB1 + """.country_instance_xref    AS EDW2    
        ON EDW1.author_ncr_unit_id = EDW2.instance_id  
        AND upper(trim(EDW2.data_source_code)) = upper(trim('FMS'))"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2           SELECT distinct
                    contract_header_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    approved_date_time,
                    archived_flag,
                    as_of_date_time,
                    author_ncr_unit_id,
                    author_oper_unit_country_code,
                    author_operating_unit_id,
                    auto_renew_day_cnt,
                    award_contract_header_id,
                    batch_nbr,
                    cancelled_date_time,
                    character_type_code,
                    contract_est_amt_ent,
                    contract_intent_code,
                    contract_nbr,
                    contract_nbr_modifier,
                    contract_subtype_code,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    curr_code,
                    cust_po_nbr,
                    cust_po_nbr_rqrd_flag,
                    deleted_flag,
                    end_date_time,
                    est_renew_amt_ent,
                    gsa_code,
                    inventory_org_id,
                    issue_receive_code,
                    issued_date_time,
                    keep_on_mail_list_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migration_contract_nbr,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    prepay_rqrd_flag,
                    price_list_header_id,
                    pricing_date_time,
                    projtd_close_date_time,
                    proposed_date_time,
                    qa_check_list_id,
                    renew_contract_header_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_header_id,
                    resolved_until_date_time,
                    resp_contract_header_id,
                    respond_date_time,
                    response_rqrd_copy_nbr,
                    set_aside_pct,
                    sign_by_date_time,
                    signed_date_time,
                    solution_portfolio_id,
                    start_date_time,
                    status_code,
                    subclass_code,
                    template_flag,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    total_list_price_amt_ent,
                    tran_code,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    user_estimated_amt_ent,
                    agreement_record_id,
                    final_price_modifier_text,
                    invoice_zero_amt_flag,
                    bill_to_site_use_id,
                    curr_cnvrsn_rate,
                    curr_cnvrsn_rate_date,
                    curr_cnvrsn_type_code,
                    document_id,
                    invoice_rule_id,
                    invoice_term_id,
                    renewal_end_date,
                    renewal_type_code,
                    ship_to_site_use_id,
                    new_for_new_duration,
                    renewal_approval_type,
                    suspend_credits_flag,
                    hold_reason_description,
                    agreement_start_date,
                    agreement_end_date  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_header_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT instance_id AS auto_c00,
                            contract_header_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t4 
                    ) AS autoAlias_29 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_29.AUTO_C00)) 
                            AND upper(trim(contract_header_id)) = upper(trim(autoAlias_29.AUTO_C01)) 
                        )"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t3"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t3           SELECT distinct
                    contract_header_id,
                    instance_id,
                    approved_date_time,
                    archived_flag,
                    author_ncr_unit_id,
                    author_oper_unit_country_code,
                    author_operating_unit_id,
                    auto_renew_day_cnt,
                    award_contract_header_id,
                    cancelled_date_time,
                    character_type_code,
                    contract_est_amt_ent,
                    contract_intent_code,
                    contract_nbr,
                    contract_nbr_modifier,
                    contract_subtype_code,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    cust_po_nbr,
                    cust_po_nbr_rqrd_flag,
                    deleted_flag,
                    end_date_time,
                    est_renew_amt_ent,
                    gsa_code,
                    inventory_org_id,
                    issue_receive_code,
                    issued_date_time,
                    keep_on_mail_list_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migration_contract_nbr,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    prepay_rqrd_flag,
                    price_list_header_id,
                    pricing_date_time,
                    projtd_close_date_time,
                    proposed_date_time,
                    qa_check_list_id,
                    renew_contract_header_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_header_id,
                    resolved_until_date_time,
                    resp_contract_header_id,
                    respond_date_time,
                    response_rqrd_copy_nbr,
                    set_aside_pct,
                    sign_by_date_time,
                    signed_date_time,
                    solution_portfolio_id,
                    start_date_time,
                    status_code,
                    subclass_code,
                    template_flag,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    total_list_price_amt_ent,
                    tran_code,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    user_estimated_amt_ent,
                    agreement_record_id,
                    final_price_modifier_text,
                    invoice_zero_amt_flag,
                    bill_to_site_use_id,
                    curr_cnvrsn_rate,
                    curr_cnvrsn_rate_date,
                    curr_cnvrsn_type_code,
                    document_id,
                    invoice_rule_id,
                    invoice_term_id,
                    renewal_end_date,
                    renewal_type_code,
                    ship_to_site_use_id,
                    new_for_new_duration,
                    renewal_approval_type,
                    suspend_credits_flag,
                    hold_reason_description,
                    agreement_start_date,
                    agreement_end_date  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT instance_id AS auto_c00,
                            contract_header_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t4 
                    ) AS autoAlias_31 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_31.AUTO_C00)) 
                            AND upper(trim(contract_header_id)) = upper(trim(autoAlias_31.AUTO_C01)) 
                        )"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t3           SELECT distinct
                    contract_header_id,
                    instance_id,
                    approved_date_time,
                    archived_flag,
                    author_ncr_unit_id,
                    author_oper_unit_country_code,
                    author_operating_unit_id,
                    auto_renew_day_cnt,
                    award_contract_header_id,
                    cancelled_date_time,
                    character_type_code,
                    contract_est_amt_ent,
                    contract_intent_code,
                    contract_nbr,
                    contract_nbr_modifier,
                    contract_subtype_code,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    cust_po_nbr,
                    cust_po_nbr_rqrd_flag,
                    deleted_flag,
                    end_date_time,
                    est_renew_amt_ent,
                    gsa_code,
                    inventory_org_id,
                    issue_receive_code,
                    issued_date_time,
                    keep_on_mail_list_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migration_contract_nbr,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    prepay_rqrd_flag,
                    price_list_header_id,
                    pricing_date_time,
                    projtd_close_date_time,
                    proposed_date_time,
                    qa_check_list_id,
                    renew_contract_header_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_header_id,
                    resolved_until_date_time,
                    resp_contract_header_id,
                    respond_date_time,
                    response_rqrd_copy_nbr,
                    set_aside_pct,
                    sign_by_date_time,
                    signed_date_time,
                    solution_portfolio_id,
                    start_date_time,
                    status_code,
                    subclass_code,
                    template_flag,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    total_list_price_amt_ent,
                    tran_code,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    user_estimated_amt_ent,
                    agreement_record_id,
                    final_price_modifier_text,
                    invoice_zero_amt_flag,
                    bill_to_site_use_id,
                    curr_cnvrsn_rate,
                    curr_cnvrsn_rate_date,
                    curr_cnvrsn_type_code,
                    document_id,
                    invoice_rule_id,
                    invoice_term_id,
                    renewal_end_date,
                    renewal_type_code,
                    ship_to_site_use_id,
                    new_for_new_duration,
                    renewal_approval_type,
                    suspend_credits_flag,
                    hold_reason_description,
                    agreement_start_date,
                    agreement_end_date  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t4 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT instance_id AS auto_c00,
                            contract_header_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2 
                    ) AS autoAlias_31 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_31.AUTO_C00)) 
                            AND upper(trim(contract_header_id)) = upper(trim(autoAlias_31.AUTO_C01)) 
                        )"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2 SELECT distinct
                    contract_header_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    approved_date_time,
                    archived_flag,
                    as_of_date_time,
                    author_ncr_unit_id,
                    author_oper_unit_country_code,
                    author_operating_unit_id,
                    auto_renew_day_cnt,
                    award_contract_header_id,
                    batch_nbr,
                    cancelled_date_time,
                    character_type_code,
                    contract_est_amt_ent,
                    contract_intent_code,
                    contract_nbr,
                    contract_nbr_modifier,
                    contract_subtype_code,
                    created_by_name,
                    creation_date_time,
                    (CASE 
                        WHEN(RCRD_UPD=1) THEN 'D' 
                        ELSE current_record_ind 
                    END) AS current_record_ind,
                    curr_code,
                    cust_po_nbr,
                    cust_po_nbr_rqrd_flag,
                    deleted_flag,
                    end_date_time,
                    est_renew_amt_ent,
                    gsa_code,
                    inventory_org_id,
                    issue_receive_code,
                    issued_date_time,
                    keep_on_mail_list_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migration_contract_nbr,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    prepay_rqrd_flag,
                    price_list_header_id,
                    pricing_date_time,
                    projtd_close_date_time,
                    proposed_date_time,
                    qa_check_list_id,
                    renew_contract_header_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_header_id,
                    resolved_until_date_time,
                    resp_contract_header_id,
                    respond_date_time,
                    response_rqrd_copy_nbr,
                    set_aside_pct,
                    sign_by_date_time,
                    signed_date_time,
                    solution_portfolio_id,
                    start_date_time,
                    status_code,
                    subclass_code,
                    template_flag,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    total_list_price_amt_ent,
                    tran_code,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    user_estimated_amt_ent,
                    agreement_record_id,
                    final_price_modifier_text,
                    invoice_zero_amt_flag,
                    bill_to_site_use_id,
                    curr_cnvrsn_rate,
                    curr_cnvrsn_rate_date,
                    curr_cnvrsn_type_code,
                    document_id,
                    invoice_rule_id,
                    invoice_term_id,
                    renewal_end_date,
                    renewal_type_code,
                    ship_to_site_use_id,
                    new_for_new_duration,
                    renewal_approval_type,
                    suspend_credits_flag,
                    hold_reason_description,
                    agreement_start_date,
                    agreement_end_date  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2 
                LEFT OUTER JOIN
                    (
                        SELECT
                            instance_id AS auto_c00,
                            contract_header_id AS auto_c01,
                            COUNT(*) AS RCRD_UPD  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t3 
                        GROUP BY
                            1,
                            2
                    ) AS autoAlias_7 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                            AND upper(trim(contract_header_id)) = upper(trim(autoAlias_7.AUTO_C01)) 
                        )"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2 COMPUTE STATISTICS  FOR COLUMNS CURRENT_RECORD_IND"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                 TABLE """ + db_params.EEDDWWDDBB1 + """.contract_header_ld 
                 SELECT distinct Q1.* FROM  """ + db_params.EEDDWWDDBB1 + """.contract_header_ld AS Q1
                 LEFT OUTER JOIN 
                        (
                        SELECT contract_header_id, instance_id
                        FROM
                         """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t4 AS Q2  
                        LEFT OUTER JOIN 
                            (   SELECT contract_header_id AS autoc000 , instance_id as autoc001
                            FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2 
                                WHERE current_record_ind = 'D' ) AS Q3 
                        ON Q2.contract_header_id = Q3.autoc000
                        AND Q2.instance_id = Q3.autoc001
                            WHERE Q3.autoc000 IS NULL 
                            AND Q3.autoc001 IS NULL
                        ) AS Q23
                 ON Q1.contract_header_id = Q23.contract_header_id
                 AND Q1.instance_id = Q23.instance_id
                    WHERE Q23.contract_header_id IS NULL 
                    AND Q23.instance_id IS NULL"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_header_ld           SELECT distinct
                    contract_header_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    approved_date_time,
                    archived_flag,
                    as_of_date_time,
                    author_ncr_unit_id,
                    author_oper_unit_country_code,
                    author_operating_unit_id,
                    auto_renew_day_cnt,
                    award_contract_header_id,
                    batch_nbr,
                    cancelled_date_time,
                    character_type_code,
                    contract_est_amt_ent,
                    contract_intent_code,
                    contract_nbr,
                    contract_nbr_modifier,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    cust_po_nbr,
                    cust_po_nbr_rqrd_flag,
                    deleted_flag,
                    end_date_time,
                    est_renew_amt_ent,
                    gsa_code,
                    inventory_org_id,
                    issue_receive_code,
                    issued_date_time,
                    keep_on_mail_list_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migration_contract_nbr,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    prepay_rqrd_flag,
                    price_list_header_id,
                    pricing_date_time,
                    projtd_close_date_time,
                    proposed_date_time,
                    qa_check_list_id,
                    renew_contract_header_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_header_id,
                    resolved_until_date_time,
                    resp_contract_header_id,
                    respond_date_time,
                    response_rqrd_copy_nbr,
                    set_aside_pct,
                    sign_by_date_time,
                    signed_date_time,
                    solution_portfolio_id,
                    start_date_time,
                    status_code,
                    subclass_code,
                    template_flag,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    total_list_price_amt_ent,
                    tran_code,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    user_estimated_amt_ent,
                    agreement_record_id,
                    final_price_modifier_text,
                    invoice_zero_amt_flag,
                    bill_to_site_use_id,
                    curr_cnvrsn_rate,
                    curr_cnvrsn_rate_date,
                    curr_cnvrsn_type_code,
                    document_id,
                    invoice_rule_id,
                    invoice_term_id,
                    renewal_end_date,
                    renewal_type_code,
                    ship_to_site_use_id,
                    contract_subtype_code,
                    new_for_new_duration,
                    renewal_approval_type,
                    suspend_credits_flag,
                    hold_reason_description,
                    agreement_start_date,
                    agreement_end_date  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t4 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT instance_id AS auto_c00,
                            contract_header_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_37 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_37.AUTO_C00)) 
                            AND upper(trim(contract_header_id)) = upper(trim(autoAlias_37.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_37.AUTO_C00 IS  NULL  
                    AND autoAlias_37.AUTO_C01 IS  NULL"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """INSERT 
        OVERWRITE
        TABLE
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2           
        SELECT DISTINCT 
        EDW1.contract_header_id,
        EDW1.instance_id,
        EDW2.as_of_date_time,
        EDW2.batch_nbr,
        EDW1.approved_date_time,
        EDW1.archived_flag,
        EDW1.as_of_date_time,
        EDW1.author_ncr_unit_id,
        EDW1.author_oper_unit_country_code,
        EDW1.author_operating_unit_id,
        EDW1.auto_renew_day_cnt,
        EDW1.award_contract_header_id,
        EDW1.batch_nbr,
        EDW1.cancelled_date_time,
        EDW1.character_type_code,
        EDW1.contract_est_amt_ent,
        EDW1.contract_intent_code,
        EDW1.contract_nbr,
        EDW1.contract_nbr_modifier,
        EDW1.contract_subtype_code,
        EDW1.created_by_name,
        EDW1.creation_date_time,
        null,
        EDW1.curr_code,
        EDW1.cust_po_nbr,
        EDW1.cust_po_nbr_rqrd_flag,
        EDW1.deleted_flag,
        EDW1.end_date_time,
        EDW1.est_renew_amt_ent,
        EDW1.gsa_code,
        EDW1.inventory_org_id,
        EDW1.issue_receive_code,
        EDW1.issued_date_time,
        EDW1.keep_on_mail_list_flag,
        EDW1.last_update_date_time,
        EDW1.last_update_login_name,
        EDW1.last_updated_by_name,
        EDW1.migration_contract_nbr,
        EDW1.orig_sys_id,
        EDW1.orig_sys_ref_id,
        EDW1.orig_sys_source_code,
        EDW1.prepay_rqrd_flag,
        EDW1.price_list_header_id,
        EDW1.pricing_date_time,
        EDW1.projtd_close_date_time,
        EDW1.proposed_date_time,
        EDW1.qa_check_list_id,
        EDW1.renew_contract_header_id,
        EDW1.renew_curr_code,
        EDW1.renew_date_time,
        EDW1.renew_to_contract_header_id,
        EDW1.resolved_until_date_time,
        EDW1.resp_contract_header_id,
        EDW1.respond_date_time,
        EDW1.response_rqrd_copy_nbr,
        EDW1.set_aside_pct,
        EDW1.sign_by_date_time,
        EDW1.signed_date_time,
        EDW1.solution_portfolio_id,
        EDW1.start_date_time,
        EDW1.status_code,
        EDW1.subclass_code,
        EDW1.template_flag,
        EDW1.template_used_name,
        EDW1.termination_code,
        EDW1.termination_date_time,
        EDW1.termination_reason_code,
        EDW1.total_list_price_amt_ent,
        EDW2.tran_code,
        EDW1.upg_orig_sys_ref_id,
        EDW1.upg_orig_sys_ref_name,
        EDW1.user_estimated_amt_ent,
        EDW1.agreement_record_id,
        EDW1.final_price_modifier_text,
        EDW1.invoice_zero_amt_flag,
        EDW1.bill_to_site_use_id,
        EDW1.curr_cnvrsn_rate,
        EDW1.curr_cnvrsn_rate_date,
        EDW1.curr_cnvrsn_type_code,
        EDW1.document_id,
        EDW1.invoice_rule_id,
        EDW1.invoice_term_id,
        EDW1.renewal_end_date,
        EDW1.renewal_type_code,
        EDW1.ship_to_site_use_id,
        EDW1.new_for_new_duration,
        EDW1.renewal_approval_type,
        EDW1.suspend_credits_flag,
        EDW1.hold_reason_description,
        EDW1.agreement_start_date,
        EDW1.agreement_end_date  
        FROM
        (
        SELECT * FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml 
        INNER JOIN
        (
        SELECT
        MAX(CONCAT(CAST(as_of_date_time AS CHAR(26)),
        batch_nbr)) AS auto_c00 ,
        instance_id AS auto_c01 ,
        contract_header_id AS auto_c02 
        FROM
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml 
        WHERE
        upper(trim(tran_code)) = upper(trim('D')) 
        GROUP BY
        instance_id ,
        contract_header_id 
        ) AS autoAlias_27 
        ON (
        CONCAT(CAST(as_of_date_time AS CHAR(26)),
        batch_nbr) = autoAlias_27.AUTO_C00 
        AND upper(trim(instance_id)) = upper(trim(autoAlias_27.AUTO_C01)) 
        AND upper(trim(contract_header_id)) = upper(trim(autoAlias_27.AUTO_C02)) ) 
        WHERE
        upper(trim(tran_code)) = upper(trim('D'))
        ) AS EDW2   
        INNER JOIN """ + db_params.EEDDWWDDBB1 + """.contract_header_ld    AS EDW1   
        ON EDW1.instance_id = EDW2.instance_id  
        AND EDW1.contract_header_id = EDW2.contract_header_id   
        WHERE  CONCAT (CAST (EDW1.as_of_date_time AS VARCHAR(26)) , EDW1.batch_nbr) <  CONCAT (CAST (EDW2.as_of_date_time AS VARCHAR(26)) , EDW2.batch_nbr)"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_header_ld SELECT distinct
                    contract_header_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    approved_date_time,
                    archived_flag,
                    as_of_date_time,
                    author_ncr_unit_id,
                    author_oper_unit_country_code,
                    author_operating_unit_id,
                    auto_renew_day_cnt,
                    award_contract_header_id,
                    batch_nbr,
                    cancelled_date_time,
                    character_type_code,
                    contract_est_amt_ent,
                    contract_intent_code,
                    contract_nbr,
                    contract_nbr_modifier,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    cust_po_nbr,
                    cust_po_nbr_rqrd_flag,
                    deleted_flag,
                    end_date_time,
                    est_renew_amt_ent,
                    gsa_code,
                    inventory_org_id,
                    issue_receive_code,
                    issued_date_time,
                    keep_on_mail_list_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migration_contract_nbr,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    prepay_rqrd_flag,
                    price_list_header_id,
                    pricing_date_time,
                    projtd_close_date_time,
                    proposed_date_time,
                    qa_check_list_id,
                    renew_contract_header_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_header_id,
                    resolved_until_date_time,
                    resp_contract_header_id,
                    respond_date_time,
                    response_rqrd_copy_nbr,
                    set_aside_pct,
                    sign_by_date_time,
                    signed_date_time,
                    solution_portfolio_id,
                    start_date_time,
                    status_code,
                    subclass_code,
                    template_flag,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    total_list_price_amt_ent,
                    tran_code,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    user_estimated_amt_ent,
                    agreement_record_id,
                    final_price_modifier_text,
                    invoice_zero_amt_flag,
                    bill_to_site_use_id,
                    curr_cnvrsn_rate,
                    curr_cnvrsn_rate_date,
                    curr_cnvrsn_type_code,
                    document_id,
                    invoice_rule_id,
                    invoice_term_id,
                    renewal_end_date,
                    renewal_type_code,
                    ship_to_site_use_id,
                    contract_subtype_code,
                    new_for_new_duration,
                    renewal_approval_type,
                    suspend_credits_flag,
                    hold_reason_description,
                    agreement_start_date,
                    agreement_end_date 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_header_ld 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT instance_id AS auto_c00,
                            contract_header_id AS auto_c01 
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2
                    ) AS autoAlias_7 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                            AND upper(trim(contract_header_id)) = upper(trim(autoAlias_7.AUTO_C01)) 
                        ) 
                WHERE
                    (
                        autoAlias_7.AUTO_C00 IS NULL 
                        AND autoAlias_7.AUTO_C01 IS NULL
                    )"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_header_ld           SELECT distinct
                    contract_header_id,
                    instance_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    approved_date_time,
                    archived_flag,
                    as_of_date_time,
                    author_ncr_unit_id,
                    author_oper_unit_country_code,
                    author_operating_unit_id,
                    auto_renew_day_cnt,
                    award_contract_header_id,
                    batch_nbr,
                    cancelled_date_time,
                    character_type_code,
                    contract_est_amt_ent,
                    contract_intent_code,
                    contract_nbr,
                    contract_nbr_modifier,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    cust_po_nbr,
                    cust_po_nbr_rqrd_flag,
                    deleted_flag,
                    end_date_time,
                    est_renew_amt_ent,
                    gsa_code,
                    inventory_org_id,
                    issue_receive_code,
                    issued_date_time,
                    keep_on_mail_list_flag,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    migration_contract_nbr,
                    orig_sys_id,
                    orig_sys_ref_id,
                    orig_sys_source_code,
                    prepay_rqrd_flag,
                    price_list_header_id,
                    pricing_date_time,
                    projtd_close_date_time,
                    proposed_date_time,
                    qa_check_list_id,
                    renew_contract_header_id,
                    renew_curr_code,
                    renew_date_time,
                    renew_to_contract_header_id,
                    resolved_until_date_time,
                    resp_contract_header_id,
                    respond_date_time,
                    response_rqrd_copy_nbr,
                    set_aside_pct,
                    sign_by_date_time,
                    signed_date_time,
                    solution_portfolio_id,
                    start_date_time,
                    status_code,
                    subclass_code,
                    template_flag,
                    template_used_name,
                    termination_code,
                    termination_date_time,
                    termination_reason_code,
                    total_list_price_amt_ent,
                    tran_code,
                    upg_orig_sys_ref_id,
                    upg_orig_sys_ref_name,
                    user_estimated_amt_ent,
                    agreement_record_id,
                    final_price_modifier_text,
                    invoice_zero_amt_flag,
                    bill_to_site_use_id,
                    curr_cnvrsn_rate,
                    curr_cnvrsn_rate_date,
                    curr_cnvrsn_type_code,
                    document_id,
                    invoice_rule_id,
                    invoice_term_id,
                    renewal_end_date,
                    renewal_type_code,
                    ship_to_site_use_id,
                    contract_subtype_code,
                    new_for_new_duration,
                    renewal_approval_type,
                    null,
                    hold_reason_description,
                    agreement_start_date,
                    agreement_end_date  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t2"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """use """ + db_params.TTMMPPDDBB1 + """"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_as_of_date           SELECT
                    '99',
                    instance_id,
                    MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml      
                GROUP BY
                    '99' ,
                    instance_id"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """
            INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_as_of_date           SELECT
                    author_oper_unit_country_code,
                    instance_id,
                    MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_t4      
                GROUP BY
                    author_oper_unit_country_code ,
                    instance_id"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """
            WITH qq1 AS (    SELECT distinct
                instance_id,
                batch_nbr  
            FROM
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml 
            LEFT OUTER JOIN
                (SELECT
                    DISTINCT instance_id AS auto_c00,
                    batch_nbr AS auto_c01  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_batch_nbr_trig ) AS autoAlias_7 
                    ON ( upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                    AND batch_nbr = autoAlias_7.AUTO_C01 ) 
            WHERE
                autoAlias_7.AUTO_C00 IS  NULL  
                AND autoAlias_7.AUTO_C01 IS  NULL         ) 
        		INSERT 
                INTO
                    TABLE
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_batch_nbr_trig       SELECT
                        * 
                    FROM
                        qq1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    WITH qq1 AS (    SELECT distinct
                instance_id,
                batch_nbr  
            FROM
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_ml 
            LEFT OUTER JOIN
                (SELECT
                    DISTINCT instance_id AS auto_c00,
                    batch_nbr AS auto_c01  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_batch_nbr_trig ) AS autoAlias_7 
                    ON ( upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                    AND batch_nbr = autoAlias_7.AUTO_C01 ) 
            WHERE
                autoAlias_7.AUTO_C00 IS  NULL  
                AND autoAlias_7.AUTO_C01 IS  NULL         ) 
        		INSERT 
                INTO
                    TABLE
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_batch_nbr_trig       SELECT
                        * 
                    FROM
                        qq1"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)


def main():
    m = d8contractheadermergepnkjsql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractheadermergepnkjsql").enableHiveSupport().getOrCreate()
    main()