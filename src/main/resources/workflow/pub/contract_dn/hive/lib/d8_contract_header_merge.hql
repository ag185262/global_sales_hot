----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_header_01.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --COLLECT STATISTICS ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml COLUMN TRAN_CODE

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml COMPUTE STATISTICS  FOR COLUMNS TRAN_CODE;


--Original Query:
  --COLLECT STATISTICS ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml COLUMN STATUS_CODE

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml COMPUTE STATISTICS  FOR COLUMNS STATUS_CODE;


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 ALL

--Translated Query: SUCCESS

    --TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1;


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1(        contract_header_id,        instance_id,        approved_date_time,        archived_flag,        as_of_date_time,        author_ncr_unit_id,        author_operating_unit_id,        auto_renew_day_cnt,        award_contract_header_id,        batch_nbr,        cancelled_date_time,        character_type_code,        contract_est_amt_ent,        contract_intent_code,        contract_nbr,        contract_nbr_modifier,        contract_subtype_code,        created_by_name,        creation_date_time,        curr_code,        cust_po_nbr,        cust_po_nbr_rqrd_flag,        deleted_flag,        end_date_time,        est_renew_amt_ent,        gsa_code,        inventory_org_id,        issue_receive_code,        issued_date_time,        keep_on_mail_list_flag,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        migration_contract_nbr,        orig_sys_id,        orig_sys_ref_id,        orig_sys_source_code,        prepay_rqrd_flag,        price_list_header_id,        pricing_date_time,        projtd_close_date_time,        proposed_date_time,        qa_check_list_id,        renew_contract_header_id,        renew_curr_code,        renew_date_time,        renew_to_contract_header_id,        resolved_until_date_time,        resp_contract_header_id,        respond_date_time,        response_rqrd_copy_nbr,        set_aside_pct,        sign_by_date_time,        signed_date_time,        solution_portfolio_id,        start_date_time,        status_code,        subclass_code,        template_flag,        template_used_name,        termination_date_time,        termination_reason_code,        termination_code,        total_list_price_amt_ent,        tran_code,        upg_orig_sys_ref_name,        upg_orig_sys_ref_id,        user_estimated_amt_ent,        agreement_record_id,        final_price_modifier_text,        invoice_zero_amt_flag,        bill_to_site_use_id,        curr_cnvrsn_rate,        curr_cnvrsn_rate_date,        curr_cnvrsn_type_code,        document_id,        invoice_rule_id,        invoice_term_id,        renewal_end_date,        renewal_type_code,        ship_to_site_use_id, new_for_new_duration,   renewal_approval_type,        suspend_credits_flag)SELECT        contract_header_id,        instance_id,        approved_date_time,        archived_flag,        as_of_date_time,        author_ncr_unit_id,        author_operating_unit_id,        auto_renew_day_cnt,        award_contract_header_id,        batch_nbr,        cancelled_date_time,        character_type_code,        contract_est_amt_ent,        contract_intent_code,        contract_nbr,        contract_nbr_modifier,        contract_subtype_code,        created_by_name,        creation_date_time,        curr_code,        cust_po_nbr,        cust_po_nbr_rqrd_flag,        deleted_flag,        end_date_time,        est_renew_amt_ent,        gsa_code,        inventory_org_id,        issue_receive_code,        issued_date_time,        keep_on_mail_list_flag,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        migration_contract_nbr,        orig_sys_id,        orig_sys_ref_id,        orig_sys_source_code,        prepay_rqrd_flag,        price_list_header_id,        pricing_date_time,        projtd_close_date_time,        proposed_date_time,        qa_check_list_id,        renew_contract_header_id,        renew_curr_code,        renew_date_time,        renew_to_contract_header_id,        resolved_until_date_time,        resp_contract_header_id,        respond_date_time,        response_rqrd_copy_nbr,        set_aside_pct,        sign_by_date_time,        signed_date_time,        solution_portfolio_id,        start_date_time,        status_code,        subclass_code,        template_flag,        template_used_name,        termination_date_time,        termination_reason_code,        termination_code,        total_list_price_amt_ent,        tran_code,        upg_orig_sys_ref_name,        upg_orig_sys_ref_id,        user_estimated_amt_ent,        agreement_record_id,        final_price_modifier_text,        invoice_zero_amt_flag,        bill_to_site_use_id,        curr_cnvrsn_rate,        curr_cnvrsn_rate_date,        curr_cnvrsn_type_code,        document_id,        invoice_rule_id,        invoice_term_id,        renewal_end_date,        renewal_type_code,        ship_to_site_use_id,    new_for_new_duration,   renewal_approval_type,        suspend_credits_flagFROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_mlWHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,        instance_id,        contract_header_id)IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),        instance_id,        contract_header_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml    WHERE tran_code <> 'D'    GROUP BY 2,3 )AND tran_code <> 'D'

--Translated Query: SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 SELECT distinct
            -- contract_header_id ,
            -- instance_id ,
            -- approved_date_time ,
            -- archived_flag ,
            -- as_of_date_time ,
            -- author_ncr_unit_id ,
            -- author_operating_unit_id ,
            -- auto_renew_day_cnt ,
            -- award_contract_header_id ,
            -- batch_nbr ,
            -- cancelled_date_time ,
            -- character_type_code ,
            -- contract_est_amt_ent ,
            -- contract_intent_code ,
            -- contract_nbr ,
            -- contract_nbr_modifier ,
            -- contract_subtype_code ,
            -- created_by_name ,
            -- creation_date_time ,
            -- curr_code ,
            -- cust_po_nbr ,
            -- cust_po_nbr_rqrd_flag ,
            -- deleted_flag ,
            -- end_date_time ,
            -- est_renew_amt_ent ,
            -- gsa_code ,
            -- inventory_org_id ,
            -- issue_receive_code ,
            -- issued_date_time ,
            -- keep_on_mail_list_flag ,
            -- last_update_date_time ,
            -- last_update_login_name ,
            -- last_updated_by_name ,
            -- migration_contract_nbr ,
            -- orig_sys_id ,
            -- orig_sys_ref_id ,
            -- orig_sys_source_code ,
            -- prepay_rqrd_flag ,
            -- price_list_header_id ,
            -- pricing_date_time ,
            -- projtd_close_date_time ,
            -- proposed_date_time ,
            -- qa_check_list_id ,
            -- renew_contract_header_id ,
            -- renew_curr_code ,
            -- renew_date_time ,
            -- renew_to_contract_header_id ,
            -- resolved_until_date_time ,
            -- resp_contract_header_id ,
            -- respond_date_time ,
            -- response_rqrd_copy_nbr ,
            -- set_aside_pct ,
            -- sign_by_date_time ,
            -- signed_date_time ,
            -- solution_portfolio_id ,
            -- start_date_time ,
            -- status_code ,
            -- subclass_code ,
            -- template_flag ,
            -- template_used_name ,
            -- termination_code ,
            -- termination_date_time ,
            -- termination_reason_code ,
            -- total_list_price_amt_ent ,
            -- tran_code ,
            -- upg_orig_sys_ref_id ,
            -- upg_orig_sys_ref_name ,
            -- user_estimated_amt_ent ,
            -- agreement_record_id ,
            -- final_price_modifier_text ,
            -- invoice_zero_amt_flag ,
            -- bill_to_site_use_id ,
            -- curr_cnvrsn_rate ,
            -- curr_cnvrsn_rate_date ,
            -- curr_cnvrsn_type_code ,
            -- document_id ,
            -- invoice_rule_id ,
            -- invoice_term_id ,
            -- renewal_end_date ,
            -- renewal_type_code ,
            -- ship_to_site_use_id ,
            -- new_for_new_duration ,
            -- renewal_approval_type ,
            -- suspend_credits_flag ,
            -- hold_reason_description ,
            -- agreement_start_date ,
            -- agreement_end_date 
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(CONCAT(CAST(as_of_date_time AS CHAR(26)),
                    -- batch_nbr)) AS auto_c00 ,
                    -- instance_id AS auto_c01 ,
                    -- contract_header_id AS auto_c02 
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml 
                -- WHERE
                    -- upper(trim(tran_code)) <> upper(trim('D')) 
                -- GROUP BY
                    -- instance_id ,
                    -- contract_header_id 
            -- ) AS autoAlias_25 
                -- ON (
                    -- CONCAT(CAST(as_of_date_time AS CHAR(26)),
                -- batch_nbr) = autoAlias_25.AUTO_C00 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_25.AUTO_C01)) 
                -- AND upper(trim(contract_header_id)) = upper(trim(autoAlias_25.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) <> upper(trim('D'));


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4;


--Original Query:
  --COLLECT STATISTICS ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 COLUMN AUTHOR_NCR_UNIT_ID

--Translated Query: SUCCESS

    --ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 COMPUTE STATISTICS  FOR COLUMNS AUTHOR_NCR_UNIT_ID;


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4(        contract_header_id,        instance_id,        alt_as_of_date_time,        alt_batch_nbr,        approved_date_time,        archived_flag,        as_of_date_time,        author_ncr_unit_id,        author_oper_unit_country_code,        author_operating_unit_id,        auto_renew_day_cnt,        award_contract_header_id,        batch_nbr,        cancelled_date_time,        character_type_code,        contract_est_amt_ent,        contract_intent_code,        contract_nbr,        contract_nbr_modifier,        contract_subtype_code,        created_by_name,        creation_date_time,        curr_code,        cust_po_nbr,        cust_po_nbr_rqrd_flag,        deleted_flag,        end_date_time,        est_renew_amt_ent,        gsa_code,        inventory_org_id,        issue_receive_code,        issued_date_time,        keep_on_mail_list_flag,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        migration_contract_nbr,        orig_sys_id,        orig_sys_ref_id,        orig_sys_source_code,        prepay_rqrd_flag,        price_list_header_id,        pricing_date_time,        projtd_close_date_time,        proposed_date_time,        qa_check_list_id,        renew_contract_header_id,        renew_curr_code,        renew_date_time,        renew_to_contract_header_id,        resolved_until_date_time,        resp_contract_header_id,        respond_date_time,        response_rqrd_copy_nbr,        set_aside_pct,        sign_by_date_time,        signed_date_time,        solution_portfolio_id,        start_date_time,        status_code,        subclass_code,        template_flag,        template_used_name,        termination_date_time,        termination_reason_code,        termination_code,        total_list_price_amt_ent,        tran_code,        upg_orig_sys_ref_name,        upg_orig_sys_ref_id,        user_estimated_amt_ent,        agreement_record_id,        final_price_modifier_text,        invoice_zero_amt_flag,        bill_to_site_use_id,        curr_cnvrsn_rate,        curr_cnvrsn_rate_date,        curr_cnvrsn_type_code,        document_id,        invoice_rule_id,        invoice_term_id,        renewal_end_date,        renewal_type_code,        ship_to_site_use_id, new_for_new_duration,        renewal_approval_type,        suspend_credits_flag)SELECT        EDW1.contract_header_id,        EDW1.instance_id,        EDW1.as_of_date_time,        EDW1.batch_nbr,        EDW1.approved_date_time,        EDW1.archived_flag,        EDW1.as_of_date_time,        EDW1.author_ncr_unit_id,        EDW2.source_country_code,        EDW1.author_operating_unit_id,        EDW1.auto_renew_day_cnt,        EDW1.award_contract_header_id,        EDW1.batch_nbr,        EDW1.cancelled_date_time,        EDW1.character_type_code,        EDW1.contract_est_amt_ent,        EDW1.contract_intent_code,        EDW1.contract_nbr,        EDW1.contract_nbr_modifier,        EDW1.contract_subtype_code,        EDW1.created_by_name,        EDW1.creation_date_time,        EDW1.curr_code,        EDW1.cust_po_nbr,        EDW1.cust_po_nbr_rqrd_flag,        EDW1.deleted_flag,        EDW1.end_date_time,        EDW1.est_renew_amt_ent,        EDW1.gsa_code,        EDW1.inventory_org_id,        EDW1.issue_receive_code,        EDW1.issued_date_time,        EDW1.keep_on_mail_list_flag,        EDW1.last_update_date_time,        EDW1.last_update_login_name,        EDW1.last_updated_by_name,        EDW1.migration_contract_nbr,        EDW1.orig_sys_id,        EDW1.orig_sys_ref_id,        EDW1.orig_sys_source_code,        EDW1.prepay_rqrd_flag,        EDW1.price_list_header_id,        EDW1.pricing_date_time,        EDW1.projtd_close_date_time,        EDW1.proposed_date_time,        EDW1.qa_check_list_id,        EDW1.renew_contract_header_id,        EDW1.renew_curr_code,        EDW1.renew_date_time,        EDW1.renew_to_contract_header_id,        EDW1.resolved_until_date_time,        EDW1.resp_contract_header_id,        EDW1.respond_date_time,        EDW1.response_rqrd_copy_nbr,        EDW1.set_aside_pct,        EDW1.sign_by_date_time,        EDW1.signed_date_time,        EDW1.solution_portfolio_id,        EDW1.start_date_time,        EDW1.status_code,        EDW1.subclass_code,        EDW1.template_flag,        EDW1.template_used_name,        EDW1.termination_date_time,        EDW1.termination_reason_code,        EDW1.termination_code,        EDW1.total_list_price_amt_ent,        EDW1.tran_code,        EDW1.upg_orig_sys_ref_name,        EDW1.upg_orig_sys_ref_id,        EDW1.user_estimated_amt_ent,        EDW1.agreement_record_id,        EDW1.final_price_modifier_text,        EDW1.invoice_zero_amt_flag,        EDW1.bill_to_site_use_id,        EDW1.curr_cnvrsn_rate,        EDW1.curr_cnvrsn_rate_date,        EDW1.curr_cnvrsn_type_code,        EDW1.document_id,        EDW1.invoice_rule_id,        EDW1.invoice_term_id,        EDW1.renewal_end_date,        EDW1.renewal_type_code,        EDW1.ship_to_site_use_id, EDW1.new_for_new_duration,        EDW1.renewal_approval_type,        EDW1.suspend_credits_flagFROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 EDW1     LEFT OUTER JOIN ${EEDDWWDDBB1}.country_instance_xref EDW2ON EDW1.author_ncr_unit_id = EDW2.instance_idAND   EDW2.data_source_code = 'FMS'

--Translated Query: --pankaj : merged T1 & T4

INSERT 
INTO
TABLE
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4           
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
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml 
INNER JOIN
(
SELECT
MAX(CONCAT(CAST(as_of_date_time AS CHAR(26)),
batch_nbr)) AS auto_c00 ,
instance_id AS auto_c01 ,
contract_header_id AS auto_c02 
FROM
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml 
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
${EEDDWWDDBB1}.country_instance_xref    AS EDW2    
ON EDW1.author_ncr_unit_id = EDW2.instance_id  
AND upper(trim(EDW2.data_source_code)) = upper(trim('FMS'));


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 ALL

--Translated Query: SUCCESS

    --TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1;


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1(        contract_header_id,        instance_id,        approved_date_time,        archived_flag,        as_of_date_time,        author_ncr_unit_id,        author_operating_unit_id,        auto_renew_day_cnt,        award_contract_header_id,        batch_nbr,        cancelled_date_time,        character_type_code,        contract_est_amt_ent,        contract_intent_code,        contract_nbr,        contract_nbr_modifier,        contract_subtype_code,        created_by_name,        creation_date_time,        curr_code,        cust_po_nbr,        cust_po_nbr_rqrd_flag,        deleted_flag,        end_date_time,        est_renew_amt_ent,        gsa_code,        inventory_org_id,        issue_receive_code,        issued_date_time,        keep_on_mail_list_flag,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        migration_contract_nbr,        orig_sys_id,        orig_sys_ref_id,        orig_sys_source_code,        prepay_rqrd_flag,        price_list_header_id,        pricing_date_time,        projtd_close_date_time,        proposed_date_time,        qa_check_list_id,        renew_contract_header_id,        renew_curr_code,        renew_date_time,        renew_to_contract_header_id,        resolved_until_date_time,        resp_contract_header_id,        respond_date_time,        response_rqrd_copy_nbr,        set_aside_pct,        sign_by_date_time,        signed_date_time,        solution_portfolio_id,        start_date_time,        status_code,        subclass_code,        template_flag,        template_used_name,        termination_date_time,        termination_reason_code,        termination_code,        total_list_price_amt_ent,        tran_code,        upg_orig_sys_ref_name,        upg_orig_sys_ref_id,        user_estimated_amt_ent,        agreement_record_id,        final_price_modifier_text,        invoice_zero_amt_flag,        bill_to_site_use_id,        curr_cnvrsn_rate,        curr_cnvrsn_rate_date,        curr_cnvrsn_type_code,        document_id,        invoice_rule_id,        invoice_term_id,        renewal_end_date,        renewal_type_code,        ship_to_site_use_id, new_for_new_duration,        renewal_approval_type,        suspend_credits_flag)SELECT        contract_header_id,        instance_id,        approved_date_time,        archived_flag,        as_of_date_time,        author_ncr_unit_id,        author_operating_unit_id,        auto_renew_day_cnt,        award_contract_header_id,        batch_nbr,        cancelled_date_time,        character_type_code,        contract_est_amt_ent,        contract_intent_code,        contract_nbr,        contract_nbr_modifier,        contract_subtype_code,        created_by_name,        creation_date_time,        curr_code,        cust_po_nbr,        cust_po_nbr_rqrd_flag,        deleted_flag,        end_date_time,        est_renew_amt_ent,        gsa_code,        inventory_org_id,        issue_receive_code,        issued_date_time,        keep_on_mail_list_flag,        last_update_date_time,        last_update_login_name,        last_updated_by_name,        migration_contract_nbr,        orig_sys_id,        orig_sys_ref_id,        orig_sys_source_code,        prepay_rqrd_flag,        price_list_header_id,        pricing_date_time,        projtd_close_date_time,        proposed_date_time,        qa_check_list_id,        renew_contract_header_id,        renew_curr_code,        renew_date_time,        renew_to_contract_header_id,        resolved_until_date_time,        resp_contract_header_id,        respond_date_time,        response_rqrd_copy_nbr,        set_aside_pct,        sign_by_date_time,        signed_date_time,        solution_portfolio_id,        start_date_time,        status_code,        subclass_code,        template_flag,        template_used_name,        termination_date_time,        termination_reason_code,        termination_code,        total_list_price_amt_ent,        tran_code,        upg_orig_sys_ref_name,        upg_orig_sys_ref_id,        user_estimated_amt_ent,        agreement_record_id,        final_price_modifier_text,        invoice_zero_amt_flag,        bill_to_site_use_id,        curr_cnvrsn_rate,        curr_cnvrsn_rate_date,        curr_cnvrsn_type_code,        document_id,        invoice_rule_id,        invoice_term_id,        renewal_end_date,        renewal_type_code,        ship_to_site_use_id,   new_for_new_duration,        renewal_approval_type,        suspend_credits_flagFROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_mlWHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,        instance_id,        contract_header_id)IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),        instance_id,        contract_header_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml    WHERE tran_code = 'D'    GROUP BY 2,3 )AND tran_code = 'D'

--Translated Query: SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 SELECT distinct
            -- contract_header_id ,
            -- instance_id ,
            -- approved_date_time ,
            -- archived_flag ,
            -- as_of_date_time ,
            -- author_ncr_unit_id ,
            -- author_operating_unit_id ,
            -- auto_renew_day_cnt ,
            -- award_contract_header_id ,
            -- batch_nbr ,
            -- cancelled_date_time ,
            -- character_type_code ,
            -- contract_est_amt_ent ,
            -- contract_intent_code ,
            -- contract_nbr ,
            -- contract_nbr_modifier ,
            -- contract_subtype_code ,
            -- created_by_name ,
            -- creation_date_time ,
            -- curr_code ,
            -- cust_po_nbr ,
            -- cust_po_nbr_rqrd_flag ,
            -- deleted_flag ,
            -- end_date_time ,
            -- est_renew_amt_ent ,
            -- gsa_code ,
            -- inventory_org_id ,
            -- issue_receive_code ,
            -- issued_date_time ,
            -- keep_on_mail_list_flag ,
            -- last_update_date_time ,
            -- last_update_login_name ,
            -- last_updated_by_name ,
            -- migration_contract_nbr ,
            -- orig_sys_id ,
            -- orig_sys_ref_id ,
            -- orig_sys_source_code ,
            -- prepay_rqrd_flag ,
            -- price_list_header_id ,
            -- pricing_date_time ,
            -- projtd_close_date_time ,
            -- proposed_date_time ,
            -- qa_check_list_id ,
            -- renew_contract_header_id ,
            -- renew_curr_code ,
            -- renew_date_time ,
            -- renew_to_contract_header_id ,
            -- resolved_until_date_time ,
            -- resp_contract_header_id ,
            -- respond_date_time ,
            -- response_rqrd_copy_nbr ,
            -- set_aside_pct ,
            -- sign_by_date_time ,
            -- signed_date_time ,
            -- solution_portfolio_id ,
            -- start_date_time ,
            -- status_code ,
            -- subclass_code ,
            -- template_flag ,
            -- template_used_name ,
            -- termination_code ,
            -- termination_date_time ,
            -- termination_reason_code ,
            -- total_list_price_amt_ent ,
            -- tran_code ,
            -- upg_orig_sys_ref_id ,
            -- upg_orig_sys_ref_name ,
            -- user_estimated_amt_ent ,
            -- agreement_record_id ,
            -- final_price_modifier_text ,
            -- invoice_zero_amt_flag ,
            -- bill_to_site_use_id ,
            -- curr_cnvrsn_rate ,
            -- curr_cnvrsn_rate_date ,
            -- curr_cnvrsn_type_code ,
            -- document_id ,
            -- invoice_rule_id ,
            -- invoice_term_id ,
            -- renewal_end_date ,
            -- renewal_type_code ,
            -- ship_to_site_use_id ,
            -- new_for_new_duration ,
            -- renewal_approval_type ,
            -- suspend_credits_flag ,
            -- hold_reason_description ,
            -- agreement_start_date ,
            -- agreement_end_date 
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(CONCAT(CAST(as_of_date_time AS CHAR(26)),
                    -- batch_nbr)) AS auto_c00 ,
                    -- instance_id AS auto_c01 ,
                    -- contract_header_id AS auto_c02 
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml 
                -- WHERE
                    -- upper(trim(tran_code)) = upper(trim('D')) 
                -- GROUP BY
                    -- instance_id ,
                    -- contract_header_id 
            -- ) AS autoAlias_27 
                -- ON (
                    -- CONCAT(CAST(as_of_date_time AS CHAR(26)),
                -- batch_nbr) = autoAlias_27.AUTO_C00 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_27.AUTO_C01)) 
                -- AND upper(trim(contract_header_id)) = upper(trim(autoAlias_27.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) = upper(trim('D'));

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_header_02.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2;


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 FOR ACCESSLOCK TABLE  ${EEDDWWDDBB1}.contract_header_ld FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2(    contract_header_id, instance_id,    alt_as_of_date_time,    alt_batch_nbr,  approved_date_time, archived_flag,  as_of_date_time,    author_ncr_unit_id, author_oper_unit_country_code,  author_operating_unit_id,   auto_renew_day_cnt, award_contract_header_id,   batch_nbr,  cancelled_date_time,    character_type_code,    contract_est_amt_ent,   contract_intent_code,   contract_nbr,   contract_nbr_modifier,  contract_subtype_code,  created_by_name,    creation_date_time, curr_code,  current_record_ind, cust_po_nbr,    cust_po_nbr_rqrd_flag,  deleted_flag,   end_date_time,  est_renew_amt_ent,  gsa_code,   inventory_org_id,   issue_receive_code, issued_date_time,   keep_on_mail_list_flag, last_update_date_time,  last_update_login_name, last_updated_by_name,   migration_contract_nbr, orig_sys_id,    orig_sys_ref_id,    orig_sys_source_code,   prepay_rqrd_flag,   price_list_header_id,   pricing_date_time,  projtd_close_date_time, proposed_date_time, qa_check_list_id,   renew_contract_header_id,   renew_curr_code,    renew_date_time,    renew_to_contract_header_id,    resolved_until_date_time,   resp_contract_header_id,    respond_date_time,  response_rqrd_copy_nbr, set_aside_pct,  sign_by_date_time,  signed_date_time,   solution_portfolio_id,  start_date_time,    status_code,        subclass_code,  template_flag,  template_used_name, termination_date_time,  termination_reason_code,    termination_code,   total_list_price_amt_ent,   tran_code,  upg_orig_sys_ref_name,  upg_orig_sys_ref_id,    user_estimated_amt_ent, agreement_record_id,        final_price_modifier_text,  invoice_zero_amt_flag,  bill_to_site_use_id,    curr_cnvrsn_rate,   curr_cnvrsn_rate_date,  curr_cnvrsn_type_code,  document_id,    invoice_rule_id,    invoice_term_id,    renewal_end_date,   renewal_type_code,  ship_to_site_use_id,    new_for_new_duration  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>, renewal_approval_type,        suspend_credits_flag)SELECT   contract_header_id, instance_id,    alt_as_of_date_time,    alt_batch_nbr,  approved_date_time, archived_flag,  as_of_date_time,    author_ncr_unit_id, author_oper_unit_country_code,  author_operating_unit_id,   auto_renew_day_cnt, award_contract_header_id,   batch_nbr,  cancelled_date_time,    character_type_code,    contract_est_amt_ent,   contract_intent_code,   contract_nbr,   contract_nbr_modifier,  contract_subtype_code,  created_by_name,    creation_date_time, curr_code,  'Y',    cust_po_nbr,    cust_po_nbr_rqrd_flag,  deleted_flag,   end_date_time,  est_renew_amt_ent,  gsa_code,   inventory_org_id,   issue_receive_code, issued_date_time,   keep_on_mail_list_flag, last_update_date_time,  last_update_login_name, last_updated_by_name,   migration_contract_nbr, orig_sys_id,    orig_sys_ref_id,    orig_sys_source_code,   prepay_rqrd_flag,   price_list_header_id,   pricing_date_time,  projtd_close_date_time, proposed_date_time, qa_check_list_id,   renew_contract_header_id,   renew_curr_code,    renew_date_time,    renew_to_contract_header_id,    resolved_until_date_time,   resp_contract_header_id,    respond_date_time,  response_rqrd_copy_nbr, set_aside_pct,  sign_by_date_time,  signed_date_time,   solution_portfolio_id,  start_date_time,    status_code,        subclass_code,  template_flag,  template_used_name, termination_date_time,  termination_reason_code,    termination_code,   total_list_price_amt_ent,   tran_code,  upg_orig_sys_ref_name,  upg_orig_sys_ref_id,    user_estimated_amt_ent, agreement_record_id,        final_price_modifier_text,  invoice_zero_amt_flag,  bill_to_site_use_id,    curr_cnvrsn_rate,   curr_cnvrsn_rate_date,  curr_cnvrsn_type_code,  document_id,    invoice_rule_id,    invoice_term_id,    renewal_end_date,   renewal_type_code,  ship_to_site_use_id,    new_for_new_duration,   renewal_approval_type,        suspend_credits_flagFROM ${EEDDWWDDBB1}.contract_header_ldWHERE (instance_id, contract_header_id)IN (SELECT     instance_id,    contract_header_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4    )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2           SELECT distinct
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
            ${EEDDWWDDBB1}.contract_header_ld 
        INNER JOIN
            (
                SELECT
                    DISTINCT instance_id AS auto_c00,
                    contract_header_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 
            ) AS autoAlias_29 
                ON (
                    upper(trim(instance_id)) = upper(trim(autoAlias_29.AUTO_C00)) 
                    AND upper(trim(contract_header_id)) = upper(trim(autoAlias_29.AUTO_C01)) 
                );


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t3 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t3;


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 FOR ACCESSLOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t3(   contract_header_id, instance_id,    approved_date_time, archived_flag,  author_ncr_unit_id, author_oper_unit_country_code,  author_operating_unit_id,   auto_renew_day_cnt, award_contract_header_id,   cancelled_date_time,    character_type_code,    contract_est_amt_ent,   contract_intent_code,   contract_nbr,   contract_nbr_modifier,  contract_subtype_code,  created_by_name,    creation_date_time, curr_code,  cust_po_nbr,    cust_po_nbr_rqrd_flag,  deleted_flag,   end_date_time,  est_renew_amt_ent,  gsa_code,   inventory_org_id,   issue_receive_code, issued_date_time,   keep_on_mail_list_flag, last_update_date_time,  last_update_login_name, last_updated_by_name,   migration_contract_nbr, orig_sys_id,    orig_sys_ref_id,    orig_sys_source_code,   prepay_rqrd_flag,   price_list_header_id,   pricing_date_time,  projtd_close_date_time, proposed_date_time, qa_check_list_id,   renew_contract_header_id,   renew_curr_code,    renew_date_time,    renew_to_contract_header_id,    resolved_until_date_time,   resp_contract_header_id,    respond_date_time,  response_rqrd_copy_nbr, set_aside_pct,  sign_by_date_time,  signed_date_time,   solution_portfolio_id,  start_date_time,    status_code,        subclass_code,  template_flag,  template_used_name, termination_date_time,  termination_reason_code,    termination_code,   total_list_price_amt_ent,   tran_code,  upg_orig_sys_ref_name,  upg_orig_sys_ref_id,    user_estimated_amt_ent, agreement_record_id,        final_price_modifier_text,  invoice_zero_amt_flag,  bill_to_site_use_id,    curr_cnvrsn_rate,   curr_cnvrsn_rate_date,  curr_cnvrsn_type_code,  document_id,    invoice_rule_id,    invoice_term_id,    renewal_end_date,   renewal_type_code,  ship_to_site_use_id,    new_for_new_duration,        renewal_approval_type,        suspend_credits_flag)SELECT  contract_header_id, instance_id,    approved_date_time, archived_flag,  author_ncr_unit_id, author_oper_unit_country_code,  author_operating_unit_id,   auto_renew_day_cnt, award_contract_header_id,   cancelled_date_time,    character_type_code,    contract_est_amt_ent,   contract_intent_code,   contract_nbr,   contract_nbr_modifier,  contract_subtype_code,  created_by_name,    creation_date_time, curr_code,  cust_po_nbr,    cust_po_nbr_rqrd_flag,  deleted_flag,   end_date_time,  est_renew_amt_ent,  gsa_code,   inventory_org_id,   issue_receive_code, issued_date_time,   keep_on_mail_list_flag, last_update_date_time,  last_update_login_name, last_updated_by_name,   migration_contract_nbr, orig_sys_id,    orig_sys_ref_id,    orig_sys_source_code,   prepay_rqrd_flag,   price_list_header_id,   pricing_date_time,  projtd_close_date_time, proposed_date_time, qa_check_list_id,   renew_contract_header_id,   renew_curr_code,    renew_date_time,    renew_to_contract_header_id,    resolved_until_date_time,   resp_contract_header_id,    respond_date_time,  response_rqrd_copy_nbr, set_aside_pct,  sign_by_date_time,  signed_date_time,   solution_portfolio_id,  start_date_time,    status_code,        subclass_code,  template_flag,  template_used_name, termination_date_time,  termination_reason_code,    termination_code,   total_list_price_amt_ent,   tran_code,  upg_orig_sys_ref_name,  upg_orig_sys_ref_id,    user_estimated_amt_ent, agreement_record_id,        final_price_modifier_text,  invoice_zero_amt_flag,  bill_to_site_use_id,    curr_cnvrsn_rate,   curr_cnvrsn_rate_date,  curr_cnvrsn_type_code,  document_id,    invoice_rule_id,    invoice_term_id,    renewal_end_date,   renewal_type_code,  ship_to_site_use_id,    new_for_new_duration,        renewal_approval_type,        suspend_credits_flagFROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2WHERE (instance_id, contract_header_id)IN (SELECT     instance_id,    contract_header_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4    )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t3           SELECT distinct
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 
        INNER JOIN
            (
                SELECT
                    DISTINCT instance_id AS auto_c00,
                    contract_header_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 
            ) AS autoAlias_31 
                ON (
                    upper(trim(instance_id)) = upper(trim(autoAlias_31.AUTO_C00)) 
                    AND upper(trim(contract_header_id)) = upper(trim(autoAlias_31.AUTO_C01)) 
                );

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:38:03 
--Script Name: d8_contract_header_03.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query: 6e481eca6ee48dc438a03b64515d1321
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 FOR ACCESS LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 FOR ACCESS  INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t3 (   contract_header_id,     instance_id,    approved_date_time,     archived_flag,  author_ncr_unit_id,     author_oper_unit_country_code,  author_operating_unit_id,   auto_renew_day_cnt,     award_contract_header_id,   cancelled_date_time,    character_type_code,    contract_est_amt_ent,   contract_intent_code,   contract_nbr,   contract_nbr_modifier,  contract_subtype_code,  created_by_name,    creation_date_time,     cust_po_nbr,    curr_code,  cust_po_nbr_rqrd_flag,  deleted_flag,   end_date_time,  est_renew_amt_ent,  gsa_code,   inventory_org_id,   issue_receive_code,     issued_date_time,   keep_on_mail_list_flag,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   migration_contract_nbr,     orig_sys_id,    orig_sys_ref_id,    orig_sys_source_code,   prepay_rqrd_flag,   price_list_header_id,   pricing_date_time,  projtd_close_date_time,     proposed_date_time,     qa_check_list_id,   renew_contract_header_id,   renew_curr_code,    renew_date_time,    renew_to_contract_header_id,    resolved_until_date_time,   resp_contract_header_id,    respond_date_time,  response_rqrd_copy_nbr,     set_aside_pct,  sign_by_date_time,  signed_date_time,   solution_portfolio_id,  start_date_time,    status_code,        subclass_code,      template_flag,  template_used_name,     termination_date_time,  termination_reason_code,    termination_code,   total_list_price_amt_ent,   tran_code,  upg_orig_sys_ref_name,  upg_orig_sys_ref_id,    user_estimated_amt_ent,     agreement_record_id,        final_price_modifier_text,  invoice_zero_amt_flag,  bill_to_site_use_id,    curr_cnvrsn_rate,   curr_cnvrsn_rate_date,  curr_cnvrsn_type_code,  document_id,    invoice_rule_id,    invoice_term_id,    renewal_end_date,   renewal_type_code,  ship_to_site_use_id,    new_for_new_duration,   renewal_approval_type,         suspend_credits_flag ) SELECT    contract_header_id,     instance_id,    approved_date_time,     archived_flag,  author_ncr_unit_id,     author_oper_unit_country_code,  author_operating_unit_id,   auto_renew_day_cnt,     award_contract_header_id,   cancelled_date_time,    character_type_code,    contract_est_amt_ent,   contract_intent_code,   contract_nbr,   contract_nbr_modifier,  contract_subtype_code,  created_by_name,    creation_date_time,     curr_code,  cust_po_nbr,    cust_po_nbr_rqrd_flag,  deleted_flag,   end_date_time,  est_renew_amt_ent,  gsa_code,   inventory_org_id,   issue_receive_code,     issued_date_time,   keep_on_mail_list_flag,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   migration_contract_nbr,     orig_sys_id,    orig_sys_ref_id,    orig_sys_source_code,   prepay_rqrd_flag,   price_list_header_id,   pricing_date_time,  projtd_close_date_time,     proposed_date_time,     qa_check_list_id,   renew_contract_header_id,   renew_curr_code,    renew_date_time,    renew_to_contract_header_id,    resolved_until_date_time,   resp_contract_header_id,    respond_date_time,  response_rqrd_copy_nbr,     set_aside_pct,  sign_by_date_time,  signed_date_time,   solution_portfolio_id,  start_date_time,    status_code,        subclass_code,      template_flag,  template_used_name,     termination_date_time,  termination_reason_code,    termination_code,   total_list_price_amt_ent,   tran_code,  upg_orig_sys_ref_name,  upg_orig_sys_ref_id,    user_estimated_amt_ent,     agreement_record_id,        final_price_modifier_text,  invoice_zero_amt_flag,  bill_to_site_use_id,    curr_cnvrsn_rate,   curr_cnvrsn_rate_date,  curr_cnvrsn_type_code,  document_id,    invoice_rule_id,    invoice_term_id,    renewal_end_date,   renewal_type_code,  ship_to_site_use_id,    new_for_new_duration,   renewal_approval_type,         suspend_credits_flag FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 WHERE (instance_id, contract_header_id) IN (SELECT      instance_id,     contract_header_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2     )  

--Translated Query: --pankaj not required as Query redundant from next file

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t3           SELECT DISTINCT
            -- contract_header_id,
            -- instance_id,
            -- approved_date_time,
            -- archived_flag,
            -- author_ncr_unit_id,
            -- author_oper_unit_country_code,
            -- author_operating_unit_id,
            -- auto_renew_day_cnt,
            -- award_contract_header_id,
            -- cancelled_date_time,
            -- character_type_code,
            -- contract_est_amt_ent,
            -- contract_intent_code,
            -- contract_nbr,
            -- contract_nbr_modifier,
            -- contract_subtype_code,
            -- created_by_name,
            -- creation_date_time,
            -- cust_po_nbr,
            -- curr_code,
            -- cust_po_nbr_rqrd_flag,
            -- deleted_flag,
            -- end_date_time,
            -- est_renew_amt_ent,
            -- gsa_code,
            -- inventory_org_id,
            -- issue_receive_code,
            -- issued_date_time,
            -- keep_on_mail_list_flag,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- migration_contract_nbr,
            -- orig_sys_id,
            -- orig_sys_ref_id,
            -- orig_sys_source_code,
            -- prepay_rqrd_flag,
            -- price_list_header_id,
            -- pricing_date_time,
            -- projtd_close_date_time,
            -- proposed_date_time,
            -- qa_check_list_id,
            -- renew_contract_header_id,
            -- renew_curr_code,
            -- renew_date_time,
            -- renew_to_contract_header_id,
            -- resolved_until_date_time,
            -- resp_contract_header_id,
            -- respond_date_time,
            -- response_rqrd_copy_nbr,
            -- set_aside_pct,
            -- sign_by_date_time,
            -- signed_date_time,
            -- solution_portfolio_id,
            -- start_date_time,
            -- status_code,
            -- subclass_code,
            -- template_flag,
            -- template_used_name,
            -- termination_code,
            -- termination_date_time,
            -- termination_reason_code,
            -- total_list_price_amt_ent,
            -- tran_code,
            -- upg_orig_sys_ref_id,
            -- upg_orig_sys_ref_name,
            -- user_estimated_amt_ent,
            -- agreement_record_id,
            -- final_price_modifier_text,
            -- invoice_zero_amt_flag,
            -- bill_to_site_use_id,
            -- curr_cnvrsn_rate,
            -- curr_cnvrsn_rate_date,
            -- curr_cnvrsn_type_code,
            -- document_id,
            -- invoice_rule_id,
            -- invoice_term_id,
            -- renewal_end_date,
            -- renewal_type_code,
            -- ship_to_site_use_id,
            -- new_for_new_duration,
            -- renewal_approval_type,
            -- suspend_credits_flag,
            -- hold_reason_description,
            -- agreement_start_date,
            -- agreement_end_date  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- DISTINCT instance_id AS auto_c00,
                    -- contract_header_id AS auto_c01  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 
            -- ) AS autoAlias_347 
                -- ON (
                    -- upper(trim(instance_id)) = upper(trim(autoAlias_347.AUTO_C00)) 
                    -- AND upper(trim(contract_header_id)) = upper(trim(autoAlias_347.AUTO_C01)) 
                -- );
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_header_04.upd.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --UPDATE d8t_contract_header_t2 SET current_record_ind = 'D' WHERE (instance_id, contract_header_id) IN    (SELECT instance_id, contract_header_id    FROM d8t_contract_header_t3    GROUP BY instance_id, contract_header_id    HAVING COUNT(*) = 1    )

--Translated Query: SUCCESS

    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 SELECT distinct
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 
        LEFT OUTER JOIN
            (
                SELECT
                    instance_id AS auto_c00,
                    contract_header_id AS auto_c01,
                    COUNT(*) AS RCRD_UPD  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t3 
                GROUP BY
                    1,
                    2
            ) AS autoAlias_7 
                ON (
                    upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                    AND upper(trim(contract_header_id)) = upper(trim(autoAlias_7.AUTO_C01)) 
                );

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_header_05.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --COLLECT STATISTICS ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 COLUMN (CURRENT_RECORD_IND)

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 COMPUTE STATISTICS  FOR COLUMNS CURRENT_RECORD_IND;


--Original Query:
  --DELETE FROM ${EEDDWWDDBB1}.contract_header_ld WHERE (instance_id, contract_header_id)IN (SELECT instance_id, contract_header_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4    WHERE (instance_id, contract_header_id) NOT IN    (SELECT instance_id, contract_header_id FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 WHERE current_record_ind = 'D'  )    )

--Translated Query: SUCCESS
--INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_header_ld SELECT CD.* FROM ${EEDDWWDDBB1}.contract_header_ld AS CD LEFT JOIN (SELECT CH.instance_id, CH.contract_header_id FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 AS CH LEFT JOIN ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 CH2 ON CH.instance_id = CH2.instance_id AND CH.contract_header_id = CH2.contract_header_id WHERE CH2.instance_id IS NULL AND CH2.contract_header_id IS NULL ) AS T ON CD.instance_id = T.instance_id AND CD.contract_header_id = T.contract_header_id WHERE T.instance_id IS NULL AND T.contract_header_id IS NULL
--Chngd NATASHA(CORRECTED the transformed query)

    INSERT OVERWRITE
         TABLE ${EEDDWWDDBB1}.contract_header_ld 
         SELECT distinct Q1.* FROM  ${EEDDWWDDBB1}.contract_header_ld AS Q1
         LEFT OUTER JOIN 
                (
                SELECT contract_header_id, instance_id
                FROM
                 ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 AS Q2  
                LEFT OUTER JOIN 
                    (   SELECT contract_header_id AS autoc000 , instance_id as autoc001
                    FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 
                        WHERE current_record_ind = 'D' ) AS Q3 
                ON Q2.contract_header_id = Q3.autoc000
                AND Q2.instance_id = Q3.autoc001
                    WHERE Q3.autoc000 IS NULL 
                    AND Q3.autoc001 IS NULL
                ) AS Q23
         ON Q1.contract_header_id = Q23.contract_header_id
         AND Q1.instance_id = Q23.instance_id
            WHERE Q23.contract_header_id IS NULL 
            AND Q23.instance_id IS NULL;

--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 FOR ACCESSINSERT INTO ${EEDDWWDDBB1}.contract_header_ld( contract_header_id, instance_id,    alt_as_of_date_time,    alt_batch_nbr,  approved_date_time, archived_flag,  as_of_date_time,    author_ncr_unit_id, author_oper_unit_country_code,  author_operating_unit_id,   auto_renew_day_cnt, award_contract_header_id,   batch_nbr,  cancelled_date_time,    character_type_code,    contract_est_amt_ent,   contract_intent_code,   contract_nbr,   contract_nbr_modifier,  contract_subtype_code,  created_by_name,    creation_date_time, curr_code,  cust_po_nbr,    cust_po_nbr_rqrd_flag,  deleted_flag,   end_date_time,  est_renew_amt_ent,  gsa_code,   inventory_org_id,   issue_receive_code, issued_date_time,   keep_on_mail_list_flag, last_update_date_time,  last_update_login_name, last_updated_by_name,   migration_contract_nbr, orig_sys_id,    orig_sys_ref_id,    orig_sys_source_code,   prepay_rqrd_flag,   price_list_header_id,   pricing_date_time,  projtd_close_date_time, proposed_date_time, qa_check_list_id,   renew_contract_header_id,   renew_curr_code,    renew_date_time,    renew_to_contract_header_id,    resolved_until_date_time,   resp_contract_header_id,    respond_date_time,  response_rqrd_copy_nbr, set_aside_pct,  sign_by_date_time,  signed_date_time,   solution_portfolio_id,  start_date_time,    status_code,        subclass_code,  template_flag,  template_used_name, termination_date_time,  termination_reason_code,    termination_code,   total_list_price_amt_ent,   tran_code,  upg_orig_sys_ref_name,  upg_orig_sys_ref_id,    user_estimated_amt_ent, agreement_record_id,        final_price_modifier_text,  invoice_zero_amt_flag,  bill_to_site_use_id,    curr_cnvrsn_rate,   curr_cnvrsn_rate_date,  curr_cnvrsn_type_code,  document_id,    invoice_rule_id,    invoice_term_id,    renewal_end_date,   renewal_type_code,  ship_to_site_use_id,    new_for_new_duration,   renewal_approval_type,        suspend_credits_flag)SELECT   contract_header_id, instance_id,    alt_as_of_date_time,    alt_batch_nbr,  approved_date_time, archived_flag,  as_of_date_time,    author_ncr_unit_id, author_oper_unit_country_code,  author_operating_unit_id,   auto_renew_day_cnt, award_contract_header_id,   batch_nbr,  cancelled_date_time,    character_type_code,    contract_est_amt_ent,   contract_intent_code,   contract_nbr,   contract_nbr_modifier,  contract_subtype_code,  created_by_name,    creation_date_time, curr_code,  cust_po_nbr,    cust_po_nbr_rqrd_flag,  deleted_flag,   end_date_time,  est_renew_amt_ent,  gsa_code,   inventory_org_id,   issue_receive_code, issued_date_time,   keep_on_mail_list_flag, last_update_date_time,  last_update_login_name, last_updated_by_name,   migration_contract_nbr, orig_sys_id,    orig_sys_ref_id,    orig_sys_source_code,   prepay_rqrd_flag,   price_list_header_id,   pricing_date_time,  projtd_close_date_time, proposed_date_time, qa_check_list_id,   renew_contract_header_id,   renew_curr_code,    renew_date_time,    renew_to_contract_header_id,    resolved_until_date_time,   resp_contract_header_id,    respond_date_time,  response_rqrd_copy_nbr, set_aside_pct,  sign_by_date_time,  signed_date_time,   solution_portfolio_id,  start_date_time,    status_code,        subclass_code,  template_flag,  template_used_name, termination_date_time,  termination_reason_code,    termination_code,   total_list_price_amt_ent,   tran_code,  upg_orig_sys_ref_name,  upg_orig_sys_ref_id,    user_estimated_amt_ent,        agreement_record_id,         final_price_modifier_text,  invoice_zero_amt_flag,  bill_to_site_use_id,    curr_cnvrsn_rate,   curr_cnvrsn_rate_date,  curr_cnvrsn_type_code,  document_id,    invoice_rule_id,    invoice_term_id,    renewal_end_date,   renewal_type_code,  ship_to_site_use_id,    new_for_new_duration,   renewal_approval_type,        suspend_credits_flagFROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4WHERE ( instance_id, contract_header_id )NOT IN (SELECT  instance_id, contract_header_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2    WHERE current_record_ind = 'D'     )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_header_ld           SELECT distinct
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4 
        LEFT OUTER JOIN
            (
                SELECT
                    DISTINCT instance_id AS auto_c00,
                    contract_header_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2     
                WHERE
                    upper(trim(current_record_ind)) = upper(trim('D')) 
            ) AS autoAlias_37 
                ON (
                    upper(trim(instance_id)) = upper(trim(autoAlias_37.AUTO_C00)) 
                    AND upper(trim(contract_header_id)) = upper(trim(autoAlias_37.AUTO_C01)) 
                ) 
        WHERE
            autoAlias_37.AUTO_C00 IS  NULL  
            AND autoAlias_37.AUTO_C01 IS  NULL;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:38:07 
--Script Name: d8_contract_header_06.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query: 43dc72f40f95ab6facba7534c9695bdd
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2;

--Original Query: 9cf4276fe0101399b953d95053e144ef
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 FOR ACCESS  INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 (         contract_header_id,         instance_id,         alt_as_of_date_time,         alt_batch_nbr,         approved_date_time,         archived_flag,         as_of_date_time,         author_ncr_unit_id,         author_oper_unit_country_code,         author_operating_unit_id,         auto_renew_day_cnt,         award_contract_header_id,         batch_nbr,         cancelled_date_time,         character_type_code,         contract_est_amt_ent,         contract_intent_code,         contract_nbr,         contract_nbr_modifier,         contract_subtype_code,         created_by_name,         creation_date_time,         curr_code,         cust_po_nbr,         cust_po_nbr_rqrd_flag,         deleted_flag,         end_date_time,         est_renew_amt_ent,         gsa_code,         inventory_org_id,         issue_receive_code,         issued_date_time,         keep_on_mail_list_flag,         last_update_date_time,         last_update_login_name,         last_updated_by_name,         migration_contract_nbr,         orig_sys_id,         orig_sys_ref_id,         orig_sys_source_code,         prepay_rqrd_flag,         price_list_header_id,         pricing_date_time,         projtd_close_date_time,         proposed_date_time,         qa_check_list_id,         renew_contract_header_id,         renew_curr_code,         renew_date_time,         renew_to_contract_header_id,         resolved_until_date_time,         resp_contract_header_id,         respond_date_time,         response_rqrd_copy_nbr,         set_aside_pct,         sign_by_date_time,         signed_date_time,         solution_portfolio_id,         start_date_time,         status_code,         subclass_code,         template_flag,         template_used_name,         termination_date_time,         termination_reason_code,         termination_code,         total_list_price_amt_ent,         tran_code,         upg_orig_sys_ref_name,         upg_orig_sys_ref_id,         user_estimated_amt_ent,         agreement_record_id,         final_price_modifier_text,         invoice_zero_amt_flag,         bill_to_site_use_id,         curr_cnvrsn_rate,         curr_cnvrsn_rate_date,         curr_cnvrsn_type_code,         document_id,         invoice_rule_id,         invoice_term_id,         renewal_end_date,         renewal_type_code,         ship_to_site_use_id,  new_for_new_duration,   renewal_approval_type,         suspend_credits_flag ) SELECT         EDW1.contract_header_id,         EDW1.instance_id,         EDW2.as_of_date_time,         EDW2.batch_nbr,         EDW1.approved_date_time,         EDW1.archived_flag,         EDW1.as_of_date_time,         EDW1.author_ncr_unit_id,         EDW1.author_oper_unit_country_code,         EDW1.author_operating_unit_id,         EDW1.auto_renew_day_cnt,         EDW1.award_contract_header_id,         EDW1.batch_nbr,         EDW1.cancelled_date_time,         EDW1.character_type_code,         EDW1.contract_est_amt_ent,         EDW1.contract_intent_code,         EDW1.contract_nbr,         EDW1.contract_nbr_modifier,         EDW1.contract_subtype_code,         EDW1.created_by_name,         EDW1.creation_date_time,         EDW1.curr_code,         EDW1.cust_po_nbr,         EDW1.cust_po_nbr_rqrd_flag,         EDW1.deleted_flag,         EDW1.end_date_time,         EDW1.est_renew_amt_ent,         EDW1.gsa_code,         EDW1.inventory_org_id,         EDW1.issue_receive_code,         EDW1.issued_date_time,         EDW1.keep_on_mail_list_flag,         EDW1.last_update_date_time,         EDW1.last_update_login_name,         EDW1.last_updated_by_name,         EDW1.migration_contract_nbr,         EDW1.orig_sys_id,         EDW1.orig_sys_ref_id,         EDW1.orig_sys_source_code,         EDW1.prepay_rqrd_flag,         EDW1.price_list_header_id,         EDW1.pricing_date_time,         EDW1.projtd_close_date_time,         EDW1.proposed_date_time,         EDW1.qa_check_list_id,         EDW1.renew_contract_header_id,         EDW1.renew_curr_code,         EDW1.renew_date_time,         EDW1.renew_to_contract_header_id,         EDW1.resolved_until_date_time,         EDW1.resp_contract_header_id,         EDW1.respond_date_time,         EDW1.response_rqrd_copy_nbr,         EDW1.set_aside_pct,         EDW1.sign_by_date_time,         EDW1.signed_date_time,         EDW1.solution_portfolio_id,         EDW1.start_date_time,         EDW1.status_code,         EDW1.subclass_code,         EDW1.template_flag,         EDW1.template_used_name,         EDW1.termination_date_time,         EDW1.termination_reason_code,         EDW1.termination_code,         EDW1.total_list_price_amt_ent,         EDW2.tran_code,         EDW1.upg_orig_sys_ref_name,         EDW1.upg_orig_sys_ref_id,         EDW1.user_estimated_amt_ent,         EDW1.agreement_record_id,         EDW1.final_price_modifier_text,         EDW1.invoice_zero_amt_flag,         EDW1.bill_to_site_use_id,         EDW1.curr_cnvrsn_rate,         EDW1.curr_cnvrsn_rate_date,         EDW1.curr_cnvrsn_type_code,         EDW1.document_id,         EDW1.invoice_rule_id,         EDW1.invoice_term_id,         EDW1.renewal_end_date,         EDW1.renewal_type_code,         EDW1.ship_to_site_use_id,     EDW1.new_for_new_duration,  EDW1.renewal_approval_type,         EDW1.suspend_credits_flag FROM ${EEDDWWDDBB1}.contract_header_ld EDW1,      ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t1 EDW2 WHERE  EDW1.instance_id = EDW2.instance_id AND    EDW1.contract_header_id= EDW2.contract_header_id AND    CAST(EDW1.as_of_date_time AS VARCHAR(26)) || EDW1.batch_nbr <         CAST(EDW2.as_of_date_time AS VARCHAR(26)) || EDW2.batch_nbr  

--Translated Query: --pankaj merged with T1 code 

INSERT 
INTO
TABLE
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2           
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
SELECT * FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml 
INNER JOIN
(
SELECT
MAX(CONCAT(CAST(as_of_date_time AS CHAR(26)),
batch_nbr)) AS auto_c00 ,
instance_id AS auto_c01 ,
contract_header_id AS auto_c02 
FROM
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml 
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
INNER JOIN ${EEDDWWDDBB1}.contract_header_ld    AS EDW1   
ON EDW1.instance_id = EDW2.instance_id  
AND EDW1.contract_header_id = EDW2.contract_header_id   
WHERE  CONCAT (CAST (EDW1.as_of_date_time AS VARCHAR(26)) , EDW1.batch_nbr) <  CONCAT (CAST (EDW2.as_of_date_time AS VARCHAR(26)) , EDW2.batch_nbr);

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_header_07.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${EEDDWWDDBB1}.contract_header_ld WHERE (instance_id, contract_header_id)IN (SELECT instance_id, contract_header_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2    )

--Translated Query: SUCCESS
--      DROP TABLE IF EXISTS  ${EEDDWWDDBB1}.contract_header_ldTemp
--CREATE TABLE ${EEDDWWDDBB1}.contract_header_ldTemp LIKE ${EEDDWWDDBB1}.contract_header_ld
--INSERT INTO TABLE ${EEDDWWDDBB1}.contract_header_ldTemp SELECT Q1.* FROM (SELECT * FROM ${EEDDWWDDBB1}.contract_header_ld ) AS  Q1  LEFT OUTER JOIN (SELECT  * FROM  ${EEDDWWDDBB1}.contract_header_ld      ) AS Q2 ON COALESCE( Q1.contract_header_id ,  '1' ) = COALESCE( Q2.contract_header_id ,  '1' ) AND COALESCE( Q1.instance_id ,  '1' ) = COALESCE( Q2.instance_id ,  '1' ) AND COALESCE( Q1.alt_as_of_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.alt_as_of_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.alt_batch_nbr , 1) = COALESCE( Q2.alt_batch_nbr , 1) AND COALESCE( Q1.approved_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.approved_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.archived_flag ,  '1' ) = COALESCE( Q2.archived_flag ,  '1' ) AND COALESCE( Q1.as_of_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.as_of_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.author_ncr_unit_id ,  '1' ) = COALESCE( Q2.author_ncr_unit_id ,  '1' ) AND COALESCE( Q1.author_oper_unit_country_code ,  '1' ) = COALESCE( Q2.author_oper_unit_country_code ,  '1' ) AND COALESCE( Q1.author_operating_unit_id , 1) = COALESCE( Q2.author_operating_unit_id , 1) AND COALESCE( Q1.auto_renew_day_cnt , 1) = COALESCE( Q2.auto_renew_day_cnt , 1) AND COALESCE( Q1.award_contract_header_id ,  '1' ) = COALESCE( Q2.award_contract_header_id ,  '1' ) AND COALESCE( Q1.batch_nbr , 1) = COALESCE( Q2.batch_nbr , 1) AND COALESCE( Q1.cancelled_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.cancelled_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.character_type_code ,  '1' ) = COALESCE( Q2.character_type_code ,  '1' ) AND COALESCE( Q1.contract_est_amt_ent , 1) = COALESCE( Q2.contract_est_amt_ent , 1) AND COALESCE( Q1.contract_intent_code ,  '1' ) = COALESCE( Q2.contract_intent_code ,  '1' ) AND COALESCE( Q1.contract_nbr ,  '1' ) = COALESCE( Q2.contract_nbr ,  '1' ) AND COALESCE( Q1.contract_nbr_modifier ,  '1' ) = COALESCE( Q2.contract_nbr_modifier ,  '1' ) AND COALESCE( Q1.created_by_name ,  '1' ) = COALESCE( Q2.created_by_name ,  '1' ) AND COALESCE( Q1.creation_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.curr_code ,  '1' ) = COALESCE( Q2.curr_code ,  '1' ) AND COALESCE( Q1.cust_po_nbr ,  '1' ) = COALESCE( Q2.cust_po_nbr ,  '1' ) AND COALESCE( Q1.cust_po_nbr_rqrd_flag ,  '1' ) = COALESCE( Q2.cust_po_nbr_rqrd_flag ,  '1' ) AND COALESCE( Q1.deleted_flag ,  '1' ) = COALESCE( Q2.deleted_flag ,  '1' ) AND COALESCE( Q1.end_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.end_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.est_renew_amt_ent , 1) = COALESCE( Q2.est_renew_amt_ent , 1) AND COALESCE( Q1.gsa_code ,  '1' ) = COALESCE( Q2.gsa_code ,  '1' ) AND COALESCE( Q1.inventory_org_id , 1) = COALESCE( Q2.inventory_org_id , 1) AND COALESCE( Q1.issue_receive_code ,  '1' ) = COALESCE( Q2.issue_receive_code ,  '1' ) AND COALESCE( Q1.issued_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.issued_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.keep_on_mail_list_flag ,  '1' ) = COALESCE( Q2.keep_on_mail_list_flag ,  '1' ) AND COALESCE( Q1.last_update_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.last_update_login_name ,  '1' ) = COALESCE( Q2.last_update_login_name ,  '1' ) AND COALESCE( Q1.last_updated_by_name ,  '1' ) = COALESCE( Q2.last_updated_by_name ,  '1' ) AND COALESCE( Q1.migration_contract_nbr ,  '1' ) = COALESCE( Q2.migration_contract_nbr ,  '1' ) AND COALESCE( Q1.orig_sys_id ,  '1' ) = COALESCE( Q2.orig_sys_id ,  '1' ) AND COALESCE( Q1.orig_sys_ref_id ,  '1' ) = COALESCE( Q2.orig_sys_ref_id ,  '1' ) AND COALESCE( Q1.orig_sys_source_code ,  '1' ) = COALESCE( Q2.orig_sys_source_code ,  '1' ) AND COALESCE( Q1.prepay_rqrd_flag ,  '1' ) = COALESCE( Q2.prepay_rqrd_flag ,  '1' ) AND COALESCE( Q1.price_list_header_id , 1) = COALESCE( Q2.price_list_header_id , 1) AND COALESCE( Q1.pricing_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.pricing_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.projtd_close_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.projtd_close_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.proposed_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.proposed_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.qa_check_list_id ,  '1' ) = COALESCE( Q2.qa_check_list_id ,  '1' ) AND COALESCE( Q1.renew_contract_header_id ,  '1' ) = COALESCE( Q2.renew_contract_header_id ,  '1' ) AND COALESCE( Q1.renew_curr_code ,  '1' ) = COALESCE( Q2.renew_curr_code ,  '1' ) AND COALESCE( Q1.renew_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.renew_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.renew_to_contract_header_id ,  '1' ) = COALESCE( Q2.renew_to_contract_header_id ,  '1' ) AND COALESCE( Q1.resolved_until_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.resolved_until_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.resp_contract_header_id ,  '1' ) = COALESCE( Q2.resp_contract_header_id ,  '1' ) AND COALESCE( Q1.respond_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.respond_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.response_rqrd_copy_nbr , 1) = COALESCE( Q2.response_rqrd_copy_nbr , 1) AND COALESCE( Q1.set_aside_pct , 1) = COALESCE( Q2.set_aside_pct , 1) AND COALESCE( Q1.sign_by_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.sign_by_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.signed_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.signed_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.solution_portfolio_id ,  '1' ) = COALESCE( Q2.solution_portfolio_id ,  '1' ) AND COALESCE( Q1.start_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.start_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.status_code ,  '1' ) = COALESCE( Q2.status_code ,  '1' ) AND COALESCE( Q1.subclass_code ,  '1' ) = COALESCE( Q2.subclass_code ,  '1' ) AND COALESCE( Q1.template_flag ,  '1' ) = COALESCE( Q2.template_flag ,  '1' ) AND COALESCE( Q1.template_used_name ,  '1' ) = COALESCE( Q2.template_used_name ,  '1' ) AND COALESCE( Q1.termination_code ,  '1' ) = COALESCE( Q2.termination_code ,  '1' ) AND COALESCE( Q1.termination_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.termination_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.termination_reason_code ,  '1' ) = COALESCE( Q2.termination_reason_code ,  '1' ) AND COALESCE( Q1.total_list_price_amt_ent , 1) = COALESCE( Q2.total_list_price_amt_ent , 1) AND COALESCE( Q1.tran_code ,  '1' ) = COALESCE( Q2.tran_code ,  '1' ) AND COALESCE( Q1.upg_orig_sys_ref_id ,  '1' ) = COALESCE( Q2.upg_orig_sys_ref_id ,  '1' ) AND COALESCE( Q1.upg_orig_sys_ref_name ,  '1' ) = COALESCE( Q2.upg_orig_sys_ref_name ,  '1' ) AND COALESCE( Q1.user_estimated_amt_ent , 1) = COALESCE( Q2.user_estimated_amt_ent , 1) AND COALESCE( Q1.agreement_record_id ,  '1' ) = COALESCE( Q2.agreement_record_id ,  '1' ) AND COALESCE( Q1.final_price_modifier_text ,  '1' ) = COALESCE( Q2.final_price_modifier_text ,  '1' ) AND COALESCE( Q1.invoice_zero_amt_flag ,  '1' ) = COALESCE( Q2.invoice_zero_amt_flag ,  '1' ) AND COALESCE( Q1.bill_to_site_use_id , 1) = COALESCE( Q2.bill_to_site_use_id , 1) AND COALESCE( Q1.curr_cnvrsn_rate , 1) = COALESCE( Q2.curr_cnvrsn_rate , 1) AND COALESCE( Q1.curr_cnvrsn_rate_date ,  CURRENT_DATE() ) = COALESCE( Q2.curr_cnvrsn_rate_date ,  CURRENT_DATE() ) AND COALESCE( Q1.curr_cnvrsn_type_code ,  '1' ) = COALESCE( Q2.curr_cnvrsn_type_code ,  '1' ) AND COALESCE( Q1.document_id , 1) = COALESCE( Q2.document_id , 1) AND COALESCE( Q1.invoice_rule_id , 1) = COALESCE( Q2.invoice_rule_id , 1) AND COALESCE( Q1.invoice_term_id , 1) = COALESCE( Q2.invoice_term_id , 1) AND COALESCE( Q1.renewal_end_date ,  CURRENT_DATE() ) = COALESCE( Q2.renewal_end_date ,  CURRENT_DATE() ) AND COALESCE( Q1.renewal_type_code ,  '1' ) = COALESCE( Q2.renewal_type_code ,  '1' ) AND COALESCE( Q1.ship_to_site_use_id , 1) = COALESCE( Q2.ship_to_site_use_id , 1) AND COALESCE( Q1.contract_subtype_code ,  '1' ) = COALESCE( Q2.contract_subtype_code ,  '1' ) AND COALESCE( Q1.new_for_new_duration ,  '1' ) = COALESCE( Q2.new_for_new_duration ,  '1' ) AND COALESCE( Q1.renewal_approval_type ,  '1' ) = COALESCE( Q2.renewal_approval_type ,  '1' ) AND COALESCE( Q1.suspend_credits_flag ,  '1' ) = COALESCE( Q2.suspend_credits_flag ,  '1' ) AND COALESCE( Q1.hold_reason_description ,  '1' ) = COALESCE( Q2.hold_reason_description ,  '1' ) AND COALESCE( Q1.agreement_start_date ,  CURRENT_DATE() ) = COALESCE( Q2.agreement_start_date ,  CURRENT_DATE() ) AND COALESCE( Q1.agreement_end_date ,  CURRENT_DATE() ) = COALESCE( Q2.agreement_end_date ,  CURRENT_DATE() ) WHERE Q2.contract_header_id IS NULL AND Q2.instance_id IS NULL AND Q2.alt_as_of_date_time IS NULL AND Q2.alt_batch_nbr IS NULL AND Q2.approved_date_time IS NULL AND Q2.archived_flag IS NULL AND Q2.as_of_date_time IS NULL AND Q2.author_ncr_unit_id IS NULL AND Q2.author_oper_unit_country_code IS NULL AND Q2.author_operating_unit_id IS NULL AND Q2.auto_renew_day_cnt IS NULL AND Q2.award_contract_header_id IS NULL AND Q2.batch_nbr IS NULL AND Q2.cancelled_date_time IS NULL AND Q2.character_type_code IS NULL AND Q2.contract_est_amt_ent IS NULL AND Q2.contract_intent_code IS NULL AND Q2.contract_nbr IS NULL AND Q2.contract_nbr_modifier IS NULL AND Q2.created_by_name IS NULL AND Q2.creation_date_time IS NULL AND Q2.curr_code IS NULL AND Q2.cust_po_nbr IS NULL AND Q2.cust_po_nbr_rqrd_flag IS NULL AND Q2.deleted_flag IS NULL AND Q2.end_date_time IS NULL AND Q2.est_renew_amt_ent IS NULL AND Q2.gsa_code IS NULL AND Q2.inventory_org_id IS NULL AND Q2.issue_receive_code IS NULL AND Q2.issued_date_time IS NULL AND Q2.keep_on_mail_list_flag IS NULL AND Q2.last_update_date_time IS NULL AND Q2.last_update_login_name IS NULL AND Q2.last_updated_by_name IS NULL AND Q2.migration_contract_nbr IS NULL AND Q2.orig_sys_id IS NULL AND Q2.orig_sys_ref_id IS NULL AND Q2.orig_sys_source_code IS NULL AND Q2.prepay_rqrd_flag IS NULL AND Q2.price_list_header_id IS NULL AND Q2.pricing_date_time IS NULL AND Q2.projtd_close_date_time IS NULL AND Q2.proposed_date_time IS NULL AND Q2.qa_check_list_id IS NULL AND Q2.renew_contract_header_id IS NULL AND Q2.renew_curr_code IS NULL AND Q2.renew_date_time IS NULL AND Q2.renew_to_contract_header_id IS NULL AND Q2.resolved_until_date_time IS NULL AND Q2.resp_contract_header_id IS NULL AND Q2.respond_date_time IS NULL AND Q2.response_rqrd_copy_nbr IS NULL AND Q2.set_aside_pct IS NULL AND Q2.sign_by_date_time IS NULL AND Q2.signed_date_time IS NULL AND Q2.solution_portfolio_id IS NULL AND Q2.start_date_time IS NULL AND Q2.status_code IS NULL AND Q2.subclass_code IS NULL AND Q2.template_flag IS NULL AND Q2.template_used_name IS NULL AND Q2.termination_code IS NULL AND Q2.termination_date_time IS NULL AND Q2.termination_reason_code IS NULL AND Q2.total_list_price_amt_ent IS NULL AND Q2.tran_code IS NULL AND Q2.upg_orig_sys_ref_id IS NULL AND Q2.upg_orig_sys_ref_name IS NULL AND Q2.user_estimated_amt_ent IS NULL AND Q2.agreement_record_id IS NULL AND Q2.final_price_modifier_text IS NULL AND Q2.invoice_zero_amt_flag IS NULL AND Q2.bill_to_site_use_id IS NULL AND Q2.curr_cnvrsn_rate IS NULL AND Q2.curr_cnvrsn_rate_date IS NULL AND Q2.curr_cnvrsn_type_code IS NULL AND Q2.document_id IS NULL AND Q2.invoice_rule_id IS NULL AND Q2.invoice_term_id IS NULL AND Q2.renewal_end_date IS NULL AND Q2.renewal_type_code IS NULL AND Q2.ship_to_site_use_id IS NULL AND Q2.contract_subtype_code IS NULL AND Q2.new_for_new_duration IS NULL AND Q2.renewal_approval_type IS NULL AND Q2.suspend_credits_flag IS NULL AND Q2.hold_reason_description IS NULL AND Q2.agreement_start_date IS NULL AND Q2.agreement_end_date IS NULL
--DROP TABLE IF EXISTS  ${EEDDWWDDBB1}.contract_header_ld
--ALTER TABLE ${EEDDWWDDBB1}.contract_header_ldTemp RENAME TO ${EEDDWWDDBB1}.contract_header_ld
--Chngd NATASHA(CORRECTED the transformed query)

    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.contract_header_ld SELECT distinct
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
            ${EEDDWWDDBB1}.contract_header_ld 
        LEFT OUTER JOIN
            (
                SELECT
                    DISTINCT instance_id AS auto_c00,
                    contract_header_id AS auto_c01 
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2
            ) AS autoAlias_7 
                ON (
                    upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                    AND upper(trim(contract_header_id)) = upper(trim(autoAlias_7.AUTO_C01)) 
                ) 
        WHERE
            (
                autoAlias_7.AUTO_C00 IS NULL 
                AND autoAlias_7.AUTO_C01 IS NULL
            );


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2 FOR ACCESSINSERT INTO ${EEDDWWDDBB1}.contract_header_ld( contract_header_id, instance_id,    alt_as_of_date_time,    alt_batch_nbr,  approved_date_time, archived_flag,  as_of_date_time,    author_ncr_unit_id, author_oper_unit_country_code,  author_operating_unit_id,   auto_renew_day_cnt, award_contract_header_id,   batch_nbr,  cancelled_date_time,    character_type_code,    contract_est_amt_ent,   contract_intent_code,   contract_nbr,   contract_nbr_modifier,  contract_subtype_code,  created_by_name,    creation_date_time, curr_code,  cust_po_nbr,    cust_po_nbr_rqrd_flag,  deleted_flag,   end_date_time,  est_renew_amt_ent,  gsa_code,   inventory_org_id,   issue_receive_code, issued_date_time,   keep_on_mail_list_flag, last_update_date_time,  last_update_login_name, last_updated_by_name,   migration_contract_nbr, orig_sys_id,    orig_sys_ref_id,    orig_sys_source_code,   prepay_rqrd_flag,   price_list_header_id,   pricing_date_time,  projtd_close_date_time, proposed_date_time, qa_check_list_id,   renew_contract_header_id,   renew_curr_code,    renew_date_time,    renew_to_contract_header_id,    resolved_until_date_time,   resp_contract_header_id,    respond_date_time,  response_rqrd_copy_nbr, set_aside_pct,  sign_by_date_time,  signed_date_time,   solution_portfolio_id,  start_date_time,    status_code,        subclass_code,  template_flag,  template_used_name, termination_date_time,  termination_reason_code,    termination_code,   total_list_price_amt_ent,   tran_code,  upg_orig_sys_ref_name,  upg_orig_sys_ref_id,    user_estimated_amt_ent, agreement_record_id,        final_price_modifier_text,  invoice_zero_amt_flag,  bill_to_site_use_id,    curr_cnvrsn_rate,   curr_cnvrsn_rate_date,  curr_cnvrsn_type_code,  document_id,    invoice_rule_id,    invoice_term_id,    renewal_end_date,   renewal_type_code,  ship_to_site_use_id,    new_for_new_duration,   renewal_approval_type)SELECT    contract_header_id, instance_id,    alt_as_of_date_time,    alt_batch_nbr,  approved_date_time, archived_flag,  as_of_date_time,    author_ncr_unit_id, author_oper_unit_country_code,  author_operating_unit_id,   auto_renew_day_cnt, award_contract_header_id,   batch_nbr,  cancelled_date_time,    character_type_code,    contract_est_amt_ent,   contract_intent_code,   contract_nbr,   contract_nbr_modifier,  contract_subtype_code,  created_by_name,    creation_date_time, curr_code,  cust_po_nbr,    cust_po_nbr_rqrd_flag,  deleted_flag,   end_date_time,  est_renew_amt_ent,  gsa_code,   inventory_org_id,   issue_receive_code, issued_date_time,   keep_on_mail_list_flag, last_update_date_time,  last_update_login_name, last_updated_by_name,   migration_contract_nbr, orig_sys_id,    orig_sys_ref_id,    orig_sys_source_code,   prepay_rqrd_flag,   price_list_header_id,   pricing_date_time,  projtd_close_date_time, proposed_date_time, qa_check_list_id,   renew_contract_header_id,   renew_curr_code,    renew_date_time,    renew_to_contract_header_id,    resolved_until_date_time,   resp_contract_header_id,    respond_date_time,  response_rqrd_copy_nbr, set_aside_pct,  sign_by_date_time,  signed_date_time,   solution_portfolio_id,  start_date_time,    status_code,        subclass_code,  template_flag,  template_used_name, termination_date_time,  termination_reason_code,    termination_code,   total_list_price_amt_ent,   tran_code,  upg_orig_sys_ref_name,  upg_orig_sys_ref_id,    user_estimated_amt_ent, agreement_record_id,        final_price_modifier_text,  invoice_zero_amt_flag,  bill_to_site_use_id,    curr_cnvrsn_rate,   curr_cnvrsn_rate_date,  curr_cnvrsn_type_code,  document_id,    invoice_rule_id,    invoice_term_id,    renewal_end_date,   renewal_type_code,  ship_to_site_use_id,    new_for_new_duration,   renewal_approval_typeFROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_header_ld           SELECT distinct
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t2;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_header_08.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------



--Original Query:
  --DATABASE ${TTMMPPDDBB1}

--Translated Query: SUCCESS

use ${TTMMPPDDBB1};

--Original Query:
--INSERT INTO AAPPLLIIDD1EENNVV_as_of_date ( oper_unit_country_code, instance_id, as_of_date ) SELECT  '99', instance_id, MAX(CAST( as_of_date_time AS DATE) ) FROM AAPPLLIIDD1EENNVV_contract_header_ml GROUP BY 1,2  ; 

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           SELECT
            '99',
            instance_id,
            MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml      
        GROUP BY
            '99' ,
            instance_id;

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           SELECT
            author_oper_unit_country_code,
            instance_id,
            MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4      
        GROUP BY
            author_oper_unit_country_code ,
            instance_id;



--Translated Query: SUCCESS

    WITH qq1 AS (    SELECT distinct
        instance_id,
        batch_nbr  
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml 
    LEFT OUTER JOIN
        (SELECT
            DISTINCT instance_id AS auto_c00,
            batch_nbr AS auto_c01  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig ) AS autoAlias_7 
            ON ( upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
            AND batch_nbr = autoAlias_7.AUTO_C01 ) 
    WHERE
        autoAlias_7.AUTO_C00 IS  NULL  
        AND autoAlias_7.AUTO_C01 IS  NULL         ) 
        INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig       SELECT
                * 
            FROM
                qq1;