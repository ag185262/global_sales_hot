----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:10 
--Script Name: d8_contract_line_01.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --UPDATE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml SET orig_serviced_qty = '' WHERE orig_serviced_qty = '7 Days, 8am-8pm, 4 hr resp'  

--Translated Query: STATUS SUCCESS
--Not required Nitin
--    INSERT OVERWRITE
--        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml SELECT distinct 
--            AAPPLLIIDD1EENNVV_contract_line_ml.contract_line_id AS contract_line_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.instance_id AS instance_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.alt_contract_header_id AS alt_contract_header_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.as_of_date_time AS as_of_date_time ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.batch_nbr AS batch_nbr ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.config_complete_flag AS config_complete_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.config_header_id AS config_header_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.config_item_id AS config_item_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.config_item_type_code AS config_item_type_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.config_revision_nbr AS config_revision_nbr ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.config_valid_flag AS config_valid_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.contract_header_id AS contract_header_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.contract_line_nbr AS contract_line_nbr ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.contract_line_style_id AS contract_line_style_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.contract_price_type_code AS contract_price_type_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.created_by_name AS created_by_name ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.creation_date_time AS creation_date_time ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.curr_code AS curr_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.display_sequence_nbr AS display_sequence_nbr ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.end_date_time AS end_date_time ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.exception_flag AS exception_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.hidden_line_flag AS hidden_line_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.invoice_line_level_flag AS invoice_line_level_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.item_to_price_flag AS item_to_price_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.last_update_date_time AS last_update_date_time ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.last_update_login_name AS last_update_login_name ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.last_updated_by_name AS last_updated_by_name ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.line_list_price_amt_ent AS line_list_price_amt_ent ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.negot_price_amt_ent AS negot_price_amt_ent ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.negot_renew_price_amt_ent AS negot_renew_price_amt_ent ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.next_day_response_flag AS next_day_response_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.orig_sys_id AS orig_sys_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.orig_sys_ref_id AS orig_sys_ref_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.orig_sys_source_code AS orig_sys_source_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.parent_contract_line_id AS parent_contract_line_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_basis_flag AS price_basis_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_hold_adjust_est_pct AS price_hold_adjust_est_pct ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_hold_amt_ent AS price_hold_amt_ent ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_hold_break_code AS price_hold_break_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_hold_enfrc_list_flag AS price_hold_enfrc_list_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_hold_intgrtd_flag AS price_hold_intgrtd_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_hold_list_ref_id AS price_hold_list_ref_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_hold_prchs_min_amt_ent AS price_hold_prchs_min_amt_ent ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_hold_prchs_min_qty AS price_hold_prchs_min_qty ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_hold_price_type_code AS price_hold_price_type_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_level_flag AS price_level_flag ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_list_header_id AS price_list_header_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.price_list_line_id AS price_list_line_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.pricing_date_time AS pricing_date_time ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.renew_contract_line_id AS renew_contract_line_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.renew_curr_code AS renew_curr_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.renew_date_time AS renew_date_time ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.renew_to_contract_line_id AS renew_to_contract_line_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.sot_order_sot_line_nbr AS sot_order_sot_line_nbr ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.start_date_time AS start_date_time ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.status_code AS status_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.template_used_name AS template_used_name ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.termination_code AS termination_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.termination_date_time AS termination_date_time ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.termination_reason_code AS termination_reason_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.top_model_contract_line_id AS top_model_contract_line_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.tran_code AS tran_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.unit_price_amt_ent AS unit_price_amt_ent ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.unit_price_pct AS unit_price_pct ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.upg_orig_sys_ref_id AS upg_orig_sys_ref_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.upg_orig_sys_ref_name AS upg_orig_sys_ref_name ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.warranty_exp_reason_code AS warranty_exp_reason_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.bill_to_site_use_id AS bill_to_site_use_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.cust_acct_id AS cust_acct_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.invoice_rule_id AS invoice_rule_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.line_renewal_type_code AS line_renewal_type_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.ship_to_site_use_id AS ship_to_site_use_id ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.holiday_coverage_code AS holiday_coverage_code ,
--            CASE 
--                WHEN upper(trim(orig_serviced_qty)) = upper(trim('7 Days, 8am-8pm, 4 hr resp'))  THEN '' 
--                ELSE AAPPLLIIDD1EENNVV_contract_line_ml.orig_serviced_qty 
--            END AS orig_serviced_qty ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.reverse_logistics_text AS reverse_logistics_text ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.routing_comment_text AS routing_comment_text ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.source_location_name AS source_location_name ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.legacy_d1_severity_code AS legacy_d1_severity_code ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.crb_invoice_nbr_text AS crb_invoice_nbr_text ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.crb_reference_nbr_text AS crb_reference_nbr_text ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.mdm_product_identifier AS mdm_product_identifier ,
--            AAPPLLIIDD1EENNVV_contract_line_ml.mdm_solution_identifier AS mdm_solution_identifier 
--        FROM
--            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml AS AAPPLLIIDD1EENNVV_contract_line_ml;
--
----Original Query:
  --COLLECT STATISTICS  COLUMN TRAN_CODE, COLUMN STATUS_CODE ON ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml COMPUTE STATISTICS  FOR COLUMNS TRAN_CODE  , STATUS_CODE;

--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1;

--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml FOR ACCESS  INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 (         contract_line_id,         instance_id,         alt_contract_header_id,         as_of_date_time,         batch_nbr,         config_complete_flag,         config_header_id,         config_item_id,         config_item_type_code,         config_revision_nbr,         config_valid_flag,         contract_header_id,         contract_line_nbr,         contract_line_style_id,         contract_price_type_code,         created_by_name,         creation_date_time,         curr_code,         display_sequence_nbr,         end_date_time,         exception_flag,         hidden_line_flag,         invoice_line_level_flag,         item_to_price_flag,         last_update_date_time,         last_update_login_name,         last_updated_by_name,         line_list_price_amt_ent,         negot_price_amt_ent,         negot_renew_price_amt_ent,         orig_sys_id,         orig_sys_ref_id,         next_day_response_flag,         orig_sys_source_code,         parent_contract_line_id,         price_basis_flag,         price_hold_adjust_est_pct,         price_hold_amt_ent,         price_hold_break_code,         price_hold_enfrc_list_flag,         price_hold_intgrtd_flag,         price_hold_list_ref_id,         price_hold_price_type_code,         price_hold_prchs_min_qty,         price_hold_prchs_min_amt_ent,         price_level_flag,         price_list_header_id,         price_list_line_id,         pricing_date_time,         renew_contract_line_id,         renew_curr_code,         renew_date_time,         renew_to_contract_line_id,         sot_order_sot_line_nbr,         start_date_time,         status_code,         termination_code,         termination_reason_code,         top_model_contract_line_id,         tran_code,         unit_price_amt_ent,         unit_price_pct,         upg_orig_sys_ref_id,         upg_orig_sys_ref_name,         termination_date_time,         template_used_name,         warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,         orig_serviced_qty,         serviced_qty,         reverse_logistics_text,         routing_comment_text,         source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text ) SELECT         contract_line_id,         instance_id,         alt_contract_header_id,         as_of_date_time,         batch_nbr,         config_complete_flag,         config_header_id,         config_item_id,         config_item_type_code,         config_revision_nbr,         config_valid_flag,         contract_header_id,         contract_line_nbr,         contract_line_style_id,         contract_price_type_code,         created_by_name,         creation_date_time,         curr_code,         display_sequence_nbr,         end_date_time,         exception_flag,         hidden_line_flag,         invoice_line_level_flag,         item_to_price_flag,         last_update_date_time,         last_update_login_name,         last_updated_by_name,         line_list_price_amt_ent,         negot_price_amt_ent,         negot_renew_price_amt_ent,         orig_sys_id,         orig_sys_ref_id,         next_day_response_flag,         orig_sys_source_code,         parent_contract_line_id,         price_basis_flag,         price_hold_adjust_est_pct,         price_hold_amt_ent,         price_hold_break_code,         price_hold_enfrc_list_flag,         price_hold_intgrtd_flag,         price_hold_list_ref_id,         price_hold_price_type_code,         price_hold_prchs_min_qty,         price_hold_prchs_min_amt_ent,         price_level_flag,         price_list_header_id,         price_list_line_id,         pricing_date_time,         renew_contract_line_id,         renew_curr_code,         renew_date_time,         renew_to_contract_line_id,         sot_order_sot_line_nbr,         start_date_time,         status_code,         termination_code,         termination_reason_code,         top_model_contract_line_id,         tran_code,         unit_price_amt_ent,         unit_price_pct,         upg_orig_sys_ref_id,         upg_orig_sys_ref_name,         termination_date_time,         template_used_name,         warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,         orig_serviced_qty,         CASE            WHEN TRIM(orig_serviced_qty) NOT BETWEEN '0' AND '9999999999999' THEN 0            ELSE CAST(orig_serviced_qty AS INTEGER)         END,         reverse_logistics_text,<WM_COMMENT_START>Added for HSR ENTITLEMENT project<WM_COMMENT_END>         routing_comment_text,         source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml WHERE (CAST(as_of_date_time AS CHAR(26))||batch_nbr,         contract_line_id,         instance_id ) IN (SELECT  MAX(CAST(as_of_date_time AS CHAR(26))||batch_nbr),         contract_line_id,         instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml     WHERE tran_code <> 'D'     GROUP BY 2,3 ) AND tran_code <> 'D'  

--Translated Query: STATUS SUCCESS

INSERT INTO TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 
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
    FROM    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml m1 
    WHERE   upper(trim(tran_code)) <> trim('D')) rcrd 
WHERE   r00=1; 


--    INSERT 
--    INTO
--        TABLE
--        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1           SELECT distinct
--            contract_line_id,
--            instance_id,
--            alt_contract_header_id,
--            as_of_date_time,
--            batch_nbr,
--            config_complete_flag,
--            config_header_id,
--            config_item_id,
--            config_item_type_code,
--            config_revision_nbr,
--            config_valid_flag,
--            contract_header_id,
--            contract_line_nbr,
--            contract_line_style_id,
--            contract_price_type_code,
--            created_by_name,
--            creation_date_time,
--            curr_code,
--            display_sequence_nbr,
--            end_date_time,
--            exception_flag,
--            hidden_line_flag,
--            invoice_line_level_flag,
--            item_to_price_flag,
--            last_update_date_time,
--            last_update_login_name,
--            last_updated_by_name,
--            line_list_price_amt_ent,
--            negot_price_amt_ent,
--            negot_renew_price_amt_ent,
--            next_day_response_flag,
--            orig_sys_id,
--            orig_sys_ref_id,
--            orig_sys_source_code,
--            parent_contract_line_id,
--            price_basis_flag,
--            price_hold_adjust_est_pct,
--            price_hold_amt_ent,
--            price_hold_break_code,
--            price_hold_enfrc_list_flag,
--            price_hold_intgrtd_flag,
--            price_hold_list_ref_id,
--            price_hold_prchs_min_amt_ent,
--            price_hold_prchs_min_qty,
--            price_hold_price_type_code,
--            price_level_flag,
--            price_list_header_id,
--            price_list_line_id,
--            pricing_date_time,
--            renew_contract_line_id,
--            renew_curr_code,
--            renew_date_time,
--            renew_to_contract_line_id,
--            sot_order_sot_line_nbr,
--            start_date_time,
--            status_code,
--            template_used_name,
--            termination_code,
--            termination_date_time,
--            termination_reason_code,
--            top_model_contract_line_id,
--            tran_code,
--            unit_price_amt_ent,
--            unit_price_pct,
--            upg_orig_sys_ref_id,
--            upg_orig_sys_ref_name,
--            warranty_exp_reason_code,
--            bill_to_site_use_id,
--            cust_acct_id,
--            invoice_rule_id,
--            line_renewal_type_code,
--            ship_to_site_use_id,
--            holiday_coverage_code,
--            orig_serviced_qty,
--            CASE 
--             -- WHEN  ( UDF_TRIM( orig_serviced_qty ) NOT BETWEEN '0' AND '9999999999999' )  THEN 0  #santosh
--              WHEN  ( TRIM( orig_serviced_qty ) NOT BETWEEN '0' AND '9999999999999' )  THEN 0              
--             -- ELSE CAST_TO_INTEGER(orig_serviced_qty)  #santosh
--              ELSE INT(orig_serviced_qty)  
--            END AS auto_c074,
--            reverse_logistics_text,
--            routing_comment_text,
--            source_location_name,
--            legacy_d1_severity_code,
--            crb_invoice_nbr_text,
--            crb_reference_nbr_text,
--            mdm_product_identifier,
--            mdm_solution_identifier  
--        FROM
--            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml 
--        INNER JOIN
--            (
--                SELECT
--                    MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
--                    batch_nbr) ) AS auto_c00,
--                    contract_line_id AS auto_c01,
--                    instance_id AS auto_c02  
--                FROM
--                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml     
--                WHERE
--                    upper(trim(tran_code)) <> upper(trim('D'))  
--                GROUP BY
--                    contract_line_id ,
--                    instance_id 
--            ) AS autoAlias_111 
--                ON (
--                    CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
--                batch_nbr) = autoAlias_111.AUTO_C00 
--                AND upper(trim(contract_line_id)) = upper(trim(autoAlias_111.AUTO_C01)) 
--                AND upper(trim(instance_id)) = upper(trim(autoAlias_111.AUTO_C02)) ) 
--            WHERE
--                upper(trim(tran_code)) <> upper(trim('D'));
------------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:10 
--Script Name: d8_contract_line_02.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2;

--Original Query:
  --LOCK TABLE  ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 FOR ACCESS LOCK TABLE  ${EEDDWWDDBB1}.contract_line_ld FOR ACCESS  INSERT INTO ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 (  contract_line_id,   instance_id,    alt_contract_header_id,     alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  config_complete_flag,   config_header_id,   config_item_id,     config_item_type_code,  config_revision_nbr,    config_valid_flag,  contract_header_id,     contract_line_nbr,  contract_line_style_id,     contract_price_type_code,   created_by_name,    creation_date_time,     curr_code,  current_record_ind,     display_sequence_nbr,   end_date_time,  exception_flag,     hidden_line_flag,   invoice_line_level_flag,    item_to_price_flag,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   line_list_price_amt_ent,    negot_price_amt_ent,    negot_renew_price_amt_ent,  orig_sys_id,    orig_sys_ref_id,    next_day_response_flag,     orig_sys_source_code,   parent_contract_line_id,    price_basis_flag,   price_hold_adjust_est_pct,  price_hold_amt_ent,     price_hold_break_code,  price_hold_enfrc_list_flag,     price_hold_intgrtd_flag,    price_hold_list_ref_id,     price_hold_price_type_code,     price_hold_prchs_min_qty,   price_hold_prchs_min_amt_ent,   price_level_flag,   price_list_header_id,   price_list_line_id,     pricing_date_time,  renew_contract_line_id,     renew_curr_code,    renew_date_time,    renew_to_contract_line_id,  sot_order_sot_line_nbr,     start_date_time,    status_code,        termination_code,   termination_reason_code,    top_model_contract_line_id,     tran_code,  unit_price_amt_ent,     unit_price_pct,     upg_orig_sys_ref_id,    upg_orig_sys_ref_name,  termination_date_time,  template_used_name,     warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,   orig_serviced_qty,  serviced_qty,         reverse_logistics_text,  <WM_COMMENT_START>Added for HSR ENTITLEMENT project<WM_COMMENT_END>  routing_comment_text,   source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text ) SELECT    contract_line_id,   instance_id,    alt_contract_header_id,     alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  config_complete_flag,   config_header_id,   config_item_id,     config_item_type_code,  config_revision_nbr,    config_valid_flag,  contract_header_id,     contract_line_nbr,  contract_line_style_id,     contract_price_type_code,   created_by_name,    creation_date_time,     curr_code,  'Y',    display_sequence_nbr,   end_date_time,  exception_flag,     hidden_line_flag,   invoice_line_level_flag,    item_to_price_flag,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   line_list_price_amt_ent,    negot_price_amt_ent,    negot_renew_price_amt_ent,  orig_sys_id,    orig_sys_ref_id,    next_day_response_flag,     orig_sys_source_code,   parent_contract_line_id,    price_basis_flag,   price_hold_adjust_est_pct,  price_hold_amt_ent,     price_hold_break_code,  price_hold_enfrc_list_flag,     price_hold_intgrtd_flag,    price_hold_list_ref_id,     price_hold_price_type_code,     price_hold_prchs_min_qty,   price_hold_prchs_min_amt_ent,   price_level_flag,   price_list_header_id,   price_list_line_id,     pricing_date_time,  renew_contract_line_id,     renew_curr_code,    renew_date_time,    renew_to_contract_line_id,  sot_order_sot_line_nbr,     start_date_time,    status_code,        termination_code,   termination_reason_code,    top_model_contract_line_id,     tran_code,  unit_price_amt_ent,     unit_price_pct,     upg_orig_sys_ref_id,    upg_orig_sys_ref_name,  termination_date_time,  template_used_name,     warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,       orig_serviced_qty,  serviced_qty,         reverse_logistics_text,   routing_comment_text,   source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text FROM ${EEDDWWDDBB1}.contract_line_ld WHERE (contract_line_id, instance_id) IN (SELECT      contract_line_id,     instance_id     FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2           SELECT distinct
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
            ${EEDDWWDDBB1}.contract_line_ld 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_line_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 
            ) AS autoAlias_113 
                ON (
                    upper(trim(contract_line_id)) = upper(trim(autoAlias_113.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_113.AUTO_C01)) 
                );

--Original Query:
  --DELETE FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t3 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t3;

--Original Query:
  --LOCK TABLE  ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 FOR ACCESS LOCK TABLE  ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 FOR ACCESS  INSERT INTO ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t3 (     contract_line_id,   instance_id,    alt_contract_header_id,     config_complete_flag,   config_header_id,   config_item_id,     config_item_type_code,  config_revision_nbr,    config_valid_flag,  contract_header_id,     contract_line_nbr,  contract_line_style_id,     contract_price_type_code,   created_by_name,    creation_date_time,     curr_code,  display_sequence_nbr,   end_date_time,  exception_flag,     hidden_line_flag,   invoice_line_level_flag,    item_to_price_flag,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   line_list_price_amt_ent,    negot_price_amt_ent,    negot_renew_price_amt_ent,  orig_sys_id,    orig_sys_ref_id,    next_day_response_flag,     orig_sys_source_code,   parent_contract_line_id,    price_basis_flag,   price_hold_adjust_est_pct,  price_hold_amt_ent,     price_hold_break_code,  price_hold_enfrc_list_flag,     price_hold_intgrtd_flag,    price_hold_list_ref_id,     price_hold_price_type_code,     price_hold_prchs_min_qty,   price_hold_prchs_min_amt_ent,   price_level_flag,   price_list_header_id,   price_list_line_id,     pricing_date_time,  renew_contract_line_id,     renew_curr_code,    renew_date_time,    renew_to_contract_line_id,  sot_order_sot_line_nbr,     start_date_time,    status_code,        termination_code,   termination_reason_code,    top_model_contract_line_id,     tran_code,  unit_price_amt_ent,     unit_price_pct,     upg_orig_sys_ref_id,    upg_orig_sys_ref_name,  termination_date_time,  template_used_name,     warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,   orig_serviced_qty,  serviced_qty,         reverse_logistics_text,         routing_comment_text,         source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text ) SELECT    contract_line_id,   instance_id,    alt_contract_header_id,     config_complete_flag,   config_header_id,   config_item_id,     config_item_type_code,  config_revision_nbr,    config_valid_flag,  contract_header_id,     contract_line_nbr,  contract_line_style_id,     contract_price_type_code,   created_by_name,    creation_date_time,     curr_code,  display_sequence_nbr,   end_date_time,  exception_flag,     hidden_line_flag,   invoice_line_level_flag,    item_to_price_flag,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   line_list_price_amt_ent,    negot_price_amt_ent,    negot_renew_price_amt_ent,  orig_sys_id,    orig_sys_ref_id,    next_day_response_flag,     orig_sys_source_code,   parent_contract_line_id,    price_basis_flag,   price_hold_adjust_est_pct,  price_hold_amt_ent,     price_hold_break_code,  price_hold_enfrc_list_flag,     price_hold_intgrtd_flag,    price_hold_list_ref_id,     price_hold_price_type_code,     price_hold_prchs_min_qty,   price_hold_prchs_min_amt_ent,   price_level_flag,   price_list_header_id,   price_list_line_id,     pricing_date_time,  renew_contract_line_id,     renew_curr_code,    renew_date_time,    renew_to_contract_line_id,  sot_order_sot_line_nbr,     start_date_time,    status_code,        termination_code,   termination_reason_code,    top_model_contract_line_id,     tran_code,  unit_price_amt_ent,     unit_price_pct,     upg_orig_sys_ref_id,    upg_orig_sys_ref_name,  termination_date_time,  template_used_name,     warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,   orig_serviced_qty,      serviced_qty,         reverse_logistics_text,         routing_comment_text,         source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 WHERE (contract_line_id, instance_id) IN (SELECT      contract_line_id,     instance_id     FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t3           SELECT distinct
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
            ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_line_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 
            ) AS autoAlias_115 
                ON (
                    upper(trim(contract_line_id)) = upper(trim(autoAlias_115.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_115.AUTO_C01)) 
                );

--Original Query:
  --COLLECT STATISTICS ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 COLUMN (CURRENT_RECORD_IND) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 COMPUTE STATISTICS  FOR COLUMNS CURRENT_RECORD_IND;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:10 
--Script Name: d8_contract_line_03.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --LOCK TABLE  ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 FOR ACCESS LOCK TABLE  ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 FOR ACCESS  INSERT INTO ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t3 (         contract_line_id,         instance_id,         alt_contract_header_id,         config_complete_flag,         config_header_id,         config_item_id,         config_item_type_code,         config_revision_nbr,         config_valid_flag,         contract_header_id,         contract_line_nbr,         contract_line_style_id,         contract_price_type_code,         created_by_name,         creation_date_time,         curr_code,         display_sequence_nbr,         end_date_time,         exception_flag,         hidden_line_flag,         invoice_line_level_flag,         item_to_price_flag,         last_update_date_time,         last_update_login_name,         last_updated_by_name,         line_list_price_amt_ent,         negot_price_amt_ent,         negot_renew_price_amt_ent,         orig_sys_id,         orig_sys_ref_id,         next_day_response_flag,         orig_sys_source_code,         parent_contract_line_id,         price_basis_flag,         price_hold_adjust_est_pct,         price_hold_amt_ent,         price_hold_break_code,         price_hold_enfrc_list_flag,         price_hold_intgrtd_flag,         price_hold_list_ref_id,         price_hold_price_type_code,         price_hold_prchs_min_qty,         price_hold_prchs_min_amt_ent,         price_level_flag,         price_list_header_id,         price_list_line_id,         pricing_date_time,         renew_contract_line_id,         renew_curr_code,         renew_date_time,         renew_to_contract_line_id,         sot_order_sot_line_nbr,         start_date_time,         status_code,         termination_code,         termination_reason_code,         top_model_contract_line_id,         tran_code,         unit_price_amt_ent,         unit_price_pct,         upg_orig_sys_ref_id,         upg_orig_sys_ref_name,         termination_date_time,         template_used_name,         warranty_exp_reason_code,         bill_to_site_use_id ,         cust_acct_id ,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,         orig_serviced_qty,         serviced_qty,         reverse_logistics_text, <WM_COMMENT_START>Added for HSR ENTITLEMENT project<WM_COMMENT_END>         routing_comment_text,         source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text ) SELECT         contract_line_id,         instance_id,         alt_contract_header_id,         config_complete_flag,         config_header_id,         config_item_id,         config_item_type_code,         config_revision_nbr,         config_valid_flag,         contract_header_id,         contract_line_nbr,         contract_line_style_id,         contract_price_type_code,         created_by_name,         creation_date_time,         curr_code,         display_sequence_nbr,         end_date_time,         exception_flag,         hidden_line_flag,         invoice_line_level_flag,         item_to_price_flag,         last_update_date_time,         last_update_login_name,         last_updated_by_name,         line_list_price_amt_ent,         negot_price_amt_ent,         negot_renew_price_amt_ent,         orig_sys_id,         orig_sys_ref_id,         next_day_response_flag,         orig_sys_source_code,         parent_contract_line_id,         price_basis_flag,         price_hold_adjust_est_pct,         price_hold_amt_ent,         price_hold_break_code,         price_hold_enfrc_list_flag,         price_hold_intgrtd_flag,         price_hold_list_ref_id,         price_hold_price_type_code,         price_hold_prchs_min_qty,         price_hold_prchs_min_amt_ent,         price_level_flag,         price_list_header_id,         price_list_line_id,         pricing_date_time,         renew_contract_line_id,         renew_curr_code,         renew_date_time,         renew_to_contract_line_id,         sot_order_sot_line_nbr,         start_date_time,         status_code,         termination_code,         termination_reason_code,         top_model_contract_line_id,         tran_code,         unit_price_amt_ent,         unit_price_pct,         upg_orig_sys_ref_id,         upg_orig_sys_ref_name,         termination_date_time,         template_used_name,         warranty_exp_reason_code,         bill_to_site_use_id ,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,         orig_serviced_qty,         serviced_qty,         reverse_logistics_text,         routing_comment_text,         source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 WHERE (contract_line_id, instance_id) IN (SELECT     contract_line_id,     instance_id     FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t3           SELECT distinct
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
            ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_line_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 
            ) AS autoAlias_117 
                ON (
                    upper(trim(contract_line_id)) = upper(trim(autoAlias_117.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_117.AUTO_C01)) 
                );
--Original Query :
 --------------------------------
         --DATABASE ${ttmmppddbb1} 
--Transformed Query : STATUS SUCCESS
--------------------------------
        use ${ttmmppddbb1} ;

--Original Query :
 --------------------------------
         --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_t3 FOR ACCESSUPDATE ${AAPPLLIIDD1EENNVV}_contract_line_t2SET current_record_ind = 'D'WHERE (contract_line_id, instance_id) IN    (SELECT contract_line_id, instance_id    FROM ${AAPPLLIIDD1EENNVV}_contract_line_t3    GROUP BY contract_line_id, instance_id    HAVING COUNT(*) = 1    ) 
--Transformed Query : STATUS SUCCESS
--------------------------------
    INSERT OVERWRITE TABLE ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 
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
FROM    ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 AS AAPPLLIIDD1EENNVV_contract_line_t2 
Left Outer Join (
    SELECT   contract_line_id AS auto_c00, instance_id AS auto_c01  
    FROM     ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t3      
    GROUP BY contract_line_id,instance_id 
    HAVING  COUNT(*) = 1)autoAlias_119 
    ON (autoAlias_119.auto_c00 = AAPPLLIIDD1EENNVV_contract_line_t2.contract_line_id 
    AND autoAlias_119.auto_c01 = AAPPLLIIDD1EENNVV_contract_line_t2.instance_id);

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:11 
--Script Name: d8_contract_line_05.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${EEDDWWDDBB1}.contract_line_ld  WHERE (contract_line_id, instance_id) IN (SELECT contract_line_id, instance_id     FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1     WHERE (contract_line_id, instance_id) NOT IN  (SELECT contract_line_id, instance_id   FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2   WHERE current_record_ind = 'D'  )     )  

--Translated Query: STATUS SUCCESS
-- wrong conversion . Commented temp by Nitin
--    INSERT OVERWRITE
--        TABLE ${EEDDWWDDBB1}.contract_line_ld SELECT
--            Q1.* 
--        FROM
--            (SELECT
--                * 
--            FROM
--                ${EEDDWWDDBB1}.contract_line_ld ) AS  Q1  
--        LEFT OUTER JOIN
--            (
--                SELECT
--                    * 
--                FROM
--                    ${EEDDWWDDBB1}.contract_line_ld      
--            ) AS Q2 
--                ON COALESCE( Q1.contract_line_id ,
--            '1' ) = COALESCE( Q2.contract_line_id ,
--            '1' ) 
--            AND COALESCE( Q1.instance_id ,
--            '1' ) = COALESCE( Q2.instance_id ,
--            '1' ) 
--            AND COALESCE( Q1.alt_as_of_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.alt_as_of_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) 
--            AND COALESCE( Q1.alt_batch_nbr ,
--            1) = COALESCE( Q2.alt_batch_nbr ,
--            1) 
--            AND COALESCE( Q1.alt_contract_header_id ,
--            '1' ) = COALESCE( Q2.alt_contract_header_id ,
--            '1' ) 
--            AND COALESCE( Q1.as_of_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.as_of_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) 
--            AND COALESCE( Q1.batch_nbr ,
--            1) = COALESCE( Q2.batch_nbr ,
--            1) 
--            AND COALESCE( Q1.config_complete_flag ,
--            '1' ) = COALESCE( Q2.config_complete_flag ,
--            '1' ) 
--            AND COALESCE( Q1.config_header_id ,
--            '1' ) = COALESCE( Q2.config_header_id ,
--            '1' ) 
--            AND COALESCE( Q1.config_item_id ,
--            '1' ) = COALESCE( Q2.config_item_id ,
--            '1' ) 
--            AND COALESCE( Q1.config_item_type_code ,
--            '1' ) = COALESCE( Q2.config_item_type_code ,
--            '1' ) 
--            AND COALESCE( Q1.config_revision_nbr ,
--            1) = COALESCE( Q2.config_revision_nbr ,
--            1) 
--            AND COALESCE( Q1.config_valid_flag ,
--            '1' ) = COALESCE( Q2.config_valid_flag ,
--            '1' ) 
--            AND COALESCE( Q1.contract_header_id ,
--            '1' ) = COALESCE( Q2.contract_header_id ,
--            '1' ) 
--            AND COALESCE( Q1.contract_line_nbr ,
--            '1' ) = COALESCE( Q2.contract_line_nbr ,
--            '1' ) 
--            AND COALESCE( Q1.contract_line_style_id ,
--            1) = COALESCE( Q2.contract_line_style_id ,
--            1) 
--            AND COALESCE( Q1.contract_price_type_code ,
--            '1' ) = COALESCE( Q2.contract_price_type_code ,
--            '1' ) 
--            AND COALESCE( Q1.created_by_name ,
--            '1' ) = COALESCE( Q2.created_by_name ,
--            '1' ) 
--            AND COALESCE( Q1.creation_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) 
--            AND COALESCE( Q1.curr_code ,
--            '1' ) = COALESCE( Q2.curr_code ,
--            '1' ) 
--            AND COALESCE( Q1.display_sequence_nbr ,
--            1) = COALESCE( Q2.display_sequence_nbr ,
--            1) 
--            AND COALESCE( Q1.end_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.end_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) 
--            AND COALESCE( Q1.exception_flag ,
--            '1' ) = COALESCE( Q2.exception_flag ,
--            '1' ) 
--            AND COALESCE( Q1.hidden_line_flag ,
--            '1' ) = COALESCE( Q2.hidden_line_flag ,
--            '1' ) 
--            AND COALESCE( Q1.invoice_line_level_flag ,
--            '1' ) = COALESCE( Q2.invoice_line_level_flag ,
--            '1' ) 
--            AND COALESCE( Q1.item_to_price_flag ,
--            '1' ) = COALESCE( Q2.item_to_price_flag ,
--            '1' ) 
--            AND COALESCE( Q1.last_update_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) 
--            AND COALESCE( Q1.last_update_login_name ,
--            '1' ) = COALESCE( Q2.last_update_login_name ,
--            '1' ) 
--            AND COALESCE( Q1.last_updated_by_name ,
--            '1' ) = COALESCE( Q2.last_updated_by_name ,
--            '1' ) 
--            AND COALESCE( Q1.line_list_price_amt_ent ,
--            1) = COALESCE( Q2.line_list_price_amt_ent ,
--            1) 
--            AND COALESCE( Q1.negot_price_amt_ent ,
--            1) = COALESCE( Q2.negot_price_amt_ent ,
--            1) 
--            AND COALESCE( Q1.negot_renew_price_amt_ent ,
--            1) = COALESCE( Q2.negot_renew_price_amt_ent ,
--            1) 
--            AND COALESCE( Q1.next_day_response_flag ,
--            '1' ) = COALESCE( Q2.next_day_response_flag ,
--            '1' ) 
--            AND COALESCE( Q1.orig_sys_id ,
--            '1' ) = COALESCE( Q2.orig_sys_id ,
--            '1' ) 
--            AND COALESCE( Q1.orig_sys_ref_id ,
--            '1' ) = COALESCE( Q2.orig_sys_ref_id ,
--            '1' ) 
--            AND COALESCE( Q1.orig_sys_source_code ,
--            '1' ) = COALESCE( Q2.orig_sys_source_code ,
--            '1' ) 
--            AND COALESCE( Q1.parent_contract_line_id ,
--            '1' ) = COALESCE( Q2.parent_contract_line_id ,
--            '1' ) 
--            AND COALESCE( Q1.price_basis_flag ,
--            '1' ) = COALESCE( Q2.price_basis_flag ,
--            '1' ) 
--            AND COALESCE( Q1.price_hold_adjust_est_pct ,
--            1) = COALESCE( Q2.price_hold_adjust_est_pct ,
--            1) 
--            AND COALESCE( Q1.price_hold_amt_ent ,
--            1) = COALESCE( Q2.price_hold_amt_ent ,
--            1) 
--            AND COALESCE( Q1.price_hold_break_code ,
--            '1' ) = COALESCE( Q2.price_hold_break_code ,
--            '1' ) 
--            AND COALESCE( Q1.price_hold_enfrc_list_flag ,
--            '1' ) = COALESCE( Q2.price_hold_enfrc_list_flag ,
--            '1' ) 
--            AND COALESCE( Q1.price_hold_intgrtd_flag ,
--            '1' ) = COALESCE( Q2.price_hold_intgrtd_flag ,
--            '1' ) 
--            AND COALESCE( Q1.price_hold_list_ref_id ,
--            1) = COALESCE( Q2.price_hold_list_ref_id ,
--            1) 
--            AND COALESCE( Q1.price_hold_prchs_min_amt_ent ,
--            1) = COALESCE( Q2.price_hold_prchs_min_amt_ent ,
--            1) 
--            AND COALESCE( Q1.price_hold_prchs_min_qty ,
--            1) = COALESCE( Q2.price_hold_prchs_min_qty ,
--            1) 
--            AND COALESCE( Q1.price_hold_price_type_code ,
--            '1' ) = COALESCE( Q2.price_hold_price_type_code ,
--            '1' ) 
--            AND COALESCE( Q1.price_level_flag ,
--            '1' ) = COALESCE( Q2.price_level_flag ,
--            '1' ) 
--            AND COALESCE( Q1.price_list_header_id ,
--            1) = COALESCE( Q2.price_list_header_id ,
--            1) 
--            AND COALESCE( Q1.price_list_line_id ,
--            1) = COALESCE( Q2.price_list_line_id ,
--            1) 
--            AND COALESCE( Q1.pricing_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.pricing_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) 
--            AND COALESCE( Q1.renew_contract_line_id ,
--            '1' ) = COALESCE( Q2.renew_contract_line_id ,
--            '1' ) 
--            AND COALESCE( Q1.renew_curr_code ,
--            '1' ) = COALESCE( Q2.renew_curr_code ,
--            '1' ) 
--            AND COALESCE( Q1.renew_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.renew_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) 
--            AND COALESCE( Q1.renew_to_contract_line_id ,
--            '1' ) = COALESCE( Q2.renew_to_contract_line_id ,
--            '1' ) 
--            AND COALESCE( Q1.sot_order_sot_line_nbr ,
--            '1' ) = COALESCE( Q2.sot_order_sot_line_nbr ,
--            '1' ) 
--            AND COALESCE( Q1.start_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.start_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) 
--            AND COALESCE( Q1.status_code ,
--            '1' ) = COALESCE( Q2.status_code ,
--            '1' ) 
--            AND COALESCE( Q1.template_used_name ,
--            '1' ) = COALESCE( Q2.template_used_name ,
--            '1' ) 
--            AND COALESCE( Q1.termination_code ,
--            '1' ) = COALESCE( Q2.termination_code ,
--            '1' ) 
--            AND COALESCE( Q1.termination_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.termination_date_time ,
--            cast ( CURRENT_DATE() as timestamp) ) 
--            AND COALESCE( Q1.termination_reason_code ,
--            '1' ) = COALESCE( Q2.termination_reason_code ,
--            '1' ) 
--            AND COALESCE( Q1.top_model_contract_line_id ,
--            '1' ) = COALESCE( Q2.top_model_contract_line_id ,
--            '1' ) 
--            AND COALESCE( Q1.tran_code ,
--            '1' ) = COALESCE( Q2.tran_code ,
--            '1' ) 
--            AND COALESCE( Q1.unit_price_amt_ent ,
--            1) = COALESCE( Q2.unit_price_amt_ent ,
--            1) 
--            AND COALESCE( Q1.unit_price_pct ,
--            1) = COALESCE( Q2.unit_price_pct ,
--            1) 
--            AND COALESCE( Q1.upg_orig_sys_ref_id ,
--            '1' ) = COALESCE( Q2.upg_orig_sys_ref_id ,
--            '1' ) 
--            AND COALESCE( Q1.upg_orig_sys_ref_name ,
--            '1' ) = COALESCE( Q2.upg_orig_sys_ref_name ,
--            '1' ) 
--            AND COALESCE( Q1.warranty_exp_reason_code ,
--            '1' ) = COALESCE( Q2.warranty_exp_reason_code ,
--            '1' ) 
--            AND COALESCE( Q1.bill_to_site_use_id ,
--            1) = COALESCE( Q2.bill_to_site_use_id ,
--            1) 
--            AND COALESCE( Q1.cust_acct_id ,
--            1) = COALESCE( Q2.cust_acct_id ,
--            1) 
--            AND COALESCE( Q1.invoice_rule_id ,
--            1) = COALESCE( Q2.invoice_rule_id ,
--            1) 
--            AND COALESCE( Q1.line_renewal_type_code ,
--            '1' ) = COALESCE( Q2.line_renewal_type_code ,
--            '1' ) 
--            AND COALESCE( Q1.ship_to_site_use_id ,
--            1) = COALESCE( Q2.ship_to_site_use_id ,
--            1) 
--            AND COALESCE( Q1.holiday_coverage_code ,
--            '1' ) = COALESCE( Q2.holiday_coverage_code ,
--            '1' ) 
--            AND COALESCE( Q1.orig_serviced_qty ,
--            '1' ) = COALESCE( Q2.orig_serviced_qty ,
--            '1' ) 
--            AND COALESCE( Q1.serviced_qty ,
--            1) = COALESCE( Q2.serviced_qty ,
--            1) 
--            AND COALESCE( Q1.routing_comment_text ,
--            '1' ) = COALESCE( Q2.routing_comment_text ,
--            '1' ) 
--            AND COALESCE( Q1.reverse_logistics_text ,
--            '1' ) = COALESCE( Q2.reverse_logistics_text ,
--            '1' ) 
--            AND COALESCE( Q1.source_location_name ,
--            '1' ) = COALESCE( Q2.source_location_name ,
--            '1' ) 
--            AND COALESCE( Q1.legacy_d1_severity_code ,
--            '1' ) = COALESCE( Q2.legacy_d1_severity_code ,
--            '1' ) 
--            AND COALESCE( Q1.crb_invoice_nbr_text ,
--            '1' ) = COALESCE( Q2.crb_invoice_nbr_text ,
--            '1' ) 
--            AND COALESCE( Q1.crb_reference_nbr_text ,
--            '1' ) = COALESCE( Q2.crb_reference_nbr_text ,
--            '1' ) 
--            AND COALESCE( Q1.mdm_product_identifier ,
--            '1' ) = COALESCE( Q2.mdm_product_identifier ,
--            '1' ) 
--            AND COALESCE( Q1.mdm_solution_identifier ,
--            '1' ) = COALESCE( Q2.mdm_solution_identifier ,
--            '1' ) 
--        WHERE
--            Q2.contract_line_id IS NULL 
--            AND Q2.instance_id IS NULL 
--            AND Q2.alt_as_of_date_time IS NULL 
--            AND Q2.alt_batch_nbr IS NULL 
--            AND Q2.alt_contract_header_id IS NULL 
--            AND Q2.as_of_date_time IS NULL 
--            AND Q2.batch_nbr IS NULL 
--            AND Q2.config_complete_flag IS NULL 
--            AND Q2.config_header_id IS NULL 
--            AND Q2.config_item_id IS NULL 
--            AND Q2.config_item_type_code IS NULL 
--            AND Q2.config_revision_nbr IS NULL 
--            AND Q2.config_valid_flag IS NULL 
--            AND Q2.contract_header_id IS NULL 
--            AND Q2.contract_line_nbr IS NULL 
--            AND Q2.contract_line_style_id IS NULL 
--            AND Q2.contract_price_type_code IS NULL 
--            AND Q2.created_by_name IS NULL 
--            AND Q2.creation_date_time IS NULL 
--            AND Q2.curr_code IS NULL 
--            AND Q2.display_sequence_nbr IS NULL 
--            AND Q2.end_date_time IS NULL 
--            AND Q2.exception_flag IS NULL 
--            AND Q2.hidden_line_flag IS NULL 
--            AND Q2.invoice_line_level_flag IS NULL 
--            AND Q2.item_to_price_flag IS NULL 
--            AND Q2.last_update_date_time IS NULL 
--            AND Q2.last_update_login_name IS NULL 
--            AND Q2.last_updated_by_name IS NULL 
--            AND Q2.line_list_price_amt_ent IS NULL 
--            AND Q2.negot_price_amt_ent IS NULL 
--            AND Q2.negot_renew_price_amt_ent IS NULL 
--            AND Q2.next_day_response_flag IS NULL 
--            AND Q2.orig_sys_id IS NULL 
--            AND Q2.orig_sys_ref_id IS NULL 
--            AND Q2.orig_sys_source_code IS NULL 
--            AND Q2.parent_contract_line_id IS NULL 
--            AND Q2.price_basis_flag IS NULL 
--            AND Q2.price_hold_adjust_est_pct IS NULL 
--            AND Q2.price_hold_amt_ent IS NULL 
--            AND Q2.price_hold_break_code IS NULL 
--            AND Q2.price_hold_enfrc_list_flag IS NULL 
--            AND Q2.price_hold_intgrtd_flag IS NULL 
--            AND Q2.price_hold_list_ref_id IS NULL 
--            AND Q2.price_hold_prchs_min_amt_ent IS NULL 
--            AND Q2.price_hold_prchs_min_qty IS NULL 
--            AND Q2.price_hold_price_type_code IS NULL 
--            AND Q2.price_level_flag IS NULL 
--            AND Q2.price_list_header_id IS NULL 
--            AND Q2.price_list_line_id IS NULL 
--            AND Q2.pricing_date_time IS NULL 
--            AND Q2.renew_contract_line_id IS NULL 
--            AND Q2.renew_curr_code IS NULL 
--            AND Q2.renew_date_time IS NULL 
--            AND Q2.renew_to_contract_line_id IS NULL 
--            AND Q2.sot_order_sot_line_nbr IS NULL 
--            AND Q2.start_date_time IS NULL 
--            AND Q2.status_code IS NULL 
--            AND Q2.template_used_name IS NULL 
--            AND Q2.termination_code IS NULL 
--            AND Q2.termination_date_time IS NULL 
--            AND Q2.termination_reason_code IS NULL 
--            AND Q2.top_model_contract_line_id IS NULL 
--            AND Q2.tran_code IS NULL 
--            AND Q2.unit_price_amt_ent IS NULL 
--            AND Q2.unit_price_pct IS NULL 
--            AND Q2.upg_orig_sys_ref_id IS NULL 
--            AND Q2.upg_orig_sys_ref_name IS NULL 
--            AND Q2.warranty_exp_reason_code IS NULL 
--            AND Q2.bill_to_site_use_id IS NULL 
--            AND Q2.cust_acct_id IS NULL 
--            AND Q2.invoice_rule_id IS NULL 
--            AND Q2.line_renewal_type_code IS NULL 
--            AND Q2.ship_to_site_use_id IS NULL 
--            AND Q2.holiday_coverage_code IS NULL 
--            AND Q2.orig_serviced_qty IS NULL 
--            AND Q2.serviced_qty IS NULL 
--            AND Q2.routing_comment_text IS NULL 
--            AND Q2.reverse_logistics_text IS NULL 
--            AND Q2.source_location_name IS NULL 
--            AND Q2.legacy_d1_severity_code IS NULL 
--            AND Q2.crb_invoice_nbr_text IS NULL 
--            AND Q2.crb_reference_nbr_text IS NULL 
--            AND Q2.mdm_product_identifier IS NULL 
--            AND Q2.mdm_solution_identifier IS NULL;
--
--Natasha : corrected the query         
INSERT OVERWRITE
         TABLE ${EEDDWWDDBB1}.contract_line_ld 
         SELECT distinct Q1.* FROM  ${EEDDWWDDBB1}.contract_line_ld AS Q1
         LEFT OUTER JOIN 
                (
                SELECT contract_line_id, instance_id
                FROM
                 ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 AS Q2  
                LEFT OUTER JOIN 
                    (   SELECT contract_line_id AS autoc000 , instance_id as autoc001
                    FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 
                        WHERE upper(trim(current_record_ind)) = 'D' ) AS Q3 
                ON Q2.contract_line_id = Q3.autoc000
                AND Q2.instance_id = Q3.autoc001
                    WHERE Q3.autoc000 IS NULL 
                    AND Q3.autoc001 IS NULL
                ) AS Q23
         ON Q1.contract_line_id = Q23.contract_line_id
         AND Q1.instance_id = Q23.instance_id
            WHERE Q23.contract_line_id IS NULL 
            AND Q23.instance_id IS NULL;    

--Original Query:
  --LOCK TABLE  ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 FOR ACCESS  INSERT INTO ${EEDDWWDDBB1}.contract_line_ld (  contract_line_id,   instance_id,    alt_contract_header_id,     alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  config_complete_flag,   config_header_id,   config_item_id,     config_item_type_code,  config_revision_nbr,    config_valid_flag,  contract_header_id,     contract_line_nbr,  contract_line_style_id,     contract_price_type_code,   created_by_name,    creation_date_time,     curr_code,  display_sequence_nbr,   end_date_time,  exception_flag,     hidden_line_flag,   invoice_line_level_flag,    item_to_price_flag,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   line_list_price_amt_ent,    negot_price_amt_ent,    negot_renew_price_amt_ent,  orig_sys_id,    orig_sys_ref_id,    next_day_response_flag,     orig_sys_source_code,   parent_contract_line_id,    price_basis_flag,   price_hold_adjust_est_pct,  price_hold_amt_ent,     price_hold_break_code,  price_hold_enfrc_list_flag,     price_hold_intgrtd_flag,    price_hold_list_ref_id,     price_hold_price_type_code,     price_hold_prchs_min_qty,   price_hold_prchs_min_amt_ent,   price_level_flag,   price_list_header_id,   price_list_line_id,     pricing_date_time,  renew_contract_line_id,     renew_curr_code,    renew_date_time,    renew_to_contract_line_id,  sot_order_sot_line_nbr,     start_date_time,    status_code,        termination_code,   termination_reason_code,    top_model_contract_line_id,     tran_code,  unit_price_amt_ent,     unit_price_pct,     upg_orig_sys_ref_id,    upg_orig_sys_ref_name,  termination_date_time,  template_used_name,     warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,   orig_serviced_qty,  serviced_qty,   reverse_logistics_text, <WM_COMMENT_START>Added for HSR ENTITLEMENT project<WM_COMMENT_END>     routing_comment_text,   source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text  ) SELECT   contract_line_id,   instance_id,    alt_contract_header_id,     as_of_date_time,    batch_nbr,  as_of_date_time,    batch_nbr,  config_complete_flag,   config_header_id,   config_item_id,     config_item_type_code,  config_revision_nbr,    config_valid_flag,  contract_header_id,     contract_line_nbr,  contract_line_style_id,     contract_price_type_code,   created_by_name,    creation_date_time,     curr_code,  display_sequence_nbr,   end_date_time,  exception_flag,     hidden_line_flag,   invoice_line_level_flag,    item_to_price_flag,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   line_list_price_amt_ent,    negot_price_amt_ent,    negot_renew_price_amt_ent,  orig_sys_id,    orig_sys_ref_id,    next_day_response_flag,     orig_sys_source_code,   parent_contract_line_id,    price_basis_flag,   price_hold_adjust_est_pct,  price_hold_amt_ent,     price_hold_break_code,  price_hold_enfrc_list_flag,     price_hold_intgrtd_flag,    price_hold_list_ref_id,     price_hold_price_type_code,     price_hold_prchs_min_qty,   price_hold_prchs_min_amt_ent,   price_level_flag,   price_list_header_id,   price_list_line_id,     pricing_date_time,  renew_contract_line_id,     renew_curr_code,    renew_date_time,    renew_to_contract_line_id,  sot_order_sot_line_nbr,     start_date_time,    status_code,        termination_code,   termination_reason_code,    top_model_contract_line_id,     tran_code,  unit_price_amt_ent,     unit_price_pct,     upg_orig_sys_ref_id,    upg_orig_sys_ref_name,  termination_date_time,  template_used_name,     warranty_exp_reason_code,         bill_to_site_use_id ,         cust_acct_id ,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,     orig_serviced_qty,  serviced_qty,         reverse_logistics_text,   routing_comment_text,   source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 WHERE ( contract_line_id, instance_id ) NOT IN (SELECT  contract_line_id, instance_id     FROM ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2     WHERE current_record_ind = 'D'      )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_line_ld           SELECT 
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
            ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 
        LEFT OUTER JOIN
            (
                SELECT
                    contract_line_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${ttmmppddbb1}.${AAPPLLIIDD1EENNVV}_contract_line_t2     
                WHERE
                    upper(trim(current_record_ind)) = 'D' 
            ) AS autoAlias_123 
                ON (
                    upper(trim(contract_line_id)) = upper(trim(autoAlias_123.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_123.AUTO_C01)) 
                ) 
        WHERE
            autoAlias_123.AUTO_C00 IS  NULL  
            AND autoAlias_123.AUTO_C01 IS  NULL;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:12 
--Script Name: d8_contract_line_06.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1;

--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml FOR ACCESS  INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 (         contract_line_id,         instance_id,         alt_contract_header_id,         as_of_date_time,         batch_nbr,         config_complete_flag,         config_header_id,         config_item_id,         config_item_type_code,         config_revision_nbr,         config_valid_flag,         contract_header_id,         contract_line_nbr,         contract_line_style_id,         contract_price_type_code,         created_by_name,         creation_date_time,         curr_code,         display_sequence_nbr,         end_date_time,         exception_flag,         hidden_line_flag,         invoice_line_level_flag,         item_to_price_flag,         last_update_date_time,         last_update_login_name,         last_updated_by_name,         line_list_price_amt_ent,         negot_price_amt_ent,         negot_renew_price_amt_ent,         orig_sys_id,         orig_sys_ref_id,         next_day_response_flag,         orig_sys_source_code,         parent_contract_line_id,         price_basis_flag,         price_hold_adjust_est_pct,         price_hold_amt_ent,         price_hold_break_code,         price_hold_enfrc_list_flag,         price_hold_intgrtd_flag,         price_hold_list_ref_id,         price_hold_price_type_code,         price_hold_prchs_min_qty,         price_hold_prchs_min_amt_ent,         price_level_flag,         price_list_header_id,         price_list_line_id,         pricing_date_time,         renew_contract_line_id,         renew_curr_code,         renew_date_time,         renew_to_contract_line_id,         sot_order_sot_line_nbr,         start_date_time,         status_code,         termination_code,         termination_reason_code,         top_model_contract_line_id,         tran_code,         unit_price_amt_ent,         unit_price_pct,         upg_orig_sys_ref_id,         upg_orig_sys_ref_name,         termination_date_time,         template_used_name,         warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,         orig_serviced_qty,         serviced_qty,         reverse_logistics_text,  <WM_COMMENT_START>Added for HSR ENTITLEMENT project<WM_COMMENT_END>         routing_comment_text,         source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text ) SELECT         contract_line_id,         instance_id,         alt_contract_header_id,         as_of_date_time,         batch_nbr,         config_complete_flag,         config_header_id,         config_item_id,         config_item_type_code,         config_revision_nbr,         config_valid_flag,         contract_header_id,         contract_line_nbr,         contract_line_style_id,         contract_price_type_code,         created_by_name,         creation_date_time,         curr_code,         display_sequence_nbr,         end_date_time,         exception_flag,         hidden_line_flag,         invoice_line_level_flag,         item_to_price_flag,         last_update_date_time,         last_update_login_name,         last_updated_by_name,         line_list_price_amt_ent,         negot_price_amt_ent,         negot_renew_price_amt_ent,         orig_sys_id,         orig_sys_ref_id,         next_day_response_flag,         orig_sys_source_code,         parent_contract_line_id,         price_basis_flag,         price_hold_adjust_est_pct,         price_hold_amt_ent,         price_hold_break_code,         price_hold_enfrc_list_flag,         price_hold_intgrtd_flag,         price_hold_list_ref_id,         price_hold_price_type_code,         price_hold_prchs_min_qty,         price_hold_prchs_min_amt_ent,         price_level_flag,         price_list_header_id,         price_list_line_id,         pricing_date_time,         renew_contract_line_id,         renew_curr_code,         renew_date_time,         renew_to_contract_line_id,         sot_order_sot_line_nbr,         start_date_time,         status_code,         termination_code,         termination_reason_code,         top_model_contract_line_id,         tran_code,         unit_price_amt_ent,         unit_price_pct,         upg_orig_sys_ref_id,         upg_orig_sys_ref_name,         termination_date_time,         template_used_name,         warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,         orig_serviced_qty,         CASE WHEN trim(orig_serviced_qty) not between '0' and '9999999999999' THEN 0              ELSE CAST(orig_serviced_qty AS INTEGER)         END,         reverse_logistics_text,         routing_comment_text,         source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,         contract_line_id,         instance_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),         contract_line_id,         instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml     WHERE tran_code = 'D'     GROUP BY 2,3 ) AND tran_code = 'D'  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1           SELECT distinct
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
                -- WHEN  ( UDF_TRIM( orig_serviced_qty ) NOT BETWEEN '0' AND '9999999999999' )  THEN 0  #santosh
                -- ELSE CAST_TO_INTEGER(orig_serviced_qty)  #santosh
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml 
        INNER JOIN
            (
                SELECT
                    MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    batch_nbr) ) AS auto_c00,
                    contract_line_id AS auto_c01,
                    instance_id AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_ml     
                WHERE
                    upper(trim(tran_code)) = upper(trim('D'))  
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
                upper(trim(tran_code)) = upper(trim('D'));

--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t2;

--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 FOR ACCESS  INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 (         contract_line_id,         instance_id,         alt_contract_header_id,         alt_as_of_date_time,         alt_batch_nbr,         as_of_date_time,         batch_nbr,         config_complete_flag,         config_header_id,         config_item_id,         config_item_type_code,         config_revision_nbr,         config_valid_flag,         contract_header_id,         contract_line_nbr,         contract_line_style_id,         contract_price_type_code,         created_by_name,         creation_date_time,         curr_code,         display_sequence_nbr,         end_date_time,         exception_flag,         hidden_line_flag,         invoice_line_level_flag,         item_to_price_flag,         last_update_date_time,         last_update_login_name,         last_updated_by_name,         line_list_price_amt_ent,         negot_price_amt_ent,         negot_renew_price_amt_ent,         orig_sys_id,         orig_sys_ref_id,         next_day_response_flag,         orig_sys_source_code,         parent_contract_line_id,         price_basis_flag,         price_hold_adjust_est_pct,         price_hold_amt_ent,         price_hold_break_code,         price_hold_enfrc_list_flag,         price_hold_intgrtd_flag,         price_hold_list_ref_id,         price_hold_price_type_code,         price_hold_prchs_min_qty,         price_hold_prchs_min_amt_ent,         price_level_flag,         price_list_header_id,         price_list_line_id,         pricing_date_time,         renew_contract_line_id,         renew_curr_code,         renew_date_time,         renew_to_contract_line_id,         sot_order_sot_line_nbr,         start_date_time,         status_code,         termination_code,         termination_reason_code,         top_model_contract_line_id,         tran_code,         unit_price_amt_ent,         unit_price_pct,         upg_orig_sys_ref_id,         upg_orig_sys_ref_name,         termination_date_time,         template_used_name,         warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,         orig_serviced_qty,         serviced_qty,         reverse_logistics_text,  <WM_COMMENT_START>Added for HSR ENTITLEMENT project<WM_COMMENT_END>         routing_comment_text,         source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text ) SELECT         l.contract_line_id,         l.instance_id,         l.alt_contract_header_id,         t.as_of_date_time,         t.batch_nbr,         l.as_of_date_time,         l.batch_nbr,         l.config_complete_flag,         l.config_header_id,         l.config_item_id,         l.config_item_type_code,         l.config_revision_nbr,         l.config_valid_flag,         l.contract_header_id,         l.contract_line_nbr,         l.contract_line_style_id,         l.contract_price_type_code,         l.created_by_name,         l.creation_date_time,         l.curr_code,         l.display_sequence_nbr,         l.end_date_time,         l.exception_flag,         l.hidden_line_flag,         l.invoice_line_level_flag,         l.item_to_price_flag,         l.last_update_date_time,         l.last_update_login_name,         l.last_updated_by_name,         l.line_list_price_amt_ent,         l.negot_price_amt_ent,         l.negot_renew_price_amt_ent,         l.orig_sys_id,         l.orig_sys_ref_id,         l.next_day_response_flag,         l.orig_sys_source_code,         l.parent_contract_line_id,         l.price_basis_flag,         l.price_hold_adjust_est_pct,         l.price_hold_amt_ent,         l.price_hold_break_code,         l.price_hold_enfrc_list_flag,         l.price_hold_intgrtd_flag,         l.price_hold_list_ref_id,         l.price_hold_price_type_code,         l.price_hold_prchs_min_qty,         l.price_hold_prchs_min_amt_ent,         l.price_level_flag,         l.price_list_header_id,         l.price_list_line_id,         l.pricing_date_time,         l.renew_contract_line_id,         l.renew_curr_code,         l.renew_date_time,         l.renew_to_contract_line_id,         l.sot_order_sot_line_nbr,         l.start_date_time,         l.status_code,         l.termination_code,         l.termination_reason_code,         l.top_model_contract_line_id,         t.tran_code,         l.unit_price_amt_ent,         l.unit_price_pct,         l.upg_orig_sys_ref_id,         l.upg_orig_sys_ref_name,         l.termination_date_time,         l.template_used_name,         l.warranty_exp_reason_code,         l.bill_to_site_use_id ,         l.cust_acct_id ,         l.invoice_rule_id,         l.line_renewal_type_code,         l.ship_to_site_use_id,         l.holiday_coverage_code,         l.orig_serviced_qty,         l.serviced_qty,         t.reverse_logistics_text,         t.routing_comment_text,         t.source_location_name,         t.legacy_d1_severity_code,         t.crb_invoice_nbr_text,         t.crb_reference_nbr_text FROM ${EEDDWWDDBB1}.contract_line_ld l,      ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1 t WHERE  l.contract_line_id = t.contract_line_id AND    l.instance_id = t.instance_id AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <         CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t2           SELECT distinct
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
            ${EEDDWWDDBB1}.contract_line_ld    AS l   ,
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t1    AS t   
        WHERE
            l.contract_line_id = t.contract_line_id  
            AND l.instance_id = t.instance_id   
            AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:12 
--Script Name: d8_contract_line_07.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${EEDDWWDDBB1}.contract_line_ld  WHERE (contract_line_id, instance_id) IN (SELECT contract_line_id, instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t2     )  

--Translated Query: STATUS SUCCESS


--Corrected Query:
INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.contract_line_ld
  SELECT  distinct Q1.* 
 FROM   ${EEDDWWDDBB1}.contract_line_ld Q1 
        LEFT JOIN (SELECT contract_line_id, instance_id FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t2) As Q2  
               ON ( Q1.contract_line_id = Q2.contract_line_id  And Q1.instance_id=Q2.instance_id) 
 WHERE  Q2.contract_line_id Is Null 
   And Q2.instance_id IS NULL;

--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t2 FOR ACCESS  INSERT INTO ${EEDDWWDDBB1}.contract_line_ld (  contract_line_id,   instance_id,    alt_contract_header_id,     alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  config_complete_flag,   config_header_id,   config_item_id,     config_item_type_code,  config_revision_nbr,    config_valid_flag,  contract_header_id,     contract_line_nbr,  contract_line_style_id,     contract_price_type_code,   created_by_name,    creation_date_time,     curr_code,  display_sequence_nbr,   end_date_time,  exception_flag,     hidden_line_flag,   invoice_line_level_flag,    item_to_price_flag,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   line_list_price_amt_ent,    negot_price_amt_ent,    negot_renew_price_amt_ent,  orig_sys_id,    orig_sys_ref_id,    next_day_response_flag,     orig_sys_source_code,   parent_contract_line_id,    price_basis_flag,   price_hold_adjust_est_pct,  price_hold_amt_ent,     price_hold_break_code,  price_hold_enfrc_list_flag,     price_hold_intgrtd_flag,    price_hold_list_ref_id,     price_hold_price_type_code,     price_hold_prchs_min_qty,   price_hold_prchs_min_amt_ent,   price_level_flag,   price_list_header_id,   price_list_line_id,     pricing_date_time,  renew_contract_line_id,     renew_curr_code,    renew_date_time,    renew_to_contract_line_id,  sot_order_sot_line_nbr,     start_date_time,    status_code,        termination_code,   termination_reason_code,    top_model_contract_line_id,     tran_code,  unit_price_amt_ent,     unit_price_pct,     upg_orig_sys_ref_id,    upg_orig_sys_ref_name,  termination_date_time,  template_used_name,     warranty_exp_reason_code,         bill_to_site_use_id,         cust_acct_id,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,   orig_serviced_qty,  serviced_qty,   reverse_logistics_text, <WM_COMMENT_START>Added for HSR ENTITLEMENT project<WM_COMMENT_END>     routing_comment_text,   source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text ) SELECT    contract_line_id,   instance_id,    alt_contract_header_id,     alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  config_complete_flag,   config_header_id,   config_item_id,     config_item_type_code,  config_revision_nbr,    config_valid_flag,  contract_header_id,     contract_line_nbr,  contract_line_style_id,     contract_price_type_code,   created_by_name,    creation_date_time,     curr_code,  display_sequence_nbr,   end_date_time,  exception_flag,     hidden_line_flag,   invoice_line_level_flag,    item_to_price_flag,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   line_list_price_amt_ent,    negot_price_amt_ent,    negot_renew_price_amt_ent,  orig_sys_id,    orig_sys_ref_id,    next_day_response_flag,     orig_sys_source_code,   parent_contract_line_id,    price_basis_flag,   price_hold_adjust_est_pct,  price_hold_amt_ent,     price_hold_break_code,  price_hold_enfrc_list_flag,     price_hold_intgrtd_flag,    price_hold_list_ref_id,     price_hold_price_type_code,     price_hold_prchs_min_qty,   price_hold_prchs_min_amt_ent,   price_level_flag,   price_list_header_id,   price_list_line_id,     pricing_date_time,  renew_contract_line_id,     renew_curr_code,    renew_date_time,    renew_to_contract_line_id,  sot_order_sot_line_nbr,     start_date_time,    status_code,        termination_code,   termination_reason_code,    top_model_contract_line_id,     tran_code,  unit_price_amt_ent,     unit_price_pct,     upg_orig_sys_ref_id,    upg_orig_sys_ref_name,  termination_date_time,  template_used_name,     warranty_exp_reason_code,         bill_to_site_use_id ,         cust_acct_id ,         invoice_rule_id,         line_renewal_type_code,         ship_to_site_use_id,         holiday_coverage_code,     orig_serviced_qty,  serviced_qty,   reverse_logistics_text,     routing_comment_text,   source_location_name,         legacy_d1_severity_code,         crb_invoice_nbr_text,         crb_reference_nbr_text FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t2  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_line_ld           SELECT  
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_t2;

--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_line_ld COLUMN (INSTANCE_ID,PARENT_CONTRACT_LINE_ID) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_line_ld COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,PARENT_CONTRACT_LINE_ID;

--Original Query:
  --COLLECT STATISTICS COLUMN (contract_line_id, instance_id)                  , COLUMN (contract_line_id)                  , COLUMN (instance_id)                  ON ${EEDDWWDDBB}.contract_line_ld 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_line_ld COMPUTE STATISTICS  FOR COLUMNS contract_line_id,instance_id  , contract_line_id  , instance_id;
