----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:08 
--Script Name: d8_contract_header_svc_supp_04.upd.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${TTMMPPDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${TTMMPPDDBB1} ;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t3 FOR ACCESS  UPDATE ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2 SET current_record_ind = 'D' WHERE (contract_header_supp_id, instance_id) IN 	(SELECT contract_header_supp_id, instance_id 	FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t3 	GROUP BY contract_header_supp_id,instance_id 	HAVING COUNT(*) = 1 	)  

--Translated Query: STATUS SUCCESS(Slightly modified, added 1 more not null check to CASE statement)

    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2 SELECT
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.contract_header_supp_id AS contract_header_supp_id ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.acctg_rule_id AS acctg_rule_id ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.alt_as_of_date_time AS alt_as_of_date_time ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.alt_batch_nbr AS alt_batch_nbr ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.ar_interface_flag AS ar_interface_flag ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.as_of_date_time AS as_of_date_time ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.batch_nbr AS batch_nbr ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.bill_sched_lvl_type_code AS bill_sched_lvl_type_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.contract_header_id AS contract_header_id ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.created_by_name AS created_by_name ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.creation_date_time AS creation_date_time ,
            CASE 
                WHEN autoAlias_101.auto_c00 IS not null and autoAlias_101.auto_c01 IS not null THEN 'D' 
                ELSE AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.current_record_ind 
            END AS current_record_ind ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.cust_trx_type_id AS cust_trx_type_id ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.est_revn_date AS est_revn_date ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.est_revn_pct AS est_revn_pct ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.est_revn_period_used_code AS est_revn_period_used_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.est_revn_used_duration AS est_revn_used_duration ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.est_revn_used_pct AS est_revn_used_pct ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.grace_duration AS grace_duration ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.grace_period_code AS grace_period_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.hold_bill_flag AS hold_bill_flag ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.invc_print_profile_flag AS invc_print_profile_flag ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.last_update_date_time AS last_update_date_time ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.last_update_login_name AS last_update_login_name ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.last_updated_by_name AS last_updated_by_name ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.payment_type_code AS payment_type_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.renewal_po_nbr AS renewal_po_nbr ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.renewal_po_required_flag AS renewal_po_required_flag ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.renewal_po_used_flag AS renewal_po_used_flag ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_est_revenue_duration AS rnw_est_revenue_duration ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_est_revenue_pct AS rnw_est_revenue_pct ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_est_revn_period_code AS rnw_est_revn_period_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_grace_period_used_code AS rnw_grace_period_used_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_grace_used_duration AS rnw_grace_used_duration ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_markup_pct AS rnw_markup_pct ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_markup_used_pct AS rnw_markup_used_pct ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_notification_to_id AS rnw_notification_to_id ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_notification_to_name AS rnw_notification_to_name ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_price_list_id AS rnw_price_list_id ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_price_list_used_id AS rnw_price_list_used_id ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_pricing_type_code AS rnw_pricing_type_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_pricing_used_type_code AS rnw_pricing_used_type_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_used_type_code AS rnw_used_type_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.service_po_nbr AS service_po_nbr ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.service_po_required_flag AS service_po_required_flag ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.summary_trx_flag AS summary_trx_flag ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.tax_exempt_status_code AS tax_exempt_status_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.tax_exemption_id AS tax_exemption_id ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.tran_code AS tran_code ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.renewal_approval_type_used AS renewal_approval_type_used ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.evn_thershhold_amt AS evn_thershhold_amt ,
            AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.ern_thershhold_amt AS ern_thershhold_amt 
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2 AS AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2 
        Left Outer Join
            (
                SELECT
                    contract_header_supp_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t3      
                GROUP BY
                    contract_header_supp_id,
                    instance_id 
                HAVING
                    COUNT(*) = 1
            )autoAlias_101 
                ON (
                    upper(trim( autoAlias_101.auto_c00)) = upper(trim( AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.contract_header_supp_id)) 
                    AND upper(trim( autoAlias_101.auto_c01)) = upper(trim( AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.instance_id))
                );
