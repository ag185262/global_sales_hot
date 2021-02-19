----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:38:51 
--Script Name: d8_contract_header_svc_supp_03.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query: 7e58aa2f355917bd712f410b9b1585c8
  --DATABASE ${TTMMPPDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${TTMMPPDDBB1} ;

--Original Query: 1a1614fc3967d13bd77ed2c98ea08ff2
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t3 ( 	contract_header_supp_id, 	instance_id, 	acctg_rule_id, 	ar_interface_flag, 	bill_sched_lvl_type_code, 	contract_header_id, 	created_by_name, 	creation_date_time, 	cust_trx_type_id, 	est_revn_date, 	est_revn_pct, 	est_revn_period_used_code, 	est_revn_used_duration, 	est_revn_used_pct, 	grace_duration, 	grace_period_code, 	hold_bill_flag, 	invc_print_profile_flag, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	payment_type_code, 	renewal_po_nbr, 	renewal_po_required_flag, 	renewal_po_used_flag, 	rnw_est_revenue_duration, 	rnw_est_revenue_pct, 	rnw_est_revn_period_code, 	rnw_grace_period_used_code, 	rnw_grace_used_duration, 	rnw_markup_pct, 	rnw_markup_used_pct, 	rnw_notification_to_id, 	rnw_notification_to_name, 	rnw_price_list_id, 	rnw_price_list_used_id, 	rnw_pricing_type_code, 	rnw_pricing_used_type_code, 	rnw_used_type_code, 	service_po_nbr, 	service_po_required_flag, 	summary_trx_flag, 	tax_exempt_status_code, 	tax_exemption_id, 	tran_code		 	,renewal_approval_type_used 	,evn_thershhold_amt 	,ern_thershhold_amt ) SELECT  	contract_header_supp_id, 	instance_id, 	acctg_rule_id, 	ar_interface_flag, 	bill_sched_lvl_type_code, 	contract_header_id, 	created_by_name, 	creation_date_time, 	cust_trx_type_id, 	est_revn_date, 	est_revn_pct, 	est_revn_period_used_code, 	est_revn_used_duration, 	est_revn_used_pct, 	grace_duration, 	grace_period_code, 	hold_bill_flag, 	invc_print_profile_flag, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	payment_type_code, 	renewal_po_nbr, 	renewal_po_required_flag, 	renewal_po_used_flag, 	rnw_est_revenue_duration, 	rnw_est_revenue_pct, 	rnw_est_revn_period_code, 	rnw_grace_period_used_code, 	rnw_grace_used_duration, 	rnw_markup_pct, 	rnw_markup_used_pct, 	rnw_notification_to_id, 	rnw_notification_to_name, 	rnw_price_list_id, 	rnw_price_list_used_id, 	rnw_pricing_type_code, 	rnw_pricing_used_type_code, 	rnw_used_type_code, 	service_po_nbr, 	service_po_required_flag, 	summary_trx_flag, 	tax_exempt_status_code, 	tax_exemption_id, 	tran_code		 	,renewal_approval_type_used 	,evn_thershhold_amt 	,ern_thershhold_amt FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 WHERE (contract_header_supp_id,instance_id) IN (SELECT  	contract_header_supp_id, 	instance_id 	FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2    )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t3           SELECT
            contract_header_supp_id,
            instance_id,
            acctg_rule_id,
            ar_interface_flag,
            bill_sched_lvl_type_code,
            contract_header_id,
            created_by_name,
            creation_date_time,
            cust_trx_type_id,
            est_revn_date,
            est_revn_pct,
            est_revn_period_used_code,
            est_revn_used_duration,
            est_revn_used_pct,
            grace_duration,
            grace_period_code,
            hold_bill_flag,
            invc_print_profile_flag,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            payment_type_code,
            renewal_po_nbr,
            renewal_po_required_flag,
            renewal_po_used_flag,
            rnw_est_revenue_duration,
            rnw_est_revenue_pct,
            rnw_est_revn_period_code,
            rnw_grace_period_used_code,
            rnw_grace_used_duration,
            rnw_markup_pct,
            rnw_markup_used_pct,
            rnw_notification_to_id,
            rnw_notification_to_name,
            rnw_price_list_id,
            rnw_price_list_used_id,
            rnw_pricing_type_code,
            rnw_pricing_used_type_code,
            rnw_used_type_code,
            service_po_nbr,
            service_po_required_flag,
            summary_trx_flag,
            tax_exempt_status_code,
            tax_exemption_id,
            tran_code,
            renewal_approval_type_used,
            evn_thershhold_amt,
            ern_thershhold_amt  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_header_supp_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2 
            ) AS autoAlias_387 
                ON (
                    upper(trim(contract_header_supp_id)) = upper(trim(autoAlias_387.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_387.AUTO_C01)) 
                );
