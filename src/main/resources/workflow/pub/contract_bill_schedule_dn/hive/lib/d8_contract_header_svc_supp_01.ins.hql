----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:07 
--Script Name: d8_contract_header_svc_supp_01.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1;

--(Nikhil: Replaced the Inner join with Row_number function)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 ( 	contract_header_supp_id, 	instance_id, 	acctg_rule_id, 	ar_interface_flag, 	as_of_date_time, 	batch_nbr, 	bill_sched_lvl_type_code, 	contract_header_id, 	created_by_name, 	creation_date_time, 	cust_trx_type_id, 	est_revn_date, 	est_revn_pct, 	est_revn_period_used_code, 	est_revn_used_duration, 	est_revn_used_pct, 	grace_duration, 	grace_period_code, 	hold_bill_flag, 	invc_print_profile_flag, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	payment_type_code, 	renewal_po_nbr, 	renewal_po_required_flag, 	renewal_po_used_flag, 	rnw_est_revenue_duration, 	rnw_est_revenue_pct, 	rnw_est_revn_period_code, 	rnw_grace_period_used_code, 	rnw_grace_used_duration, 	rnw_markup_pct, 	rnw_markup_used_pct, 	rnw_notification_to_id, 	rnw_notification_to_name, 	rnw_price_list_id, 	rnw_price_list_used_id, 	rnw_pricing_type_code, 	rnw_pricing_used_type_code, 	rnw_used_type_code, 	service_po_nbr, 	service_po_required_flag, 	summary_trx_flag, 	tax_exempt_status_code, 	tax_exemption_id, 	tran_code	 	,renewal_approval_type_used 	,evn_thershhold_amt 	,ern_thershhold_amt ) SELECT  	contract_header_supp_id, 	instance_id, 	acctg_rule_id, 	ar_interface_flag, 	as_of_date_time, 	batch_nbr, 	bill_sched_lvl_type_code, 	contract_header_id, 	created_by_name, 	creation_date_time, 	cust_trx_type_id, 	est_revn_date, 	est_revn_pct, 	est_revn_period_used_code, 	est_revn_used_duration, 	est_revn_used_pct, 	grace_duration, 	grace_period_code, 	hold_bill_flag, 	invc_print_profile_flag, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	payment_type_code, 	renewal_po_nbr, 	renewal_po_required_flag, 	renewal_po_used_flag, 	rnw_est_revenue_duration, 	rnw_est_revenue_pct, 	rnw_est_revn_period_code, 	rnw_grace_period_used_code, 	rnw_grace_used_duration, 	rnw_markup_pct, 	rnw_markup_used_pct, 	rnw_notification_to_id, 	rnw_notification_to_name, 	rnw_price_list_id, 	rnw_price_list_used_id, 	rnw_pricing_type_code, 	rnw_pricing_used_type_code, 	rnw_used_type_code, 	service_po_nbr, 	service_po_required_flag, 	summary_trx_flag, 	tax_exempt_status_code, 	tax_exemption_id, 	tran_code	 	,renewal_approval_type_used 	,evn_thershhold_amt 	,ern_thershhold_amt FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml WHERE (CAST(as_of_date_time AS CHAR(26))||batch_nbr,  	contract_header_supp_id,  	instance_id ) IN (SELECT  MAX(CAST(as_of_date_time AS CHAR(26))||batch_nbr),  	contract_header_supp_id,  	instance_id     FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml     WHERE tran_code <> 'D'     GROUP BY 2,3 ) AND tran_code <> 'D'  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1           SELECT
            -- contract_header_supp_id,
            -- instance_id,
            -- acctg_rule_id,
            -- ar_interface_flag,
            -- as_of_date_time,
            -- batch_nbr,
            -- bill_sched_lvl_type_code,
            -- contract_header_id,
            -- created_by_name,
            -- creation_date_time,
            -- cust_trx_type_id,
            -- est_revn_date,
            -- est_revn_pct,
            -- est_revn_period_used_code,
            -- est_revn_used_duration,
            -- est_revn_used_pct,
            -- grace_duration,
            -- grace_period_code,
            -- hold_bill_flag,
            -- invc_print_profile_flag,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- payment_type_code,
            -- renewal_po_nbr,
            -- renewal_po_required_flag,
            -- renewal_po_used_flag,
            -- rnw_est_revenue_duration,
            -- rnw_est_revenue_pct,
            -- rnw_est_revn_period_code,
            -- rnw_grace_period_used_code,
            -- rnw_grace_used_duration,
            -- rnw_markup_pct,
            -- rnw_markup_used_pct,
            -- rnw_notification_to_id,
            -- rnw_notification_to_name,
            -- rnw_price_list_id,
            -- rnw_price_list_used_id,
            -- rnw_pricing_type_code,
            -- rnw_pricing_used_type_code,
            -- rnw_used_type_code,
            -- service_po_nbr,
            -- service_po_required_flag,
            -- summary_trx_flag,
            -- tax_exempt_status_code,
            -- tax_exemption_id,
            -- tran_code,
            -- renewal_approval_type_used,
            -- evn_thershhold_amt,
            -- ern_thershhold_amt  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- contract_header_supp_id AS auto_c01,
                    -- instance_id AS auto_c02  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml     
                -- WHERE
                    -- upper(trim(tran_code)) <> upper(trim('D'))  
                -- GROUP BY
                    -- contract_header_supp_id ,
                    -- instance_id 
            -- ) AS autoAlias_95 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_95.AUTO_C00 
                -- AND upper(trim(contract_header_supp_id)) = upper(trim(autoAlias_95.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_95.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) <> upper(trim('D'));

--Translated Query(Corrected Manually)
       
   with qq1 as (
SELECT 
            contract_header_supp_id,
            instance_id,
            acctg_rule_id,
            ar_interface_flag,
            as_of_date_time,
            batch_nbr,
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
            ern_thershhold_amt,
            ROW_NUMBER() OVER( PARTITION BY contract_header_supp_id,instance_id ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
            batch_nbr) DESC)AS RNK
            FROM 
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml 
            WHERE upper(trim(tran_code)) <> upper(trim('D')) 
                ) 
INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 
SELECT
            EDW2.contract_header_supp_id,
    EDW2.instance_id,
    EDW2.acctg_rule_id,
    EDW2.ar_interface_flag,
    EDW2.as_of_date_time,
    EDW2.batch_nbr,
    EDW2.bill_sched_lvl_type_code,
    EDW2.contract_header_id,
    EDW2.created_by_name,
    EDW2.creation_date_time,
    EDW2.cust_trx_type_id,
    EDW2.est_revn_date,
    EDW2.est_revn_pct,
    EDW2.est_revn_period_used_code,
    EDW2.est_revn_used_duration,
    EDW2.est_revn_used_pct,
    EDW2.grace_duration,
    EDW2.grace_period_code,
    EDW2.hold_bill_flag,
    EDW2.invc_print_profile_flag,
    EDW2.last_update_date_time,
    EDW2.last_update_login_name,
    EDW2.last_updated_by_name,
    EDW2.payment_type_code,
    EDW2.renewal_po_nbr,
    EDW2.renewal_po_required_flag,
    EDW2.renewal_po_used_flag,
    EDW2.rnw_est_revenue_duration,
    EDW2.rnw_est_revenue_pct,
    EDW2.rnw_est_revn_period_code,
    EDW2.rnw_grace_period_used_code,
    EDW2.rnw_grace_used_duration,
    EDW2.rnw_markup_pct,
    EDW2.rnw_markup_used_pct,
    EDW2.rnw_notification_to_id,
    EDW2.rnw_notification_to_name,
    EDW2.rnw_price_list_id,
    EDW2.rnw_price_list_used_id,
    EDW2.rnw_pricing_type_code,
    EDW2.rnw_pricing_used_type_code,
    EDW2.rnw_used_type_code,
    EDW2.service_po_nbr,
    EDW2.service_po_required_flag,
    EDW2.summary_trx_flag,
    EDW2.tax_exempt_status_code,
    EDW2.tax_exemption_id,
    EDW2.tran_code,
    EDW2.renewal_approval_type_used,
    EDW2.evn_thershhold_amt,
    EDW2.ern_thershhold_amt 
FROM   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml EDW2 
  INNER JOIN (
    SELECT 
       contract_header_supp_id,
    instance_id,
    acctg_rule_id,
    ar_interface_flag,
    as_of_date_time,
    batch_nbr,
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
    ern_thershhold_amt, 
            RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND upper(trim(EDW2.contract_header_supp_id)) =upper(trim(IBVL.contract_header_supp_id)) 
  AND upper(trim(EDW2.tran_code)) <> upper(trim('D'));
    
