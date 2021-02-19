----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:19 
--Script Name: d8_contract_line_svc_supp_03.ins.sql
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
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1 FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t3 ( 	 contract_line_supp_id 	,instance_id 	,acctg_rule_id 	,allow_before_tax_disc_flag 	,alt_contract_header_id 	,bill_sched_lvl_type_code 	,contract_line_id 	,cover_lvl_ext_amt_ent 	,cover_lvl_list_prc_amt_ent 	,cover_lvl_qty 	,cover_lvl_uom_code 	,coverage_type_code 	,created_by_name 	,creation_date_time 	,credit_amt_ent 	,cust_po_nbr 	,cust_po_nbr_rqrd_flag 	,full_credit_flag 	,incident_severity_id 	,invoice_print_flag 	,last_update_date_time 	,last_update_login_name 	,last_updated_by_name 	,offset_duration 	,offset_period_code 	,override_amt_ent 	,pricing_uom_code 	,process_defn_id 	,product_price_amt_ent 	,product_upgrade_flag 	,react_active_flag 	,service_price_amt_ent 	,suppressed_credit_amt_ent 	,sync_date_installed_flag 	,tax_amt_ent 	,tax_exempt_status_code 	,tax_exemption_id 	,vat_tax_id 	,tax_inclusive_flag 	,top_lvl_adj_prc_amt_ent 	,top_lvl_operator_code 	,top_lvl_oprnd_prc_amt_ent 	,top_lvl_pricing_uom_code 	,top_lvl_pricing_uom_qty 	,top_lvl_qty 	,tran_code 	,transfer_option_code 	,unbilled_amt_ent 	,work_thru_flag ) SELECT 	 contract_line_supp_id 	,instance_id 	,acctg_rule_id 	,allow_before_tax_disc_flag 	,alt_contract_header_id 	,bill_sched_lvl_type_code 	,contract_line_id 	,cover_lvl_ext_amt_ent 	,cover_lvl_list_prc_amt_ent 	,cover_lvl_qty 	,cover_lvl_uom_code 	,coverage_type_code 	,created_by_name 	,creation_date_time 	,credit_amt_ent 	,cust_po_nbr 	,cust_po_nbr_rqrd_flag 	,full_credit_flag 	,incident_severity_id 	,invoice_print_flag 	,last_update_date_time 	,last_update_login_name 	,last_updated_by_name 	,offset_duration 	,offset_period_code 	,override_amt_ent 	,pricing_uom_code 	,process_defn_id 	,product_price_amt_ent 	,product_upgrade_flag 	,react_active_flag 	,service_price_amt_ent 	,suppressed_credit_amt_ent 	,sync_date_installed_flag 	,tax_amt_ent 	,tax_exempt_status_code 	,tax_exemption_id 	,vat_tax_id 	,tax_inclusive_flag 	,top_lvl_adj_prc_amt_ent 	,top_lvl_operator_code 	,top_lvl_oprnd_prc_amt_ent 	,top_lvl_pricing_uom_code 	,top_lvl_pricing_uom_qty 	,top_lvl_qty 	,tran_code 	,transfer_option_code 	,unbilled_amt_ent 	,work_thru_flag FROM	${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1 WHERE	(contract_line_supp_id 	,instance_id) 	IN 	(SELECT  contract_line_supp_id 		,instance_id 	FROM ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2 	)  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t3           SELECT
            contract_line_supp_id,
            instance_id,
            acctg_rule_id,
            allow_before_tax_disc_flag,
            alt_contract_header_id,
            bill_sched_lvl_type_code,
            contract_line_id,
            cover_lvl_ext_amt_ent,
            cover_lvl_list_prc_amt_ent,
            cover_lvl_qty,
            cover_lvl_uom_code,
            coverage_type_code,
            created_by_name,
            creation_date_time,
            credit_amt_ent,
            cust_po_nbr,
            cust_po_nbr_rqrd_flag,
            full_credit_flag,
            incident_severity_id,
            invoice_print_flag,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            offset_duration,
            offset_period_code,
            override_amt_ent,
            pricing_uom_code,
            process_defn_id,
            product_price_amt_ent,
            product_upgrade_flag,
            react_active_flag,
            service_price_amt_ent,
            suppressed_credit_amt_ent,
            sync_date_installed_flag,
            tax_amt_ent,
            tax_exempt_status_code,
            tax_exemption_id,
            vat_tax_id,
            tax_inclusive_flag,
            top_lvl_adj_prc_amt_ent,
            top_lvl_operator_code,
            top_lvl_oprnd_prc_amt_ent,
            top_lvl_pricing_uom_code,
            top_lvl_pricing_uom_qty,
            top_lvl_qty,
            tran_code,
            transfer_option_code,
            unbilled_amt_ent,
            work_thru_flag  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_line_supp_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2 
            ) AS autoAlias_169 
                ON (
                    upper(trim(contract_line_supp_id)) = upper(trim(autoAlias_169.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_169.AUTO_C01)) 
                );
