----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:19 
--Script Name: d8_contract_line_svc_supp_04.upd.sql
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

    --(Nikhil: Corrected the case statement used in translated query)
--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contract_line_t3 FOR ACCESS  UPDATE	${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2 SET	current_record_ind = 'D' WHERE	( 	 contract_line_supp_id 	,instance_id) 	IN 	(SELECT contract_line_supp_id 		,instance_id 	FROM ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t3 	GROUP BY contract_line_supp_id 		,instance_id 	HAVING COUNT(*) = 1 	)  

--Translated Query: STATUS SUCCESS

    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2 SELECT
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.contract_line_supp_id AS contract_line_supp_id ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.acctg_rule_id AS acctg_rule_id ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.allow_before_tax_disc_flag AS allow_before_tax_disc_flag ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.alt_as_of_date_time AS alt_as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.alt_batch_nbr AS alt_batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.alt_contract_header_id AS alt_contract_header_id ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.as_of_date_time AS as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.batch_nbr AS batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.bill_sched_lvl_type_code AS bill_sched_lvl_type_code ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.contract_line_id AS contract_line_id ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cover_lvl_ext_amt_ent AS cover_lvl_ext_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cover_lvl_list_prc_amt_ent AS cover_lvl_list_prc_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cover_lvl_qty AS cover_lvl_qty ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cover_lvl_uom_code AS cover_lvl_uom_code ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.coverage_type_code AS coverage_type_code ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.created_by_name AS created_by_name ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.creation_date_time AS creation_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.credit_amt_ent AS credit_amt_ent ,
            CASE 
                --WHEN autoAlias_171.auto_c00 IS not null THEN 'D' 
				WHEN autoAlias_171.auto_c00 IS not null and autoAlias_171.auto_c01 is not null THEN 'D' 
                ELSE AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.current_record_ind 
            END AS current_record_ind ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cust_po_nbr AS cust_po_nbr ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cust_po_nbr_rqrd_flag AS cust_po_nbr_rqrd_flag ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.full_credit_flag AS full_credit_flag ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.incident_severity_id AS incident_severity_id ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.invoice_print_flag AS invoice_print_flag ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.last_update_date_time AS last_update_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.last_update_login_name AS last_update_login_name ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.last_updated_by_name AS last_updated_by_name ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.offset_duration AS offset_duration ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.offset_period_code AS offset_period_code ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.override_amt_ent AS override_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.pricing_uom_code AS pricing_uom_code ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.process_defn_id AS process_defn_id ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.product_price_amt_ent AS product_price_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.product_upgrade_flag AS product_upgrade_flag ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.react_active_flag AS react_active_flag ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.service_price_amt_ent AS service_price_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.suppressed_credit_amt_ent AS suppressed_credit_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.sync_date_installed_flag AS sync_date_installed_flag ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.tax_amt_ent AS tax_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.tax_exempt_status_code AS tax_exempt_status_code ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.tax_exemption_id AS tax_exemption_id ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.vat_tax_id AS vat_tax_id ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.tax_inclusive_flag AS tax_inclusive_flag ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_adj_prc_amt_ent AS top_lvl_adj_prc_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_operator_code AS top_lvl_operator_code ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_oprnd_prc_amt_ent AS top_lvl_oprnd_prc_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_pricing_uom_code AS top_lvl_pricing_uom_code ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_pricing_uom_qty AS top_lvl_pricing_uom_qty ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_qty AS top_lvl_qty ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.tran_code AS tran_code ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.transfer_option_code AS transfer_option_code ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.unbilled_amt_ent AS unbilled_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.work_thru_flag AS work_thru_flag 
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2 AS AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2 
        Left Outer Join
            (
                SELECT
                    contract_line_supp_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t3      
                GROUP BY
                    contract_line_supp_id,
                    instance_id 
                HAVING
                    COUNT(*) = 1
            )autoAlias_171 
                ON (
                    trim(upper(autoAlias_171.auto_c00)) = trim(upper(AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.contract_line_supp_id))
                    AND trim(upper(autoAlias_171.auto_c01)) = trim(upper(AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.instance_id))
                );
