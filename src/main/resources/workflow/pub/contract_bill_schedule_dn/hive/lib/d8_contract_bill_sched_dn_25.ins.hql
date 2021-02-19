----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:47 
--Script Name: d8_contract_bill_sched_dn_25.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${TTMMPPDDBB}  

--Translated Query: STATUS SUCCESS

    use ${TTMMPPDDBB} ;

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t3 FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t24 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 (   contract_header_id  ,instance_id  ,author_oper_unit_country_code  ,bill_to_site_use_id  ,contract_end_date  ,contract_est_amt_ent  ,contract_nbr  ,contract_nbr_modifier  ,contract_start_date  ,curr_code  ,cust_po_nbr  ,invoice_rule_id  ,migration_contract_nbr  ,renew_date  ,renewal_type_code  ,solution_portfolio_id  ,status_code  ,tran_code  ,renewal_type_name  ,invoice_rule_desc  ,invoice_rule_name  ,cust_trx_type_id  ,hdr_tax_exempt_sts_code  ,hdr_tax_exemption_id  ,hdr_trx_type_name  ,hold_billing_flag  ,party_id  ,country_code  ,bill_to_addr_line_1  ,bill_to_addr_line_2  ,bill_to_addr_line_3  ,bill_to_addr_line_4  ,bill_to_city_name  ,bill_to_collctn_org_code  ,bill_to_country_code  ,bill_to_county_name  ,bill_to_cs_fml_org_code  ,bill_to_cust_indstry_code  ,bill_to_cust_indstry_name  ,bill_to_cust_name  ,bill_to_cust_nbr  ,bill_to_cust_site_name  ,bill_to_cust_site_nbr  ,bill_to_cust_type_code  ,bill_to_fml_org_code  ,bill_to_oper_unit_country_code  ,bill_to_party_id  ,bill_to_party_site_addr_id  ,bill_to_postal_code  ,bill_to_province_name  ,bill_to_state_name  ,bill_to_store_nbr  ,bill_to_svc_loc_code  ,bill_to_svc_territory_id  ,contract_group_name  ,area_code  ,area_desc  ,country_desc  ,region_code  ,region_desc  ,bill_to_cs_fml_org_name  ,bill_to_fml_org_name  ,hdr_cust_exempt_nbr  ,hdr_exempt_reason_code  ,customer_name  ,customer_nbr  ,service_line_id  ,service_line_nbr  ,service_line_style_id  ,service_line_style_name  ,tax_exempt_status_code  ,tax_exemption_id  ,vat_tax_id  ,customer_exemption_nbr  ,exemption_reason_code  ,exemption_tax_code  ,vat_tax_code  ,inventory_item_id  ,inventory_org_id  ,line_product_id  ,line_product_id_formatted  ,serviced_qty  )  SELECT   EDW2.contract_header_id ,EDW2.instance_id ,EDW1.author_oper_unit_country_code ,EDW1.bill_to_site_use_id ,EDW1.contract_end_date ,EDW1.contract_est_amt_ent ,EDW1.contract_nbr ,EDW1.contract_nbr_modifier ,EDW1.contract_start_date ,EDW1.curr_code ,EDW1.cust_po_nbr ,EDW1.invoice_rule_id ,EDW1.migration_contract_nbr ,EDW1.renew_date ,EDW1.renewal_type_code ,EDW1.solution_portfolio_id ,EDW1.status_code ,EDW1.tran_code ,EDW1.renewal_type_name ,EDW1.invoice_rule_desc ,EDW1.invoice_rule_name ,EDW1.cust_trx_type_id ,EDW1.hdr_tax_exempt_sts_code ,EDW1.hdr_tax_exemption_id ,EDW1.hdr_trx_type_name ,EDW1.hold_billing_flag ,EDW1.party_id ,EDW1.country_code ,EDW1.bill_to_addr_line_1 ,EDW1.bill_to_addr_line_2 ,EDW1.bill_to_addr_line_3 ,EDW1.bill_to_addr_line_4 ,EDW1.bill_to_city_name ,EDW1.bill_to_collctn_org_code ,EDW1.bill_to_country_code ,EDW1.bill_to_county_name ,EDW1.bill_to_cs_fml_org_code ,EDW1.bill_to_cust_indstry_code ,EDW1.bill_to_cust_indstry_name ,EDW1.bill_to_cust_name ,EDW1.bill_to_cust_nbr ,EDW1.bill_to_cust_site_name ,EDW1.bill_to_cust_site_nbr ,EDW1.bill_to_cust_type_code ,EDW1.bill_to_fml_org_code ,EDW1.bill_to_oper_unit_country_code ,EDW1.bill_to_party_id ,EDW1.bill_to_party_site_addr_id ,EDW1.bill_to_postal_code ,EDW1.bill_to_province_name ,EDW1.bill_to_state_name ,EDW1.bill_to_store_nbr ,EDW1.bill_to_svc_loc_code ,EDW1.bill_to_svc_territory_id ,EDW1.contract_group_name ,EDW1.area_code ,EDW1.area_desc ,EDW1.country_desc ,EDW1.region_code ,EDW1.region_desc ,EDW1.bill_to_cs_fml_org_name ,EDW1.bill_to_fml_org_name ,EDW1.hdr_cust_exempt_nbr ,EDW1.hdr_exempt_reason_code  ,EDW1.customer_name ,EDW1.customer_nbr ,EDW2.service_line_id ,EDW2.service_line_nbr ,EDW2.service_line_style_id ,EDW2.service_line_style_name ,EDW2.tax_exempt_status_code ,EDW2.tax_exemption_id ,EDW2.vat_tax_id ,EDW2.customer_exemption_nbr ,EDW2.exemption_reason_code ,EDW2.exemption_tax_code ,EDW2.vat_tax_code ,EDW2.inventory_item_id ,EDW2.inventory_org_id ,EDW2.line_product_id  ,EDW2.line_product_id_formatted ,EDW2.serviced_qty  FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t24 EDW2 LEFT OUTER JOIN      ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t3  EDW1 ON   EDW2.instance_id                           = EDW1.instance_id AND  EDW2.contract_header_id                    = EDW1.contract_header_id  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25           SELECT
            EDW2.contract_header_id,
            EDW2.instance_id,
            EDW2.service_line_id,
            EDW1.author_oper_unit_country_code,
            EDW1.bill_to_site_use_id,
            EDW1.contract_end_date,
            EDW1.contract_est_amt_ent,
            EDW1.contract_nbr,
            EDW1.contract_nbr_modifier,
            EDW1.contract_start_date,
            EDW1.curr_code,
            EDW1.cust_po_nbr,
            EDW1.invoice_rule_id,
            EDW1.migration_contract_nbr,
            EDW1.renew_date,
            EDW1.renewal_type_code,
            EDW1.solution_portfolio_id,
            EDW1.status_code,
            EDW1.tran_code,
            EDW1.renewal_type_name,
            EDW1.invoice_rule_desc,
            EDW1.invoice_rule_name,
            EDW1.cust_trx_type_id,
            EDW1.hdr_tax_exempt_sts_code,
            EDW1.hdr_tax_exemption_id,
            EDW1.hdr_trx_type_name,
            EDW1.hold_billing_flag,
            EDW1.party_id,
            EDW1.country_code,
            EDW1.bill_to_addr_line_1,
            EDW1.bill_to_addr_line_2,
            EDW1.bill_to_addr_line_3,
            EDW1.bill_to_addr_line_4,
            EDW1.bill_to_city_name,
            EDW1.bill_to_collctn_org_code,
            EDW1.bill_to_country_code,
            EDW1.bill_to_county_name,
            EDW1.bill_to_cs_fml_org_code,
            EDW1.bill_to_cust_indstry_code,
            EDW1.bill_to_cust_indstry_name,
            EDW1.bill_to_cust_name,
            EDW1.bill_to_cust_nbr,
            EDW1.bill_to_cust_site_name,
            EDW1.bill_to_cust_site_nbr,
            EDW1.bill_to_cust_type_code,
            EDW1.bill_to_fml_org_code,
            EDW1.bill_to_oper_unit_country_code,
            EDW1.bill_to_party_id,
            EDW1.bill_to_party_site_addr_id,
            EDW1.bill_to_postal_code,
            EDW1.bill_to_province_name,
            EDW1.bill_to_state_name,
            EDW1.bill_to_store_nbr,
            EDW1.bill_to_svc_loc_code,
            EDW1.bill_to_svc_territory_id,
            EDW1.area_code,
            EDW1.area_desc,
            EDW1.country_desc,
            EDW1.region_code,
            EDW1.region_desc,
            EDW1.bill_to_cs_fml_org_name,
            EDW1.bill_to_fml_org_name,
            EDW1.hdr_cust_exempt_nbr,
            EDW1.hdr_exempt_reason_code,
            EDW1.customer_name,
            EDW1.customer_nbr,
            EDW1.contract_group_name,
            EDW2.service_line_nbr,
            EDW2.service_line_style_id,
            EDW2.service_line_style_name,
            EDW2.tax_exempt_status_code,
            EDW2.tax_exemption_id,
            EDW2.vat_tax_id,
            EDW2.customer_exemption_nbr,
            EDW2.exemption_reason_code,
            EDW2.exemption_tax_code,
            EDW2.vat_tax_code,
            EDW2.inventory_item_id,
            EDW2.inventory_org_id,
            EDW2.line_product_id,
            EDW2.line_product_id_formatted,
            EDW2.serviced_qty  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t24    AS EDW2   
        LEFT OUTER  JOIN
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t3    AS EDW1    
                ON upper(trim(EDW2.instance_id)) = upper(trim(EDW1.instance_id))  
                AND upper(trim(EDW2.contract_header_id)) = upper(trim(EDW1.contract_header_id));
