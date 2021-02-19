----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:42 
--Script Name: d8_contract_bill_sched_dn_03.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t3 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t3;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t2 FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.geo_rollup_xref  FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.ncr_organization  FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.tax_exemption FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.party FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.customer_account FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t3 (  contract_header_id ,instance_id ,author_oper_unit_country_code ,bill_to_site_use_id ,contract_end_date ,contract_est_amt_ent ,contract_nbr ,contract_nbr_modifier ,contract_start_date ,curr_code ,cust_po_nbr ,invoice_rule_id ,migration_contract_nbr ,renew_date ,renewal_type_code ,solution_portfolio_id ,status_code ,tran_code ,renewal_type_name ,invoice_rule_desc ,invoice_rule_name ,cust_trx_type_id                    ,hdr_tax_exempt_sts_code ,hdr_tax_exemption_id ,hdr_trx_type_name ,hold_billing_flag ,party_id ,country_code ,bill_to_addr_line_1 ,bill_to_addr_line_2 ,bill_to_addr_line_3 ,bill_to_addr_line_4 ,bill_to_city_name ,bill_to_collctn_org_code ,bill_to_country_code ,bill_to_county_name ,bill_to_cs_fml_org_code ,bill_to_cust_indstry_code ,bill_to_cust_indstry_name ,bill_to_cust_name ,bill_to_cust_nbr ,bill_to_cust_site_name ,bill_to_cust_site_nbr ,bill_to_cust_type_code ,bill_to_fml_org_code ,bill_to_oper_unit_country_code ,bill_to_party_id ,bill_to_party_site_addr_id ,bill_to_postal_code ,bill_to_province_name ,bill_to_state_name ,bill_to_store_nbr ,bill_to_svc_loc_code ,bill_to_svc_territory_id ,contract_group_name  ,area_code                ,area_desc                ,country_desc             ,region_code              ,region_desc              ,bill_to_cs_fml_org_name  ,bill_to_fml_org_name     ,hdr_cust_exempt_nbr      ,hdr_exempt_reason_code   ,customer_name   ,customer_nbr     ) SELECT  EDW1.contract_header_id ,EDW1.instance_id ,EDW1.author_oper_unit_country_code ,EDW1.bill_to_site_use_id ,EDW1.contract_end_date ,EDW1.contract_est_amt_ent ,EDW1.contract_nbr ,EDW1.contract_nbr_modifier ,EDW1.contract_start_date ,EDW1.curr_code ,EDW1.cust_po_nbr	 ,EDW1.invoice_rule_id ,EDW1.migration_contract_nbr ,EDW1.renew_date ,EDW1.renewal_type_code ,EDW1.solution_portfolio_id ,EDW1.status_code ,EDW1.tran_code ,EDW1.renewal_type_name ,EDW1.invoice_rule_desc ,EDW1.invoice_rule_name ,EDW1.cust_trx_type_id ,EDW1.hdr_tax_exempt_sts_code ,EDW1.hdr_tax_exemption_id ,EDW1.hdr_trx_type_name ,EDW1.hold_billing_flag ,EDW1.party_id ,EDW1.country_code ,EDW1.bill_to_addr_line_1 ,EDW1.bill_to_addr_line_2 ,EDW1.bill_to_addr_line_3 ,EDW1.bill_to_addr_line_4 ,EDW1.bill_to_city_name ,EDW1.bill_to_collctn_org_code ,EDW1.bill_to_country_code ,EDW1.bill_to_county_name ,EDW1.bill_to_cs_fml_org_code ,EDW1.bill_to_cust_indstry_code ,EDW1.bill_to_cust_indstry_name ,EDW1.bill_to_cust_name ,EDW1.bill_to_cust_nbr ,EDW1.bill_to_cust_site_name ,EDW1.bill_to_cust_site_nbr ,EDW1.bill_to_cust_type_code ,EDW1.bill_to_fml_org_code ,EDW1.bill_to_oper_unit_country_code ,EDW1.bill_to_party_id ,EDW1.bill_to_party_site_addr_id ,EDW1.bill_to_postal_code ,EDW1.bill_to_province_name ,EDW1.bill_to_state_name ,EDW1.bill_to_store_nbr ,EDW1.bill_to_svc_loc_code ,EDW1.bill_to_svc_territory_id ,EDW1.contract_group_name  <WM_COMMENT_START> area_code <WM_COMMENT_END>    ,COALESCE(EDW2.area_code, ' ')                <WM_COMMENT_START> area_desc <WM_COMMENT_END> ,COALESCE(EDW2.area_desc, ' ')                <WM_COMMENT_START> country_desc <WM_COMMENT_END> ,COALESCE(EDW2.country_desc, ' ')            <WM_COMMENT_START> region_code <WM_COMMENT_END> ,COALESCE(EDW2.region_code, ' ')             <WM_COMMENT_START> region_desc <WM_COMMENT_END> ,COALESCE(EDW2.region_desc, ' ')             <WM_COMMENT_START> bill_to_cs_fml_org_name <WM_COMMENT_END> ,COALESCE(EDW3.organization_name, ' ') <WM_COMMENT_START> bill_to_fml_org_name <WM_COMMENT_END> ,COALESCE(EDW4.organization_name, ' ') <WM_COMMENT_START> hdr_cust_exempt_nbr <WM_COMMENT_END>      ,COALESCE(EDW5.customer_exemption_nbr, ' ') <WM_COMMENT_START> hdr_exempt_reason_code <WM_COMMENT_END> ,COALESCE(EDW5.reason_code, ' ')  <WM_COMMENT_START> customer_name <WM_COMMENT_END> ,COALESCE(EDW6.party_name, ' ') <WM_COMMENT_START> customer_nbr <WM_COMMENT_END> ,COALESCE(EDW7.customer_nbr, ' ') FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t2 EDW1 LEFT OUTER JOIN      ${EEDDWWDDBB2}.geo_rollup_xref EDW2 ON   EDW1.country_code = EDW2.country_code           AND  EDW2.business_unit_code = 'B4'                    LEFT  OUTER JOIN                                                 ${EEDDWWDDBB2}.ncr_organization EDW3                          ON   EDW1.bill_to_cs_fml_org_code = EDW3.fml_organization_code  LEFT  OUTER JOIN                                                 ${EEDDWWDDBB2}.ncr_organization EDW4                          ON   EDW1.bill_to_fml_org_code = EDW4.fml_organization_code  LEFT OUTER JOIN ${EEDDWWDDBB1}.tax_exemption EDW5                                 ON   EDW1.instance_id = EDW5.instance_id                            AND  EDW1.hdr_tax_exemption_id = EDW5.tax_exemption_id               LEFT OUTER JOIN                                       ${EEDDWWDDBB2}.party EDW6                          ON   EDW1.party_id = EDW6.party_id                  AND  EDW1.instance_id = EDW6.instance_id             LEFT OUTER JOIN                                       ${EEDDWWDDBB2}.customer_account EDW7                 ON   EDW1.party_id = EDW7.party_id                    AND  EDW1.instance_id = EDW7.instance_id               

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t3           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
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
            COALESCE( EDW2.area_code ,
            ' ' ) AS auto_c055,
            COALESCE( EDW2.area_desc ,
            ' ' ) AS auto_c056,
            COALESCE( EDW2.country_desc ,
            ' ' ) AS auto_c057,
            COALESCE( EDW2.region_code ,
            ' ' ) AS auto_c058,
            COALESCE( EDW2.region_desc ,
            ' ' ) AS auto_c059,
            COALESCE( EDW3.organization_name ,
            ' ' ) AS auto_c060,
            COALESCE( EDW4.organization_name ,
            ' ' ) AS auto_c061,
            COALESCE( EDW5.customer_exemption_nbr ,
            ' ' ) AS auto_c062,
            COALESCE( EDW5.reason_code ,
            ' ' ) AS auto_c063,
            COALESCE( EDW6.party_name ,
            ' ' ) AS auto_c064,
            COALESCE( EDW7.customer_nbr ,
            ' ' ) AS auto_c065,
            EDW1.contract_group_name  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t2    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.geo_rollup_xref    AS EDW2    
                ON EDW1.country_code = EDW2.country_code  
                AND upper(trim(EDW2.business_unit_code)) = upper(trim('B4'))    
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.ncr_organization    AS EDW3    
                ON upper(trim(EDW1.bill_to_cs_fml_org_code)) = upper(trim(EDW3.fml_organization_code))   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.ncr_organization    AS EDW4    
                ON upper(trim(EDW1.bill_to_fml_org_code)) = upper(trim(EDW4.fml_organization_code))   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB1}.tax_exemption    AS EDW5    
                ON upper(trim(EDW1.instance_id)) = upper(trim(EDW5.instance_id))  
                AND EDW1.hdr_tax_exemption_id = EDW5.tax_exemption_id
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.party    AS EDW6    
                ON EDW1.party_id = EDW6.party_id
                AND upper(trim(EDW1.instance_id)) = upper(trim(EDW6.instance_id))    
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.customer_account    AS EDW7    
                ON EDW1.party_id = EDW7.party_id
                AND upper(trim(EDW1.instance_id)) = upper(trim(EDW7.instance_id));
