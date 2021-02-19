----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:42 
--Script Name: d8_contract_bill_sched_dn_02.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t2 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t2;

--(Mohit: Manually translated, Comments:Removed the use of WITH Block as it was nowhere being used, removed additional WHERE clause with window funt.[ROW_NUMBER])
--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1 FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.renewal_type FOR ACCESS  LOCK TABLE ${EEDDWWDDBB2}.invoice_rule FOR ACCESS  LOCK TABLE ${EEDDWWDDBB2}.contract_header_svc_supp_ld FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.contract_party_role_ld FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.cust_acct_site_use_dn FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.contract_group_xref_ld FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.contract_group_lk_ld FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t2   (  contract_header_id ,instance_id ,author_oper_unit_country_code ,bill_to_site_use_id ,contract_end_date ,contract_est_amt_ent ,contract_nbr ,contract_nbr_modifier ,contract_start_date ,curr_code ,cust_po_nbr ,invoice_rule_id ,migration_contract_nbr ,renew_date ,renewal_type_code ,solution_portfolio_id ,status_code ,tran_code ,renewal_type_name ,invoice_rule_desc ,invoice_rule_name ,cust_trx_type_id                    ,hdr_tax_exempt_sts_code ,hdr_tax_exemption_id ,hdr_trx_type_name ,hold_billing_flag ,party_id ,country_code ,bill_to_addr_line_1 ,bill_to_addr_line_2 ,bill_to_addr_line_3 ,bill_to_addr_line_4 ,bill_to_city_name ,bill_to_collctn_org_code ,bill_to_country_code ,bill_to_county_name ,bill_to_cs_fml_org_code ,bill_to_cust_indstry_code ,bill_to_cust_indstry_name ,bill_to_cust_name ,bill_to_cust_nbr ,bill_to_cust_site_name ,bill_to_cust_site_nbr ,bill_to_cust_type_code ,bill_to_fml_org_code ,bill_to_oper_unit_country_code ,bill_to_party_id ,bill_to_party_site_addr_id ,bill_to_postal_code ,bill_to_province_name ,bill_to_state_name ,bill_to_store_nbr ,bill_to_svc_loc_code ,bill_to_svc_territory_id ,contract_group_name ) SELECT  EDW1.contract_header_id ,EDW1.instance_id ,EDW1.author_oper_unit_country_code ,EDW1.bill_to_site_use_id ,EDW1.contract_end_date ,EDW1.contract_est_amt_ent ,EDW1.contract_nbr ,EDW1.contract_nbr_modifier ,EDW1.contract_start_date ,EDW1.curr_code ,EDW1.cust_po_nbr  ,EDW1.invoice_rule_id ,EDW1.migration_contract_nbr ,EDW1.renew_date ,EDW1.renewal_type_code ,EDW1.solution_portfolio_id ,EDW1.status_code ,EDW1.tran_code <WM_COMMENT_START> renewal_type_name <WM_COMMENT_END> ,COALESCE(EDW2.renewal_type_name, ' ') <WM_COMMENT_START> invoice_rule_desc <WM_COMMENT_END> ,COALESCE(EDW3.invoice_rule_desc, ' ') <WM_COMMENT_START> invoice_rule_name <WM_COMMENT_END> ,COALESCE(EDW3.invoice_rule_name, ' ') <WM_COMMENT_START> cust_trx_type_id <WM_COMMENT_END> ,COALESCE(EDW4.cust_trx_type_id, ' ') <WM_COMMENT_START> hdr_trx_exempt_sts_code <WM_COMMENT_END>                   ,COALESCE(EDW4.tax_exempt_status_code, ' ') <WM_COMMENT_START> hdr_trx_exemption_id <WM_COMMENT_END>                   ,COALESCE(EDW4.tax_exemption_id, 0) <WM_COMMENT_START> hdr_trx_type_name <WM_COMMENT_END>                   ,CASE WHEN EDW4.cust_trx_type_id = '1699'       THEN 'MO-Invoice-OKS'                          WHEN EDW4.cust_trx_type_id = '1701'            THEN 'BN-Invoice-OKS'                          WHEN EDW4.cust_trx_type_id = '1824'            THEN 'LU-Invoice-OKS'                          ELSE ' '                                  END                                      <WM_COMMENT_START> hold_billing_flag <WM_COMMENT_END>  ,COALESCE(EDW4.hold_bill_flag, ' ') <WM_COMMENT_START> party_id <WM_COMMENT_END>                          ,COALESCE(CAST(EDW5.object1_id1 AS DECIMAL(18,0)), 0) <WM_COMMENT_START> country_code <WM_COMMENT_END>                          ,COALESCE(EDW6.oper_unit_country_code, ' ')             <WM_COMMENT_START> bill_to_addr_line_1 <WM_COMMENT_END>            ,COALESCE(EDW6.address_line_1, ' ')                <WM_COMMENT_START> bill_to_addr_line_2 <WM_COMMENT_END>            ,COALESCE(EDW6.address_line_2, ' ')                <WM_COMMENT_START> bill_to_addr_line_3 <WM_COMMENT_END>            ,COALESCE(EDW6.address_line_3, ' ')                <WM_COMMENT_START> bill_to_addr_line_4 <WM_COMMENT_END>            ,COALESCE(EDW6.address_line_4, ' ')                <WM_COMMENT_START> bill_to_city_name <WM_COMMENT_END>              ,COALESCE(EDW6.city_name, ' ')                     <WM_COMMENT_START> bill_to_collctn_org_code <WM_COMMENT_END>       ,COALESCE(EDW6.contract_collctn_org_code, ' ')     <WM_COMMENT_START> bill_to_country_code <WM_COMMENT_END>           ,COALESCE(EDW6.country_code, ' ')                  <WM_COMMENT_START> bill_to_county_name <WM_COMMENT_END>            ,COALESCE(EDW6.county_name, ' ')                   <WM_COMMENT_START> bill_to_cs_fml_org_code <WM_COMMENT_END>        ,COALESCE(EDW6.cs_fml_organization_code, ' ')      <WM_COMMENT_START> bill_to_cust_indstry_code <WM_COMMENT_END>      ,COALESCE(EDW6.cust_industry_code, ' ')            <WM_COMMENT_START> bill_to_cust_indstry_name <WM_COMMENT_END>      ,COALESCE(EDW6.cust_industry_name, ' ')            <WM_COMMENT_START> bill_to_cust_name <WM_COMMENT_END>               ,COALESCE(EDW6.customer_name, ' ')                  <WM_COMMENT_START> bill_to_cust_nbr <WM_COMMENT_END> ,COALESCE(EDW6.customer_nbr, ' ')                <WM_COMMENT_START> bill_to_cust_site_name <WM_COMMENT_END> ,COALESCE(EDW6.customer_site_name, ' ')            <WM_COMMENT_START> bill_to_cust_site_nbr <WM_COMMENT_END>  ,COALESCE(EDW6.customer_site_nbr, ' ')         <WM_COMMENT_START> bill_to_cust_type_code <WM_COMMENT_END> ,COALESCE(EDW6.customer_type_code, ' ')          <WM_COMMENT_START> bill_to_fml_org_code <WM_COMMENT_END> ,COALESCE(EDW6.fml_organization_code, ' ')    <WM_COMMENT_START> bill_to_oper_unit_country_code <WM_COMMENT_END> ,COALESCE(EDW6.oper_unit_country_code, ' ')             <WM_COMMENT_START> bill_to_party_id <WM_COMMENT_END> ,COALESCE(EDW6.party_id, 0)                          <WM_COMMENT_START> bill_to_party_site_addr_id <WM_COMMENT_END> ,COALESCE(EDW6.party_site_addr_id, 0)                 <WM_COMMENT_START> bill_to_postal_code <WM_COMMENT_END>  ,COALESCE(EDW6.postal_code, ' ')                       <WM_COMMENT_START> bill_to_province_name <WM_COMMENT_END> ,COALESCE(EDW6.province_name, ' ')                      <WM_COMMENT_START> bill_to_state_name <WM_COMMENT_END> ,COALESCE(EDW6.state_name, ' ')                       <WM_COMMENT_START> bill_to_store_nbr <WM_COMMENT_END> ,COALESCE(EDW6.store_nbr, ' ')                       <WM_COMMENT_START> bill_to_svc_loc_code <WM_COMMENT_END> ,COALESCE(EDW6.service_loc_code, ' ')                  <WM_COMMENT_START> bill_to_svc_territory_id <WM_COMMENT_END> ,COALESCE(EDW6.service_territory_id, 0)             <WM_COMMENT_START>contract_group_name<WM_COMMENT_END> ,COALESCE(EDW8.contract_group_name, ' ') FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1 EDW1 LEFT OUTER JOIN      ${EEDDWWDDBB2}.renewal_type EDW2 ON  EDW1.renewal_type_code = EDW2.renewal_type_code AND EDW1.instance_id = EDW2.instance_id  LEFT OUTER JOIN ${EEDDWWDDBB2}.invoice_rule EDW3 ON  EDW1.invoice_rule_id = EDW3.invoice_rule_id AND EDW1.instance_id = EDW3.instance_id  LEFT OUTER JOIN ${EEDDWWDDBB2}.contract_header_svc_supp_ld EDW4 ON  EDW1.contract_header_id = EDW4.contract_header_id AND EDW1.instance_id = EDW4.instance_id AND EDW4.tran_code <> 'D'  LEFT OUTER JOIN                                             ${EEDDWWDDBB2}.contract_party_role_ld EDW5                ON  EDW1.contract_header_id = EDW5.alt_contract_header_id AND EDW1.instance_id = EDW5.instance_id                   AND EDW5.role_code = 'CUSTOMER'                            AND EDW5.contract_line_id = ' '                            AND EDW5.tran_code <> 'D'                                   LEFT OUTER JOIN                                               ${EEDDWWDDBB2}.cust_acct_site_use_dn EDW6                 ON  EDW1.bill_to_site_use_id = EDW6.cust_acct_site_use_id AND EDW1.instance_id = EDW6.instance_id                   AND EDW6.cust_acct_site_use_code = 'BILL_TO'               LEFT OUTER JOIN                                               ${EEDDWWDDBB2}.contract_group_xref_ld EDW7                 ON  EDW1.contract_header_id = EDW7.included_contract_header_id AND EDW1.instance_id = EDW7.instance_id                   and EDW7.tran_code <> 'D' AND (EDW7.included_contract_header_id,      EDW7.instance_id,      EDW7.contract_group_xref_id) IN (SELECT       included_contract_header_id,      instance_id,      MIN(contract_group_xref_id)      FROM ${EEDDWWDDBB2}.contract_group_xref_ld      GROUP BY 1,2) LEFT OUTER JOIN ${EEDDWWDDBB2}.contract_group_lk_ld EDW8 ON EDW7.parent_contract_group_id = EDW8.contract_group_id and EDW7.instance_id = EDW8.instance_id and EDW8.tran_code <> 'D'  

--Translated Query: STATUS SUCCESS

    -- WITH EDW7 AS (SELECT
        -- EDW7.* 
    -- FROM
        -- TTMP.D8T_contr_bill_sched_dn_t1 AS EDW1 
    -- LEFT OUTER  JOIN
        -- ${EEDDWWDDBB2}.contract_group_xref_ld AS EDW7 
            -- ON (1=1) 
    -- WHERE
        -- (EDW7.included_contract_header_id,EDW7.instance_id,EDW7.contract_group_xref_id) IN(  SELECT
            -- included_contract_header_id AS auto_c00,
            -- instance_id AS auto_c01,
            -- MIN( contract_group_xref_id ) AS auto_c02  
        -- FROM
            -- ${EEDDWWDDBB2}.contract_group_xref_ld      
        -- GROUP BY
            -- included_contract_header_id ,
            -- instance_id    ) 
        -- AND  (EDW7.included_contract_header_id,EDW7.instance_id,EDW7.contract_group_xref_id) IN(  SELECT
            -- included_contract_header_id AS auto_c00,
            -- instance_id AS auto_c01,
            -- MIN( contract_group_xref_id ) AS auto_c02  
        -- FROM
            -- ${EEDDWWDDBB2}.contract_group_xref_ld      
        -- GROUP BY
            -- included_contract_header_id ,
            -- instance_id    ) 
        -- OR (EDW7.included_contract_header_id,EDW7.instance_id,EDW7.contract_group_xref_id) IS NULL  )
        -- INSERT 
        -- INTO
            -- TABLE
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t2           SELECT
                -- EDW1.contract_header_id,
                -- EDW1.instance_id,
                -- EDW1.author_oper_unit_country_code,
                -- EDW1.bill_to_site_use_id,
                -- EDW1.contract_end_date,
                -- EDW1.contract_est_amt_ent,
                -- EDW1.contract_nbr,
                -- EDW1.contract_nbr_modifier,
                -- EDW1.contract_start_date,
                -- EDW1.curr_code,
                -- EDW1.cust_po_nbr,
                -- EDW1.invoice_rule_id,
                -- EDW1.migration_contract_nbr,
                -- EDW1.renew_date,
                -- EDW1.renewal_type_code,
                -- EDW1.solution_portfolio_id,
                -- EDW1.status_code,
                -- EDW1.tran_code,
                -- COALESCE( EDW2.renewal_type_name ,
                -- ' ' ) AS auto_c018,
                -- COALESCE( EDW3.invoice_rule_desc ,
                -- ' ' ) AS auto_c019,
                -- COALESCE( EDW3.invoice_rule_name ,
                -- ' ' ) AS auto_c020,
                -- COALESCE( EDW4.cust_trx_type_id ,
                -- ' ' ) AS auto_c021,
                -- COALESCE( EDW4.tax_exempt_status_code ,
                -- ' ' ) AS auto_c022,
                -- COALESCE( EDW4.tax_exemption_id ,
                -- 0 ) AS auto_c023,
                -- CASE 
                    -- WHEN upper(trim(EDW4.cust_trx_type_id)) = upper(trim('1699'))  THEN 'MO-Invoice-OKS'  
                    -- WHEN upper(trim(EDW4.cust_trx_type_id)) = upper(trim('1701'))  THEN 'BN-Invoice-OKS' 
                    -- WHEN upper(trim(EDW4.cust_trx_type_id)) = upper(trim('1824'))  THEN 'LU-Invoice-OKS' 
                    -- ELSE ' '  
                -- END AS auto_c024,
                -- COALESCE( EDW4.hold_bill_flag ,
                -- ' ' ) AS auto_c025,
                -- COALESCE( CAST( EDW5.object1_id1 AS decimal(18,0) ) ,
                -- 0 ) AS auto_c026,
                -- COALESCE( EDW6.oper_unit_country_code ,
                -- ' ' ) AS auto_c027,
                -- COALESCE( EDW6.address_line_1 ,
                -- ' ' ) AS auto_c028,
                -- COALESCE( EDW6.address_line_2 ,
                -- ' ' ) AS auto_c029,
                -- COALESCE( EDW6.address_line_3 ,
                -- ' ' ) AS auto_c030,
                -- COALESCE( EDW6.address_line_4 ,
                -- ' ' ) AS auto_c031,
                -- COALESCE( EDW6.city_name ,
                -- ' ' ) AS auto_c032,
                -- COALESCE( EDW6.contract_collctn_org_code ,
                -- ' ' ) AS auto_c033,
                -- COALESCE( EDW6.country_code ,
                -- ' ' ) AS auto_c034,
                -- COALESCE( EDW6.county_name ,
                -- ' ' ) AS auto_c035,
                -- COALESCE( EDW6.cs_fml_organization_code ,
                -- ' ' ) AS auto_c036,
                -- COALESCE( EDW6.cust_industry_code ,
                -- ' ' ) AS auto_c037,
                -- COALESCE( EDW6.cust_industry_name ,
                -- ' ' ) AS auto_c038,
                -- COALESCE( EDW6.customer_name ,
                -- ' ' ) AS auto_c039,
                -- COALESCE( EDW6.customer_nbr ,
                -- ' ' ) AS auto_c040,
                -- COALESCE( EDW6.customer_site_name ,
                -- ' ' ) AS auto_c041,
                -- COALESCE( EDW6.customer_site_nbr ,
                -- ' ' ) AS auto_c042,
                -- COALESCE( EDW6.customer_type_code ,
                -- ' ' ) AS auto_c043,
                -- COALESCE( EDW6.fml_organization_code ,
                -- ' ' ) AS auto_c044,
                -- COALESCE( EDW6.oper_unit_country_code ,
                -- ' ' ) AS auto_c045,
                -- COALESCE( EDW6.party_id ,
                -- 0 ) AS auto_c046,
                -- COALESCE( EDW6.party_site_addr_id ,
                -- 0 ) AS auto_c047,
                -- COALESCE( EDW6.postal_code ,
                -- ' ' ) AS auto_c048,
                -- COALESCE( EDW6.province_name ,
                -- ' ' ) AS auto_c049,
                -- COALESCE( EDW6.state_name ,
                -- ' ' ) AS auto_c050,
                -- COALESCE( EDW6.store_nbr ,
                -- ' ' ) AS auto_c051,
                -- COALESCE( EDW6.service_loc_code ,
                -- ' ' ) AS auto_c052,
                -- COALESCE( EDW6.service_territory_id ,
                -- 0 ) AS auto_c053,
                -- COALESCE( EDW8.contract_group_name ,
                -- ' ' ) AS auto_c054  
            -- FROM
                -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1    AS EDW1   
            -- LEFT OUTER  JOIN
                -- ${EEDDWWDDBB2}.renewal_type    AS EDW2    
                    -- ON EDW1.renewal_type_code = EDW2.renewal_type_code  
                    -- AND EDW1.instance_id = EDW2.instance_id    
            -- LEFT OUTER  JOIN
                -- ${EEDDWWDDBB2}.invoice_rule    AS EDW3    
                    -- ON EDW1.invoice_rule_id = EDW3.invoice_rule_id  
                    -- AND EDW1.instance_id = EDW3.instance_id    
            -- LEFT OUTER  JOIN
                -- ${EEDDWWDDBB2}.contract_header_svc_supp_ld    AS EDW4    
                    -- ON EDW1.contract_header_id = EDW4.contract_header_id  
                    -- AND EDW1.instance_id = EDW4.instance_id   
                    -- AND upper(trim(EDW4.tran_code)) <> upper(trim('D'))    
            -- LEFT OUTER  JOIN
                -- ${EEDDWWDDBB2}.contract_party_role_ld    AS EDW5    
                    -- ON EDW1.contract_header_id = EDW5.alt_contract_header_id  
                    -- AND EDW1.instance_id = EDW5.instance_id   
                    -- AND upper(trim(EDW5.role_code)) = upper(trim('CUSTOMER'))   
                    -- AND upper(trim(EDW5.contract_line_id)) = upper(trim(' '))   
                    -- AND upper(trim(EDW5.tran_code)) <> upper(trim('D'))    
            -- LEFT OUTER  JOIN
                -- ${EEDDWWDDBB2}.cust_acct_site_use_dn    AS EDW6    
                    -- ON EDW1.bill_to_site_use_id = EDW6.cust_acct_site_use_id  
                    -- AND EDW1.instance_id = EDW6.instance_id   
                    -- AND upper(trim(EDW6.cust_acct_site_use_code)) = upper(trim('BILL_TO'))    
            -- LEFT OUTER  JOIN
                -- ${EEDDWWDDBB2}.contract_group_xref_ld    AS EDW7    
                    -- ON EDW1.contract_header_id = EDW7.included_contract_header_id  
                    -- AND EDW1.instance_id = EDW7.instance_id   
                    -- AND upper(trim(EDW7.tran_code)) <> upper(trim('D'))    
            -- LEFT OUTER  JOIN
                -- ${EEDDWWDDBB2}.contract_group_lk_ld    AS EDW8    
                    -- ON EDW7.parent_contract_group_id = EDW8.contract_group_id  
                    -- AND upper(trim(EDW7.instance_id)) = upper(trim(EDW8.instance_id))   
                    -- AND upper(trim(EDW8.tran_code)) <> upper(trim('D'));


--Corrected Query: 


INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t2 (
  contract_header_id,
  instance_id, 
  author_oper_unit_country_code, 
  bill_to_site_use_id,
  contract_end_date, 
  contract_est_amt_ent,
  contract_nbr, 
  contract_nbr_modifier,
  contract_start_date, 
  curr_code,
  cust_po_nbr,
  invoice_rule_id, 
  migration_contract_nbr,
  renew_date, 
  renewal_type_code,
  solution_portfolio_id, 
  status_code,
  tran_code,
  renewal_type_name, 
  invoice_rule_desc,
  invoice_rule_name, 
  cust_trx_type_id,
  hdr_tax_exempt_sts_code, 
  hdr_tax_exemption_id,
  hdr_trx_type_name, 
  hold_billing_flag,
  party_id, country_code, 
  bill_to_addr_line_1,
  bill_to_addr_line_2, 
  bill_to_addr_line_3, 
  bill_to_addr_line_4, 
  bill_to_city_name, 
  bill_to_collctn_org_code, 
  bill_to_country_code, 
  bill_to_county_name, 
  bill_to_cs_fml_org_code,
  bill_to_cust_indstry_code, 
  bill_to_cust_indstry_name, 
  bill_to_cust_name, 
  bill_to_cust_nbr, 
  bill_to_cust_site_name, 
  bill_to_cust_site_nbr,
  bill_to_cust_type_code, 
  bill_to_fml_org_code, 
  bill_to_oper_unit_country_code, 
  bill_to_party_id, 
  bill_to_party_site_addr_id, 
  bill_to_postal_code, 
  bill_to_province_name, 
  bill_to_state_name, 
  bill_to_store_nbr, 
  bill_to_svc_loc_code, 
  bill_to_svc_territory_id, 
  contract_group_name
) 
SELECT 
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
  COALESCE(EDW2.renewal_type_name, ' '), 
  COALESCE(EDW3.invoice_rule_desc, ' '), 
  COALESCE(EDW3.invoice_rule_name, ' '), 
  COALESCE(EDW4.cust_trx_type_id, ' '), 
  COALESCE(EDW4.tax_exempt_status_code, ' '), 
  COALESCE(EDW4.tax_exemption_id, 0) , 
  CASE WHEN EDW4.cust_trx_type_id = '1699' THEN 'MO-Invoice-OKS' WHEN EDW4.cust_trx_type_id = '1701' THEN 'BN-Invoice-OKS' WHEN EDW4.cust_trx_type_id = '1824' THEN 'LU-Invoice-OKS' ELSE ' ' END , 
  COALESCE(EDW4.hold_bill_flag, ' ') , 
  COALESCE(CAST(EDW5.object1_id1 AS DECIMAL(18, 0)), 0) , 
  COALESCE(EDW6.oper_unit_country_code, ' ') , 
  COALESCE(EDW6.address_line_1, ' ') , 
  COALESCE(EDW6.address_line_2, ' ') , 
  COALESCE(EDW6.address_line_3, ' ') , 
  COALESCE(EDW6.address_line_4, ' ') , 
  COALESCE(EDW6.city_name, ' ') , 
  COALESCE(EDW6.contract_collctn_org_code, ' '), 
  COALESCE(EDW6.country_code, ' '), 
  COALESCE(EDW6.county_name, ' '), 
  COALESCE(EDW6.cs_fml_organization_code, ' ') , 
  COALESCE(EDW6.cust_industry_code, ' ') , 
  COALESCE(EDW6.cust_industry_name, ' ') , 
  COALESCE(EDW6.customer_name, ' ') , 
  COALESCE(EDW6.customer_nbr, ' ') , 
  COALESCE(EDW6.customer_site_name, ' ') , 
  COALESCE(EDW6.customer_site_nbr, ' ') , 
  COALESCE(EDW6.customer_type_code, ' ') , 
  COALESCE(EDW6.fml_organization_code, ' ') , 
  COALESCE(EDW6.oper_unit_country_code, ' ') , 
  COALESCE(EDW6.party_id, 0) , 
  COALESCE(EDW6.party_site_addr_id, 0) , 
  COALESCE(EDW6.postal_code, ' ') , 
  COALESCE(EDW6.province_name, ' ') , 
  COALESCE(EDW6.state_name, ' '), 
  COALESCE(EDW6.store_nbr, ' ') , 
  COALESCE(EDW6.service_loc_code, ' ') , 
  COALESCE(EDW6.service_territory_id, 0) , 
  COALESCE(EDW8.contract_group_name, ' ') 
FROM 
  ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1 EDW1 
  LEFT OUTER JOIN ${EEDDWWDDBB2}.renewal_type EDW2 ON upper(trim(EDW1.renewal_type_code)) = upper(trim(EDW2.renewal_type_code)) AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))
  LEFT OUTER JOIN ${EEDDWWDDBB2}.invoice_rule EDW3 ON EDW1.invoice_rule_id = EDW3.invoice_rule_id AND upper(trim(EDW1.instance_id)) = upper(trim(EDW3.instance_id))
  LEFT OUTER JOIN ${EEDDWWDDBB2}.contract_header_svc_supp_ld EDW4 ON upper(trim(EDW1.contract_header_id)) = upper(trim(EDW4.contract_header_id)) AND upper(trim(EDW1.instance_id)) = upper(trim(EDW4.instance_id)) AND upper(trim(EDW4.tran_code)) <> 'D' 
  LEFT OUTER JOIN ${EEDDWWDDBB2}.contract_party_role_ld EDW5 ON upper(trim(EDW1.contract_header_id)) = upper(trim(EDW5.alt_contract_header_id)) AND upper(trim(EDW1.instance_id)) = upper(trim(EDW5.instance_id)) AND upper(trim(EDW5.role_code)) = 'CUSTOMER' AND EDW5.contract_line_id = ' ' AND upper(trim(EDW5.tran_code)) <> 'D' 
  LEFT OUTER JOIN ${EEDDWWDDBB2}.cust_acct_site_use_dn EDW6 ON EDW1.bill_to_site_use_id = EDW6.cust_acct_site_use_id AND upper(trim(EDW1.instance_id)) = upper(trim(EDW6.instance_id)) AND upper(trim(EDW6.cust_acct_site_use_code)) = 'BILL_TO' 
  LEFT OUTER JOIN 
  (SELECT upper(trim(included_contract_header_id)) as included_contract_header_id, parent_contract_group_id, upper(trim(instance_id)) as instance_id, contract_group_xref_id, ROW_NUMBER() OVER (PARTITION BY upper(trim(included_contract_header_id)), instance_id ORDER BY contract_group_xref_id ASC) AS ROW_NUM FROM ${EEDDWWDDBB2}.contract_group_xref_ld WHERE upper(trim(tran_code)) <> 'D') EDW7 
  ON 
  upper(trim(EDW1.contract_header_id)) = EDW7.included_contract_header_id AND upper(trim(EDW1.instance_id)) = EDW7.instance_id AND EDW7.ROW_NUM =1
  LEFT OUTER JOIN ${EEDDWWDDBB2}.contract_group_lk_ld EDW8 ON EDW7.parent_contract_group_id = EDW8.contract_group_id and upper(trim(EDW7.instance_id))= upper(trim(EDW8.instance_id)) and upper(trim(EDW8.tran_code)) <> 'D';

