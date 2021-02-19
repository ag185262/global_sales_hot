----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:59 
--Script Name: d8_contract_bill_sched_dn_ld_1.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 COLUMN (contract_header_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 COMPUTE STATISTICS  FOR COLUMNS contract_header_id;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 COLUMN (instance_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 COLUMN (service_line_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 COMPUTE STATISTICS  FOR COLUMNS service_line_id;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56     COLUMN (contract_header_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56 COMPUTE STATISTICS  FOR COLUMNS contract_header_id;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56 COLUMN (instance_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56      COLUMN (service_line_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56 COMPUTE STATISTICS  FOR COLUMNS service_line_id;

--Original Query:
  --DELETE FROM ${EEDDWWDDBB}.contract_bill_sched_dn_ld ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${EEDDWWDDBB}.contract_bill_sched_dn_ld;

--Original Query:
  --LOCK TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t51 FOR ACCESS LOCK TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 FOR ACCESS  INSERT INTO ${EEDDWWDDBB}.contract_bill_sched_dn_ld (   contract_header_id   ,instance_id   ,author_oper_unit_country_code   ,bill_to_site_use_id   ,contract_end_date   ,contract_est_amt_ent   ,contract_nbr   ,contract_nbr_modifier   ,contract_start_date   ,curr_code   ,cust_po_nbr   ,invoice_rule_id   ,migration_contract_nbr   ,renew_date   ,renewal_type_code   ,solution_portfolio_id   ,status_code     ,renewal_type_name   ,invoice_rule_desc   ,invoice_rule_name   ,cust_trx_type_id   ,hdr_tax_exempt_sts_code   ,hdr_tax_exemption_id   ,hdr_trx_type_name   ,hold_billing_flag   ,party_id   ,country_code   ,bill_to_addr_line_1   ,bill_to_addr_line_2   ,bill_to_addr_line_3   ,bill_to_addr_line_4   ,bill_to_city_name   ,bill_to_collctn_org_code   ,bill_to_country_code   ,bill_to_county_name   ,bill_to_cs_fml_org_code   ,bill_to_cust_indstry_code   ,bill_to_cust_indstry_name   ,bill_to_cust_name   ,bill_to_cust_nbr   ,bill_to_cust_site_name   ,bill_to_cust_site_nbr   ,bill_to_cust_type_code   ,bill_to_fml_org_code   ,bill_to_oper_unit_country_code   ,bill_to_party_id   ,bill_to_party_site_addr_id   ,bill_to_postal_code   ,bill_to_province_name   ,bill_to_state_name   ,bill_to_store_nbr   ,bill_to_svc_loc_code   ,bill_to_svc_territory_id   ,contract_group_name   ,area_code   ,area_desc   ,country_desc   ,region_code   ,region_desc   ,bill_to_cs_fml_org_name   ,bill_to_fml_org_name   ,hdr_cust_exempt_nbr   ,hdr_exempt_reason_code   ,customer_name   ,customer_nbr   ,service_line_id   ,service_line_nbr   ,service_line_style_id   ,service_line_style_name   ,tax_exempt_status_code   ,tax_exemption_id   ,vat_tax_id   ,customer_exemption_nbr   ,exemption_reason_code    ,exemption_tax_code   ,vat_tax_code   ,inventory_item_id   ,inventory_org_id   ,line_product_id   ,line_product_id_formatted   ,service_subline_id   ,sline_line_style_id   ,sline_covered_lvl_name   ,sequence_nbr   ,billing_stream_end_date   ,billing_stream_start_date     ,interface_offset_day_nbr   ,period_uom_cnt   ,period_uom_code   ,Sl_discount_amt_ent   ,sl_surcharge_amt_ent   ,sl_top_lvl_adj_prc_amt_ent   ,sl_top_lvl_oprnd_prc_amt_ent   ,sl_top_lvl_pricing_uom_qty   ,sl_unbilled_amt_ent     ,interface_date   ,level_sequence_nbr   ,period_end_date   ,period_start_date   ,bill_on_date   ,invoice_type_code   ,bill_amt_ent   ,completed_date   ,level_element_amt_ent   ,invoice_currency_code   ,invoice_date   ,invoice_nbr   ,line_credit_amt_ent   ,sline_credit_amt_ent   ,install_location_id   ,item_instance_id   ,external_reference_nbr   ,last_valid_invtry_org_id   ,prev_serial_nbr   ,serial_nbr   ,sline_product_id_formatted   ,subline_ind   ,service_subline_end_date   ,service_subline_nbr   ,service_subline_start_date   ,service_subline_term_date   ,service_subline_status_code   ,serviced_qty   ,update_date_time   ,bill_to_cust_sales_chnl_code   ,bill_to_pnt_cust_ind_code   ,bill_to_pnt_cust_ind_name    ,naics_code   ,naics_desc   ,sub_region_name   ,sub_region_code  )  SELECT   EDW2.contract_header_id ,EDW2.instance_id ,COALESCE(EDW1.author_oper_unit_country_code, ' ' ) ,COALESCE(EDW1.bill_to_site_use_id, 0) ,EDW1.contract_end_date ,COALESCE(EDW1.contract_est_amt_ent, 0) ,COALESCE(EDW1.contract_nbr, ' ' ) ,COALESCE(EDW1.contract_nbr_modifier, ' ' ) ,EDW1.contract_start_date ,COALESCE(EDW1.curr_code, ' ' ) ,COALESCE(EDW1.cust_po_nbr, ' ' ) ,COALESCE(EDW1.invoice_rule_id, 0) ,COALESCE(EDW1.migration_contract_nbr, ' ' ) ,EDW1.renew_date ,COALESCE(EDW1.renewal_type_code, ' ' ) ,COALESCE(EDW1.solution_portfolio_id, ' ' ) ,COALESCE(EDW1.status_code, ' ' ) ,EDW1.renewal_type_name ,EDW1.invoice_rule_desc ,EDW1.invoice_rule_name ,EDW1.cust_trx_type_id ,EDW1.hdr_tax_exempt_sts_code ,EDW1.hdr_tax_exemption_id ,COALESCE(EDW1.hdr_trx_type_name, ' ' ) ,EDW1.hold_billing_flag ,EDW1.party_id ,EDW1.country_code ,EDW1.bill_to_addr_line_1 ,EDW1.bill_to_addr_line_2 ,EDW1.bill_to_addr_line_3 ,EDW1.bill_to_addr_line_4 ,EDW1.bill_to_city_name ,EDW1.bill_to_collctn_org_code ,EDW1.bill_to_country_code ,EDW1.bill_to_county_name ,EDW1.bill_to_cs_fml_org_code ,EDW1.bill_to_cust_indstry_code ,EDW1.bill_to_cust_indstry_name ,EDW1.bill_to_cust_name ,EDW1.bill_to_cust_nbr ,EDW1.bill_to_cust_site_name ,EDW1.bill_to_cust_site_nbr ,EDW1.bill_to_cust_type_code ,EDW1.bill_to_fml_org_code ,EDW1.bill_to_oper_unit_country_code ,EDW1.bill_to_party_id ,EDW1.bill_to_party_site_addr_id ,EDW1.bill_to_postal_code ,EDW1.bill_to_province_name ,EDW1.bill_to_state_name ,EDW1.bill_to_store_nbr ,EDW1.bill_to_svc_loc_code ,EDW1.bill_to_svc_territory_id ,EDW1.contract_group_name ,EDW1.area_code ,EDW1.area_desc ,EDW1.country_desc ,EDW1.region_code ,EDW1.region_desc ,EDW1.bill_to_cs_fml_org_name ,EDW1.bill_to_fml_org_name ,EDW1.hdr_cust_exempt_nbr ,EDW1.hdr_exempt_reason_code ,EDW1.customer_name ,EDW1.customer_nbr ,EDW2.service_line_id ,EDW1.service_line_nbr ,EDW1.service_line_style_id ,COALESCE(EDW1.service_line_style_name,' ') ,COALESCE(EDW1.tax_exempt_status_code, ' ') ,COALESCE(EDW1.tax_exemption_id, 0) ,COALESCE(EDW1.vat_tax_id , 0) ,EDW1.customer_exemption_nbr ,EDW1.exemption_reason_code ,EDW1.exemption_tax_code ,EDW1.vat_tax_code ,EDW1.inventory_item_id ,EDW1.inventory_org_id ,EDW1.line_product_id ,EDW1.line_product_id_formatted ,EDW2.service_subline_id ,EDW2.sline_line_style_id ,EDW2.sline_covered_lvl_name ,EDW2.sequence_nbr ,EDW2.billing_stream_end_date ,EDW2.billing_stream_start_date ,COALESCE(EDW2.interface_offset_day_nbr, ' ' ) ,COALESCE(EDW2.period_uom_cnt, ' ' ) ,COALESCE(EDW2.period_uom_code, ' ' ) ,COALESCE(EDW2.sl_discount_amt_ent,0) ,COALESCE(EDW2.sl_surcharge_amt_ent,0) ,COALESCE(EDW2.sl_top_lvl_adj_prc_amt_ent,0) ,COALESCE(EDW2.sl_top_lvl_oprnd_prc_amt_ent,0) ,COALESCE(EDW2.sl_top_lvl_pricing_uom_qty,0) ,COALESCE(EDW2.sl_unbilled_amt_ent,0) ,EDW2.interface_date ,EDW2.level_sequence_nbr ,EDW2.period_end_date ,EDW2.period_start_date ,CASE WHEN EDW2.invoice_type_code = 'NOT BILLED' THEN NULL       ELSE EDW2.bill_on_date  END                                                        (NAMED bill_on_date) ,EDW2.invoice_type_code ,COALESCE(EDW2.bill_amt_ent, 0) ,EDW2.completed_date ,COALESCE(EDW2.level_element_amt_ent, 0) ,COALESCE(EDW2.invoice_currency_code, ' ') ,EDW2.invoice_date ,COALESCE(EDW2.invoice_nbr, ' ') ,COALESCE(EDW2.line_credit_amt_ent,0) ,COALESCE(EDW2.sline_credit_amt_ent,0) ,COALESCE(EDW2.install_location_id,0) ,COALESCE(EDW2.item_instance_id,0) ,EDW2.external_reference_nbr ,EDW2.last_valid_invtry_org_id ,EDW2.prev_serial_nbr ,EDW2.serial_nbr ,EDW2.sline_product_id_formatted ,EDW2.subline_ind ,EDW2.service_subline_end_date ,EDW2.service_subline_nbr ,EDW2.service_subline_start_date ,EDW2.service_subline_term_date ,EDW2.service_subline_status_code ,EDW2.serviced_qty ,CURRENT_TIMESTAMP(0) ,COALESCE(EDW3.bill_to_cust_sales_chnl_code,' ') ,COALESCE(EDW3.bill_to_pnt_cust_ind_code,' ') ,COALESCE(EDW3.bill_to_pnt_cust_ind_name,' ') ,COALESCE(EDW3.naics_code,' ') ,COALESCE(EDW3.naics_desc,' ') ,COALESCE(EDW3.sub_region_name,' ') ,COALESCE(EDW3.sub_region_code,' ')   FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t51 EDW2 LEFT OUTER JOIN      ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 EDW1 ON   EDW2.instance_id         = EDW1.instance_id AND  EDW2.contract_header_id  = EDW1.contract_header_id AND  EDW2.service_line_id     = EDW1.service_line_id LEFT OUTER JOIN      ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56 EDW3 ON   EDW1.service_line_id     = EDW3.service_line_id AND  EDW1.contract_header_id  = EDW3.contract_header_id AND  EDW1.instance_id         = EDW3.instance_id  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB}.contract_bill_sched_dn_ld           SELECT
            CASE 
                WHEN EDW2.invoice_type_code = 'NOT BILLED'  THEN NULL  
                ELSE EDW2.bill_on_date  
            END AS bill_on_date,
            EDW2.contract_header_id,
            EDW2.instance_id,
            EDW2.interface_date,
            EDW2.invoice_type_code,
            EDW2.level_sequence_nbr,
            EDW2.period_end_date,
            EDW2.period_start_date,
            EDW2.sequence_nbr,
            EDW2.service_line_id,
            EDW2.service_subline_id,
            EDW1.area_code,
            EDW1.area_desc,
            COALESCE( EDW1.author_oper_unit_country_code ,
            ' ' ) AS auto_c02,
            COALESCE( EDW2.bill_amt_ent ,
            0 ) AS auto_c0101,
            EDW1.bill_to_addr_line_1,
            EDW1.bill_to_addr_line_2,
            EDW1.bill_to_addr_line_3,
            EDW1.bill_to_addr_line_4,
            EDW1.bill_to_city_name,
            EDW1.bill_to_collctn_org_code,
            EDW1.bill_to_country_code,
            EDW1.bill_to_county_name,
            EDW1.bill_to_cs_fml_org_code,
            EDW1.bill_to_cs_fml_org_name,
            EDW1.bill_to_cust_indstry_code,
            EDW1.bill_to_cust_indstry_name,
            EDW1.bill_to_cust_name,
            EDW1.bill_to_cust_nbr,
            EDW1.bill_to_cust_site_name,
            EDW1.bill_to_cust_site_nbr,
            EDW1.bill_to_cust_type_code,
            EDW1.bill_to_fml_org_code,
            EDW1.bill_to_fml_org_name,
            EDW1.bill_to_oper_unit_country_code,
            EDW1.bill_to_party_id,
            EDW1.bill_to_party_site_addr_id,
            EDW1.bill_to_postal_code,
            EDW1.bill_to_province_name,
            COALESCE( EDW1.bill_to_site_use_id ,
            0 ) AS auto_c03,
            EDW1.bill_to_state_name,
            EDW1.bill_to_store_nbr,
            EDW1.bill_to_svc_loc_code,
            EDW1.bill_to_svc_territory_id,
            EDW2.billing_stream_end_date,
            EDW2.billing_stream_start_date,
            EDW2.completed_date,
            EDW1.contract_end_date,
            COALESCE( EDW1.contract_est_amt_ent ,
            0 ) AS auto_c05,
            EDW1.contract_group_name,
            COALESCE( EDW1.contract_nbr ,
            ' ' ) AS auto_c06,
            COALESCE( EDW1.contract_nbr_modifier ,
            ' ' ) AS auto_c07,
            EDW1.contract_start_date,
            EDW1.country_code,
            EDW1.country_desc,
            COALESCE( EDW1.curr_code ,
            ' ' ) AS auto_c09,
            EDW1.customer_exemption_nbr,
            EDW1.customer_name,
            EDW1.customer_nbr,
            EDW1.exemption_reason_code,
            EDW1.exemption_tax_code,
            EDW2.external_reference_nbr,
            EDW1.hold_billing_flag,
            COALESCE( EDW2.install_location_id ,
            0 ) AS auto_c0109,
            COALESCE( EDW2.interface_offset_day_nbr ,
            ' ' ) AS auto_c086,
            EDW1.inventory_item_id,
            EDW1.inventory_org_id,
            COALESCE( EDW2.invoice_currency_code ,
            ' ' ) AS auto_c0104,
            EDW2.invoice_date,
            COALESCE( EDW2.invoice_nbr ,
            ' ' ) AS auto_c0106,
            EDW1.invoice_rule_desc,
            COALESCE( EDW1.invoice_rule_id ,
            0 ) AS auto_c011,
            EDW1.invoice_rule_name,
            COALESCE( EDW2.item_instance_id ,
            0 ) AS auto_c0110,
            EDW2.last_valid_invtry_org_id,
            COALESCE( EDW2.level_element_amt_ent ,
            0 ) AS auto_c0103,
            EDW1.line_product_id,
            EDW1.line_product_id_formatted,
            COALESCE( EDW1.migration_contract_nbr ,
            ' ' ) AS auto_c012,
            EDW1.party_id,
            COALESCE( EDW2.period_uom_cnt ,
            ' ' ) AS auto_c087,
            COALESCE( EDW2.period_uom_code ,
            ' ' ) AS auto_c088,
            EDW2.prev_serial_nbr,
            EDW1.region_code,
            EDW1.region_desc,
            EDW1.renew_date,
            COALESCE( EDW1.renewal_type_code ,
            ' ' ) AS auto_c014,
            EDW1.renewal_type_name,
            EDW2.serial_nbr,
            EDW1.service_line_nbr,
            EDW1.service_line_style_id,
            COALESCE( EDW1.service_line_style_name ,
            ' ' ) AS auto_c068,
            EDW2.service_subline_end_date,
            EDW2.service_subline_nbr,
            EDW2.service_subline_start_date,
            EDW2.service_subline_status_code,
            EDW2.service_subline_term_date,
            EDW2.sline_covered_lvl_name,
            EDW2.sline_line_style_id,
            EDW2.sline_product_id_formatted,
            COALESCE( EDW1.solution_portfolio_id ,
            ' ' ) AS auto_c015,
            COALESCE( EDW1.status_code ,
            ' ' ) AS auto_c016,
            COALESCE( EDW1.tax_exempt_status_code ,
            ' ' ) AS auto_c069,
            COALESCE( EDW1.tax_exemption_id ,
            0 ) AS auto_c070,
            CURRENT_TIMESTAMP() AS auto_c0123,
            EDW1.vat_tax_code,
            COALESCE( EDW1.vat_tax_id ,
            0 ) AS auto_c071,
            COALESCE( EDW2.line_credit_amt_ent ,
            0 ) AS auto_c0107,
            COALESCE( EDW2.sline_credit_amt_ent ,
            0 ) AS auto_c0108,
            COALESCE( EDW2.sl_discount_amt_ent ,
            0 ) AS auto_c089,
            COALESCE( EDW2.sl_surcharge_amt_ent ,
            0 ) AS auto_c090,
            COALESCE( EDW2.sl_top_lvl_adj_prc_amt_ent ,
            0 ) AS auto_c091,
            COALESCE( EDW2.sl_top_lvl_oprnd_prc_amt_ent ,
            0 ) AS auto_c092,
            COALESCE( EDW2.sl_top_lvl_pricing_uom_qty ,
            0 ) AS auto_c093,
            COALESCE( EDW2.sl_unbilled_amt_ent ,
            0 ) AS auto_c094,
            EDW2.subline_ind,
            COALESCE( EDW1.cust_po_nbr ,
            ' ' ) AS auto_c010,
            EDW1.cust_trx_type_id,
            EDW1.hdr_cust_exempt_nbr,
            EDW1.hdr_exempt_reason_code,
            EDW1.hdr_tax_exempt_sts_code,
            EDW1.hdr_tax_exemption_id,
            COALESCE( EDW1.hdr_trx_type_name ,
            ' ' ) AS auto_c023,
            EDW2.serviced_qty,
            COALESCE( EDW3.bill_to_cust_sales_chnl_code ,
            ' ' ) AS auto_c0124,
            COALESCE( EDW3.bill_to_pnt_cust_ind_code ,
            ' ' ) AS auto_c0125,
            COALESCE( EDW3.bill_to_pnt_cust_ind_name ,
            ' ' ) AS auto_c0126,
            COALESCE( EDW3.naics_code ,
            ' ' ) AS auto_c0127,
            COALESCE( EDW3.naics_desc ,
            ' ' ) AS auto_c0128,
            COALESCE( EDW3.sub_region_code ,
            ' ' ) AS auto_c0130,
            COALESCE( EDW3.sub_region_name ,
            ' ' ) AS auto_c0129  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t51    AS EDW2   
        LEFT OUTER  JOIN
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25    AS EDW1    
                ON upper(trim( EDW2.instance_id ))= upper(trim( EDW1.instance_id )) 
                AND upper(trim( EDW2.contract_header_id)) = upper(trim( EDW1.contract_header_id ))  
                AND upper(trim( EDW2.service_line_id)) = upper(trim( EDW1.service_line_id))    
        LEFT OUTER  JOIN
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56    AS EDW3    
                ON upper(trim( EDW1.service_line_id)) = upper(trim( EDW3.service_line_id )) 
                AND upper(trim( EDW1.contract_header_id)) = upper(trim( EDW3.contract_header_id))   
                AND upper(trim( EDW1.instance_id)) = upper(trim( EDW3.instance_id));
