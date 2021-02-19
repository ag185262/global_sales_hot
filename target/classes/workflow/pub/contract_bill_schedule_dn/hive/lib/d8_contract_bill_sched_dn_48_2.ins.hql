----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:56 
--Script Name: d8_contract_bill_sched_dn_48_2.ins.sql
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
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 COLUMN(install_location_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 COMPUTE STATISTICS  FOR COLUMNS install_location_id;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48  (  instance_id ,contract_header_id ,service_line_id ,service_subline_id ,sline_line_style_id ,sline_covered_lvl_name ,sequence_nbr ,billing_stream_end_date ,billing_stream_start_date ,contr_bill_stream_id ,interface_offset_day_nbr ,period_uom_cnt ,period_uom_code ,sl_discount_amt_ent ,sl_surcharge_amt_ent ,sl_top_lvl_adj_prc_amt_ent ,sl_top_lvl_oprnd_prc_amt_ent ,sl_top_lvl_pricing_uom_qty ,sl_unbilled_amt_ent ,credit_amt_ent ,interface_date ,level_sequence_nbr ,period_end_date ,period_start_date ,bill_on_date ,invoice_type_code ,bill_amt_ent ,completed_date ,level_element_amt_ent ,invoice_currency_code ,invoice_date ,invoice_nbr ,line_credit_amt_ent ,sline_credit_amt_ent ,install_location_id ,item_instance_id ,external_reference_nbr ,inventory_item_id ,last_valid_invtry_org_id ,prev_serial_nbr ,serial_nbr ,sline_product_id_formatted ) SELECT   EDW1.instance_id ,EDW1.contract_header_id ,EDW1.service_line_id ,EDW1.service_subline_id ,EDW1.sline_line_style_id ,EDW1.sline_covered_lvl_name ,EDW1.sequence_nbr ,EDW1.billing_stream_end_date ,EDW1.billing_stream_start_date ,EDW1.contr_bill_stream_id ,EDW1.interface_offset_day_nbr ,EDW1.period_uom_cnt ,EDW1.period_uom_code ,EDW1.sl_discount_amt_ent ,EDW1.sl_surcharge_amt_ent ,EDW1.sl_top_lvl_adj_prc_amt_ent ,EDW1.sl_top_lvl_oprnd_prc_amt_ent ,EDW1.sl_top_lvl_pricing_uom_qty ,EDW1.sl_unbilled_amt_ent ,EDW1.credit_amt_ent ,EDW1.interface_date ,EDW1.level_sequence_nbr ,EDW1.period_end_date ,EDW1.period_start_date ,EDW1.bill_on_date ,EDW1.invoice_type_code ,EDW1.bill_amt_ent ,EDW1.completed_date ,EDW1.level_element_amt_ent ,EDW1.invoice_currency_code ,EDW1.invoice_date ,EDW1.invoice_nbr ,EDW1.line_credit_amt_ent ,EDW1.sline_credit_amt_ent ,EDW1.install_location_id ,EDW1.item_instance_id ,EDW1.external_reference_nbr ,EDW1.inventory_item_id ,EDW1.last_valid_invtry_org_id ,EDW1.prev_serial_nbr ,EDW1.serial_nbr <WM_COMMENT_START> sline_product_id_formatted <WM_COMMENT_END> ,EDW2.customer_site_nbr  FROM  ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 EDW1,       (SELECT party_site_id               ,instance_id               ,customer_site_nbr        FROM   ${EEDDWWDDBB}.party_site_ld        ) EDW2  WHERE    EDW1.install_location_id = EDW2.party_site_id AND      EDW1.instance_id = EDW2.instance_id AND      EDW1.sline_line_style_id = 10  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
            EDW1.service_line_id,
            EDW1.service_subline_id,
            EDW1.sline_line_style_id,
            EDW1.sline_covered_lvl_name,
            EDW1.sequence_nbr,
            EDW1.billing_stream_end_date,
            EDW1.billing_stream_start_date,
            EDW1.contr_bill_stream_id,
            EDW1.interface_offset_day_nbr,
            EDW1.period_uom_cnt,
            EDW1.period_uom_code,
            EDW1.sl_discount_amt_ent,
            EDW1.sl_surcharge_amt_ent,
            EDW1.sl_top_lvl_adj_prc_amt_ent,
            EDW1.sl_top_lvl_oprnd_prc_amt_ent,
            EDW1.sl_top_lvl_pricing_uom_qty,
            EDW1.sl_unbilled_amt_ent,
            EDW1.credit_amt_ent,
            EDW1.interface_date,
            EDW1.level_sequence_nbr,
            EDW1.period_end_date,
            EDW1.period_start_date,
            EDW1.bill_on_date,
            EDW1.invoice_type_code,
            EDW1.bill_amt_ent,
            EDW1.completed_date,
            EDW1.level_element_amt_ent,
            EDW1.invoice_currency_code,
            EDW1.invoice_date,
            EDW1.invoice_nbr,
            EDW1.line_credit_amt_ent,
            EDW1.sline_credit_amt_ent,
            EDW1.item_instance_id,
            EDW1.install_location_id,
            EDW1.external_reference_nbr,
            EDW1.inventory_item_id,
            EDW1.last_valid_invtry_org_id,
            EDW1.prev_serial_nbr,
            EDW1.serial_nbr,
            EDW2.customer_site_nbr  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47    AS EDW1   ,
            (   SELECT
                party_site_id,
                instance_id,
                customer_site_nbr  
            FROM
                ${EEDDWWDDBB}.party_site_ld            )    AS EDW2   
        WHERE
            EDW1.install_location_id = EDW2.party_site_id
            AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id ))  
            AND EDW1.sline_line_style_id = 10;
