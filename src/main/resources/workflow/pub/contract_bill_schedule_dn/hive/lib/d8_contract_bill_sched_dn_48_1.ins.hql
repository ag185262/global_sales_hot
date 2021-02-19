----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:56 
--Script Name: d8_contract_bill_sched_dn_48_1.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 COLUMN(instance_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 COLUMN(inventory_item_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 COMPUTE STATISTICS  FOR COLUMNS inventory_item_id;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 COLUMN(last_valid_invtry_org_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 COMPUTE STATISTICS  FOR COLUMNS last_valid_invtry_org_id;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48 (  instance_id ,contract_header_id ,service_line_id ,service_subline_id ,sline_line_style_id ,sline_covered_lvl_name ,sequence_nbr ,billing_stream_end_date ,billing_stream_start_date ,contr_bill_stream_id ,interface_offset_day_nbr ,period_uom_cnt ,period_uom_code ,sl_discount_amt_ent ,sl_surcharge_amt_ent ,sl_top_lvl_adj_prc_amt_ent ,sl_top_lvl_oprnd_prc_amt_ent ,sl_top_lvl_pricing_uom_qty ,sl_unbilled_amt_ent ,credit_amt_ent ,interface_date ,level_sequence_nbr ,period_end_date ,period_start_date ,bill_on_date ,invoice_type_code ,bill_amt_ent ,completed_date ,level_element_amt_ent ,invoice_currency_code ,invoice_date ,invoice_nbr ,line_credit_amt_ent ,sline_credit_amt_ent ,install_location_id ,item_instance_id ,external_reference_nbr ,inventory_item_id ,last_valid_invtry_org_id ,prev_serial_nbr ,serial_nbr ,sline_product_id_formatted ) WITH T9 (sline_line_style_id) AS (SELECT 9 )  SELECT   EDW1.instance_id ,EDW1.contract_header_id ,EDW1.service_line_id ,EDW1.service_subline_id ,EDW1.sline_line_style_id ,EDW1.sline_covered_lvl_name ,EDW1.sequence_nbr ,EDW1.billing_stream_end_date ,EDW1.billing_stream_start_date ,EDW1.contr_bill_stream_id ,EDW1.interface_offset_day_nbr ,EDW1.period_uom_cnt ,EDW1.period_uom_code ,EDW1.sl_discount_amt_ent ,EDW1.sl_surcharge_amt_ent ,EDW1.sl_top_lvl_adj_prc_amt_ent ,EDW1.sl_top_lvl_oprnd_prc_amt_ent ,EDW1.sl_top_lvl_pricing_uom_qty ,EDW1.sl_unbilled_amt_ent ,EDW1.credit_amt_ent ,EDW1.interface_date ,EDW1.level_sequence_nbr ,EDW1.period_end_date ,EDW1.period_start_date ,EDW1.bill_on_date ,EDW1.invoice_type_code ,EDW1.bill_amt_ent ,EDW1.completed_date ,EDW1.level_element_amt_ent ,EDW1.invoice_currency_code ,EDW1.invoice_date ,EDW1.invoice_nbr ,EDW1.line_credit_amt_ent ,EDW1.sline_credit_amt_ent ,EDW1.install_location_id ,EDW1.item_instance_id ,EDW1.external_reference_nbr ,EDW1.inventory_item_id ,EDW1.last_valid_invtry_org_id ,EDW1.prev_serial_nbr ,EDW1.serial_nbr <WM_COMMENT_START> sline_product_id_formatted <WM_COMMENT_END> ,COALESCE(EDW2.offering_id_hyphenated,' ')  FROM  ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 EDW1 JOIN             (SELECT  a.instance_id                     ,a.inventory_item_id                     ,a.inventory_org_id                      ,a.offering_id_hyphenated               FROM ${AAPPLLIIDD1EENNVV}_cfs_invtry_item_xref a JOIN                          (SELECT  inventory_item_id                                   ,last_valid_invtry_org_id                                  ,instance_id                            FROM  ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47                             WHERE sline_line_style_id = 9                            GROUP  BY 1,2,3                           ) b                ON a.inventory_item_id = b.inventory_item_id                AND a.inventory_org_id = b.last_valid_invtry_org_id                 AND a.instance_id = b.instance_id              ) EDW2 ON    EDW1.inventory_item_id = EDW2.inventory_item_id       AND   EDW1.last_valid_invtry_org_id = EDW2.inventory_org_id AND   EDW1.instance_id = EDW2.instance_id  WHERE sline_line_style_id = 9 <WM_COMMENT_START> INNER JOIN       ${AAPPLLIIDD1EENNVV}_cfs_invtry_item_xref EDW2 ON    EDW1.inventory_item_id = EDW2.inventory_item_id AND   EDW1.last_valid_invtry_org_id = EDW2.inventory_org_id AND   EDW1.instance_id = EDW2.instance_id ,T9                   WHERE EDW1.sline_line_style_id = T9.sline_line_style_id <WM_COMMENT_END>  

--Translated Query: STATUS FAILED

    INSERT INTO TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48 SELECT EDW1.contract_header_id, EDW1.instance_id, EDW1.service_line_id, EDW1.service_subline_id, EDW1.sline_line_style_id, EDW1.sline_covered_lvl_name, EDW1.sequence_nbr, EDW1.billing_stream_end_date, EDW1.billing_stream_start_date, EDW1.contr_bill_stream_id, EDW1.interface_offset_day_nbr, EDW1.period_uom_cnt, EDW1.period_uom_code, EDW1.sl_discount_amt_ent, EDW1.sl_surcharge_amt_ent, EDW1.sl_top_lvl_adj_prc_amt_ent, EDW1.sl_top_lvl_oprnd_prc_amt_ent, EDW1.sl_top_lvl_pricing_uom_qty, EDW1.sl_unbilled_amt_ent, EDW1.credit_amt_ent, EDW1.interface_date, EDW1.level_sequence_nbr, EDW1.period_end_date, EDW1.period_start_date, EDW1.bill_on_date, EDW1.invoice_type_code, EDW1.bill_amt_ent, EDW1.completed_date, EDW1.level_element_amt_ent, EDW1.invoice_currency_code, EDW1.invoice_date, EDW1.invoice_nbr, EDW1.line_credit_amt_ent, EDW1.sline_credit_amt_ent, EDW1.item_instance_id, EDW1.install_location_id, EDW1.external_reference_nbr, EDW1.inventory_item_id, EDW1.last_valid_invtry_org_id, EDW1.prev_serial_nbr, EDW1.serial_nbr, EDW2.offering_id_hyphenated FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 AS EDW1 JOIN ( SELECT a.instance_id, a.inventory_item_id, a.inventory_org_id, a.offering_id_hyphenated  FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_invtry_item_xref    AS a    JOIN ( SELECT inventory_item_id, last_valid_invtry_org_id, instance_id  FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47     WHERE sline_line_style_id = 9  GROUP BY inventory_item_id , last_valid_invtry_org_id , instance_id      ) AS b    ON a.inventory_item_id = b.inventory_item_id  AND a.inventory_org_id = b.last_valid_invtry_org_id   AND upper(trim(a.instance_id)) = upper(trim(b.instance_id))           ) AS EDW2    ON EDW1.inventory_item_id = EDW2.inventory_item_id  AND EDW1.last_valid_invtry_org_id = EDW2.inventory_org_id   AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))    WHERE sline_line_style_id = 9;
