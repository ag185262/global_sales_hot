----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:55 
--Script Name: d8_contract_bill_sched_dn_47.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47;

--Original Query:
  --COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 COLUMN item_instance_id 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 COMPUTE STATISTICS  FOR COLUMNS item_instance_id;

--Original Query:
  --COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 COLUMN instance_id 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATISTICS ON ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 COLUMN (SLINE_LINE_STYLE_ID) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 COMPUTE STATISTICS  FOR COLUMNS SLINE_LINE_STYLE_ID;

--Original Query:
  --COLLECT STATISTICS ON ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 COLUMN (INSTANCE_ID ,ITEM_INSTANCE_ID) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,ITEM_INSTANCE_ID;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 FOR ACCESS  LOCK TABLE ${EEDDWWDDBB}.install_base_item_ld FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47  (  instance_id ,contract_header_id                  ,service_line_id                     ,service_subline_id                  ,sline_line_style_id                 ,sline_covered_lvl_name              ,sequence_nbr                        ,billing_stream_end_date             ,billing_stream_start_date           ,contr_bill_stream_id                ,interface_offset_day_nbr            ,period_uom_cnt                      ,period_uom_code                     ,sl_discount_amt_ent                 ,sl_surcharge_amt_ent                ,sl_top_lvl_adj_prc_amt_ent          ,sl_top_lvl_oprnd_prc_amt_ent        ,sl_top_lvl_pricing_uom_qty          ,sl_unbilled_amt_ent                 ,credit_amt_ent                      ,interface_date                      ,level_sequence_nbr                  ,period_end_date                     ,period_start_date                   ,bill_on_date                        ,invoice_type_code                   ,bill_amt_ent                        ,completed_date                      ,level_element_amt_ent               ,invoice_currency_code               ,invoice_date                        ,invoice_nbr                         ,line_credit_amt_ent                 ,sline_credit_amt_ent                ,install_location_id                 ,item_instance_id                    ,external_reference_nbr              ,inventory_item_id                   ,last_valid_invtry_org_id            ,prev_serial_nbr                     ,serial_nbr                          ) SELECT   EDW1.instance_id ,EDW1.contract_header_id ,EDW1.service_line_id ,EDW1.service_subline_id ,EDW1.sline_line_style_id ,EDW1.sline_covered_lvl_name ,EDW1.sequence_nbr ,EDW1.billing_stream_end_date ,EDW1.billing_stream_start_date ,EDW1.contr_bill_stream_id ,EDW1.interface_offset_day_nbr ,EDW1.period_uom_cnt ,EDW1.period_uom_code ,EDW1.sl_discount_amt_ent          ,EDW1.sl_surcharge_amt_ent         ,EDW1.sl_top_lvl_adj_prc_amt_ent   ,EDW1.sl_top_lvl_oprnd_prc_amt_ent ,EDW1.sl_top_lvl_pricing_uom_qty ,EDW1.sl_unbilled_amt_ent     ,EDW1.credit_amt_ent ,EDW1.interface_date ,EDW1.level_sequence_nbr ,EDW1.period_end_date ,EDW1.period_start_date ,EDW1.bill_on_date ,EDW1.invoice_type_code ,EDW1.bill_amt_ent ,EDW1.completed_date ,EDW1.level_element_amt_ent ,EDW1.invoice_currency_code ,EDW1.invoice_date ,EDW1.invoice_nbr ,EDW1.line_credit_amt_ent  ,EDW1.sline_credit_amt_ent ,EDW1.install_location_id ,EDW1.item_instance_id <WM_COMMENT_START> external_reference_nbr <WM_COMMENT_END> ,COALESCE(EDW2.external_reference_nbr,' ') <WM_COMMENT_START> inventory_item_id <WM_COMMENT_END> ,COALESCE(EDW2.inventory_item_id, ' ') <WM_COMMENT_START> last_valid_invtry_org_id <WM_COMMENT_END> ,COALESCE(EDW2.last_valid_invtry_org_id,0) <WM_COMMENT_START> prev_serial_nbr <WM_COMMENT_END> ,COALESCE(EDW2.prev_serial_nbr,' ') <WM_COMMENT_START> serial_nbr <WM_COMMENT_END> ,COALESCE(EDW2.serial_nbr,' ')  FROM  ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 EDW1 LEFT OUTER JOIN       ${EEDDWWDDBB}.install_base_item_ld EDW2 ON    EDW1.item_instance_id = EDW2.item_instance_id AND   EDW1.instance_id  = EDW2.instance_id WHERE EDW1.sline_line_style_id = 9  UNION ALL  SELECT   EDW1.instance_id ,EDW1.contract_header_id ,EDW1.service_line_id ,EDW1.service_subline_id ,EDW1.sline_line_style_id ,EDW1.sline_covered_lvl_name ,EDW1.sequence_nbr ,EDW1.billing_stream_end_date ,EDW1.billing_stream_start_date ,EDW1.contr_bill_stream_id ,EDW1.interface_offset_day_nbr ,EDW1.period_uom_cnt ,EDW1.period_uom_code ,EDW1.sl_discount_amt_ent          ,EDW1.sl_surcharge_amt_ent         ,EDW1.sl_top_lvl_adj_prc_amt_ent   ,EDW1.sl_top_lvl_oprnd_prc_amt_ent ,EDW1.sl_top_lvl_pricing_uom_qty ,EDW1.sl_unbilled_amt_ent     ,EDW1.credit_amt_ent ,EDW1.interface_date ,EDW1.level_sequence_nbr ,EDW1.period_end_date ,EDW1.period_start_date ,EDW1.bill_on_date ,EDW1.invoice_type_code ,EDW1.bill_amt_ent ,EDW1.completed_date ,EDW1.level_element_amt_ent ,EDW1.invoice_currency_code ,EDW1.invoice_date ,EDW1.invoice_nbr ,EDW1.line_credit_amt_ent  ,EDW1.sline_credit_amt_ent ,EDW1.install_location_id ,COALESCE(EDW1.item_instance_id, 0) <WM_COMMENT_START> external_reference_nbr <WM_COMMENT_END> ,' ' <WM_COMMENT_START> inventory_item_id <WM_COMMENT_END> ,' ' <WM_COMMENT_START> last_valid_invtry_org_id <WM_COMMENT_END> , 0 <WM_COMMENT_START> prev_serial_nbr <WM_COMMENT_END> ,' ' <WM_COMMENT_START> serial_nbr <WM_COMMENT_END> ,' '  FROM    ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 EDW1 WHERE   EDW1.sline_line_style_id = 10  

--Translated Query: STATUS SUCCESS (Added trim+upper on join condition)

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t47           SELECT
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
            COALESCE( EDW2.external_reference_nbr ,
            ' ' ) AS auto_c036,
            COALESCE( EDW2.last_valid_invtry_org_id ,
            0 ) AS auto_c038,
            COALESCE( EDW2.prev_serial_nbr ,
            ' ' ) AS auto_c039,
            COALESCE( EDW2.serial_nbr ,
            ' ' ) AS auto_c040,
            COALESCE( EDW2.inventory_item_id ,
            ' ' ) AS auto_c037  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB}.install_base_item_ld    AS EDW2    
                ON trim(upper(EDW1.item_instance_id)) = trim(upper(EDW2.item_instance_id))  
                AND  trim(upper(EDW1.instance_id)) =  trim(upper(EDW2.instance_id))    
        WHERE
            EDW1.sline_line_style_id = 9     
        UNION
        ALL   
        SELECT
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
            COALESCE( EDW1.item_instance_id ,
            0 ) AS auto_c035,
            EDW1.install_location_id,
            ' ' AS auto_c036,
            0 AS auto_c038,
            ' ' AS auto_c039,
            ' ' AS auto_c040,
            ' ' AS auto_c037  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46    AS EDW1   
        WHERE
            EDW1.sline_line_style_id = 10;
