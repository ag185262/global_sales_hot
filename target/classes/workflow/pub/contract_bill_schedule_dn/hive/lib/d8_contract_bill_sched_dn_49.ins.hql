----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:56 
--Script Name: d8_contract_bill_sched_dn_49.ins.sql
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
  --COLLECT STATISTICS COLUMN (CONTRACT_HEADER_ID ,INSTANCE_ID,SERVICE_LINE_ID ,SERVICE_SUBLINE_ID ,SEQUENCE_NBR,INTERFACE_DATE ,LEVEL_SEQUENCE_NBR ,PERIOD_END_DATE,PERIOD_START_DATE ,BILL_ON_DATE ,INVOICE_TYPE_CODE ,INVOICE_NBR,ITEM_INSTANCE_ID), COLUMN (SERVICE_SUBLINE_ID,ITEM_INSTANCE_ID) , COLUMN (CONTRACT_HEADER_ID ,INSTANCE_ID,SERVICE_LINE_ID ,SEQUENCE_NBR ,INTERFACE_DATE,LEVEL_SEQUENCE_NBR ,PERIOD_END_DATE ,PERIOD_START_DATE,BILL_ON_DATE ,INVOICE_TYPE_CODE ,INVOICE_NBR) , COLUMN (INVOICE_NBR) , COLUMN (SERVICE_LINE_ID), COLUMN (SEQUENCE_NBR), COLUMN (PERIOD_START_DATE), COLUMN (PERIOD_END_DATE), COLUMN (LEVEL_SEQUENCE_NBR), COLUMN (INVOICE_TYPE_CODE), COLUMN (INTERFACE_DATE), COLUMN (INSTANCE_ID), COLUMN (CONTRACT_HEADER_ID), COLUMN (BILL_ON_DATE) ON     ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48 COMPUTE STATISTICS  FOR COLUMNS CONTRACT_HEADER_ID,INSTANCE_ID,SERVICE_LINE_ID,SERVICE_SUBLINE_ID,SEQUENCE_NBR,INTERFACE_DATE,LEVEL_SEQUENCE_NBR,PERIOD_END_DATE,PERIOD_START_DATE,BILL_ON_DATE,INVOICE_TYPE_CODE,INVOICE_NBR,ITEM_INSTANCE_ID  , SERVICE_SUBLINE_ID,ITEM_INSTANCE_ID  , CONTRACT_HEADER_ID,INSTANCE_ID,SERVICE_LINE_ID,SEQUENCE_NBR,INTERFACE_DATE,LEVEL_SEQUENCE_NBR,PERIOD_END_DATE,PERIOD_START_DATE,BILL_ON_DATE,INVOICE_TYPE_CODE,INVOICE_NBR  , INVOICE_NBR  , SERVICE_LINE_ID  , SEQUENCE_NBR  , PERIOD_START_DATE  , PERIOD_END_DATE  , LEVEL_SEQUENCE_NBR  , INVOICE_TYPE_CODE  , INTERFACE_DATE  , INSTANCE_ID  , CONTRACT_HEADER_ID  , BILL_ON_DATE;

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t49 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t49;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t49  (   instance_id  ,contract_header_id  ,service_line_id  ,service_subline_id  ,sline_line_style_id  ,sline_covered_lvl_name  ,sequence_nbr  ,billing_stream_end_date  ,billing_stream_start_date  ,contr_bill_stream_id  ,interface_offset_day_nbr  ,period_uom_cnt  ,period_uom_code  ,sl_discount_amt_ent           ,sl_surcharge_amt_ent          ,sl_top_lvl_adj_prc_amt_ent    ,sl_top_lvl_oprnd_prc_amt_ent  ,sl_top_lvl_pricing_uom_qty  ,sl_unbilled_amt_ent      ,credit_amt_ent  ,interface_date  ,level_sequence_nbr  ,period_end_date  ,period_start_date  ,bill_on_date  ,invoice_type_code  ,bill_amt_ent  ,completed_date  ,level_element_amt_ent  ,invoice_currency_code  ,invoice_date  ,invoice_nbr  ,line_credit_amt_ent   ,sline_credit_amt_ent  ,install_location_id  ,item_instance_id          ,external_reference_nbr  ,last_valid_invtry_org_id  ,prev_serial_nbr  ,serial_nbr  ,sline_product_id_formatted          ,subline_ind ) SELECT   EDW1.instance_id ,EDW1.contract_header_id ,EDW1.service_line_id ,EDW1.service_subline_id ,EDW1.sline_line_style_id ,EDW1.sline_covered_lvl_name ,EDW1.sequence_nbr ,EDW1.billing_stream_end_date ,EDW1.billing_stream_start_date ,EDW1.contr_bill_stream_id ,EDW1.interface_offset_day_nbr ,EDW1.period_uom_cnt ,EDW1.period_uom_code ,EDW1.sl_discount_amt_ent ,EDW1.sl_surcharge_amt_ent ,EDW1.sl_top_lvl_adj_prc_amt_ent ,EDW1.sl_top_lvl_oprnd_prc_amt_ent ,EDW1.sl_top_lvl_pricing_uom_qty ,EDW1.sl_unbilled_amt_ent ,EDW1.credit_amt_ent ,EDW1.interface_date ,EDW1.level_sequence_nbr ,EDW1.period_end_date ,EDW1.period_start_date ,EDW1.bill_on_date ,EDW1.invoice_type_code ,EDW1.bill_amt_ent ,EDW1.completed_date ,EDW1.level_element_amt_ent ,EDW1.invoice_currency_code ,EDW1.invoice_date ,EDW1.invoice_nbr ,EDW1.line_credit_amt_ent ,EDW1.sline_credit_amt_ent ,EDW1.install_location_id ,EDW1.item_instance_id ,EDW1.external_reference_nbr ,EDW1.last_valid_invtry_org_id ,EDW1.prev_serial_nbr ,EDW1.serial_nbr ,EDW1.sline_product_id_formatted ,1 FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48 EDW1 WHERE (bill_on_date ,contract_header_id ,instance_id ,         interface_date ,invoice_type_code ,level_sequence_nbr ,period_end_date ,         period_start_date ,sequence_nbr ,service_line_id ,         invoice_nbr , service_subline_id||item_instance_id) IN  ( SELECT bill_on_date ,contract_header_id ,instance_id ,         interface_date ,invoice_type_code ,level_sequence_nbr ,period_end_date ,         period_start_date ,sequence_nbr ,service_line_id ,         invoice_nbr , MAX(service_subline_id||item_instance_id) FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48 GROUP BY 1,2,3,4,5,6,7,8,9,10,11 )  

--Translated Query: STATUS SUCCESS (Modified the query from an inner join to using row number for fetch a MAX Value)

    with qq1 as (
SELECT
             contract_header_id,
             instance_id,
             service_line_id,
             service_subline_id,
             sline_line_style_id,
             sline_covered_lvl_name,
             sequence_nbr,
             billing_stream_end_date,
             billing_stream_start_date,
             contr_bill_stream_id,
             interface_offset_day_nbr,
             period_uom_cnt,
             period_uom_code,
             sl_discount_amt_ent,
             sl_surcharge_amt_ent,
             sl_top_lvl_adj_prc_amt_ent,
             sl_top_lvl_oprnd_prc_amt_ent,
             sl_top_lvl_pricing_uom_qty,
             sl_unbilled_amt_ent,
             credit_amt_ent,
             interface_date,
             level_sequence_nbr,
             period_end_date,
             period_start_date,
             bill_on_date,
             invoice_type_code,
             bill_amt_ent,
             completed_date,
             level_element_amt_ent,
             invoice_currency_code,
             invoice_date,
             invoice_nbr,
             line_credit_amt_ent,
             sline_credit_amt_ent,
             item_instance_id,
             install_location_id,
             external_reference_nbr,
             last_valid_invtry_org_id,
             prev_serial_nbr,
             serial_nbr,
             sline_product_id_formatted,
             ROW_NUMBER() OVER (PARTITION BY bill_on_date ,contract_header_id ,instance_id ,interface_date ,invoice_type_code ,level_sequence_nbr ,period_end_date ,period_start_date ,sequence_nbr ,service_line_id ,invoice_nbr ORDER BY CONCAT (service_subline_id ,item_instance_id) DESC) AS RNK
            FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48
) 
INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t49 
SELECT 
             EDW2.contract_header_id,
             EDW2.instance_id,
             EDW2.service_line_id,
             EDW2.service_subline_id,
             EDW2.sline_line_style_id,
             EDW2.sline_covered_lvl_name,
             EDW2.sequence_nbr,
             EDW2.billing_stream_end_date,
             EDW2.billing_stream_start_date,
             EDW2.contr_bill_stream_id,
             EDW2.interface_offset_day_nbr,
             EDW2.period_uom_cnt,
             EDW2.period_uom_code,
             EDW2.sl_discount_amt_ent,
             EDW2.sl_surcharge_amt_ent,
             EDW2.sl_top_lvl_adj_prc_amt_ent,
             EDW2.sl_top_lvl_oprnd_prc_amt_ent,
             EDW2.sl_top_lvl_pricing_uom_qty,
             EDW2.sl_unbilled_amt_ent,
             EDW2.credit_amt_ent,
             EDW2.interface_date,
             EDW2.level_sequence_nbr,
             EDW2.period_end_date,
             EDW2.period_start_date,
             EDW2.bill_on_date,
             EDW2.invoice_type_code,
             EDW2.bill_amt_ent,
             EDW2.completed_date,
             EDW2.level_element_amt_ent,
             EDW2.invoice_currency_code,
             EDW2.invoice_date,
             EDW2.invoice_nbr,
             EDW2.line_credit_amt_ent,
             EDW2.sline_credit_amt_ent,
             EDW2.item_instance_id,
             EDW2.install_location_id,
             EDW2.external_reference_nbr,
             EDW2.last_valid_invtry_org_id,
             EDW2.prev_serial_nbr,
             EDW2.serial_nbr,
             EDW2.sline_product_id_formatted,
1 AS auto_c041  
FROM 
  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t48 EDW2 
  INNER JOIN (
    SELECT 
            contract_header_id,
             instance_id,
             service_line_id,
             service_subline_id,
             sline_line_style_id,
             sline_covered_lvl_name,
             sequence_nbr,
             billing_stream_end_date,
             billing_stream_start_date,
             contr_bill_stream_id,
             interface_offset_day_nbr,
             period_uom_cnt,
             period_uom_code,
             sl_discount_amt_ent,
             sl_surcharge_amt_ent,
             sl_top_lvl_adj_prc_amt_ent,
             sl_top_lvl_oprnd_prc_amt_ent,
             sl_top_lvl_pricing_uom_qty,
             sl_unbilled_amt_ent,
             credit_amt_ent,
             interface_date,
             level_sequence_nbr,
             period_end_date,
             period_start_date,
             bill_on_date,
             invoice_type_code,
             bill_amt_ent,
             completed_date,
             level_element_amt_ent,
             invoice_currency_code,
             invoice_date,
             invoice_nbr,
             line_credit_amt_ent,
             sline_credit_amt_ent,
             item_instance_id,
             install_location_id,
             external_reference_nbr,
             last_valid_invtry_org_id,
             prev_serial_nbr,
             serial_nbr,
             sline_product_id_formatted, 
            RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND EDW2.bill_on_date=IBVL.bill_on_date
   AND upper(trim(EDW2.contract_header_id)) =upper(trim(IBVL.contract_header_id)) 
   AND EDW2.interface_date=IBVL.interface_date
   AND upper(trim(EDW2.invoice_type_code)) = upper(trim(IBVL.invoice_type_code)) 
   AND upper(trim(EDW2.level_sequence_nbr)) =upper(trim(IBVL.level_sequence_nbr))  
   AND EDW2.period_end_date =IBVL.period_end_date
   AND EDW2.period_start_date=IBVL.period_start_date  
   AND EDW2.sequence_nbr = IBVL.sequence_nbr
   AND upper(trim(EDW2.service_line_id )) =upper(trim(IBVL.service_line_id )) 
   AND upper(trim(EDW2.invoice_nbr)) = upper(trim(IBVL.invoice_nbr));
