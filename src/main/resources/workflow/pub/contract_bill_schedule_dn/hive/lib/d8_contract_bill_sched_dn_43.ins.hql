----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:50 
--Script Name: d8_contract_bill_sched_dn_43.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t43 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t43;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42 FOR ACCESS LOCK TABLE ${EEDDWWDDBB}.contract_lvl_element_ld FOR ACCESS  LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t43_trg1 FOR ACCESS   INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t43 (  instance_id ,contract_header_id ,service_line_id ,service_subline_id ,sline_line_style_id ,sline_covered_lvl_name ,sequence_nbr ,billing_stream_end_date ,billing_stream_start_date ,contr_bill_stream_id ,interface_offset_day_nbr ,period_uom_cnt ,period_uom_code ,sl_discount_amt_ent          ,sl_surcharge_amt_ent         ,sl_top_lvl_adj_prc_amt_ent   ,sl_top_lvl_oprnd_prc_amt_ent ,sl_top_lvl_pricing_uom_qty ,sl_unbilled_amt_ent     ,credit_amt_ent ,interface_date ,level_sequence_nbr ,period_end_date ,period_start_date ,completed_date ,level_element_amt_ent   ) SELECT	  EDW1.instance_id ,EDW1.contract_header_id ,EDW1.service_line_id ,EDW1.service_subline_id ,EDW1.sline_line_style_id ,EDW1.sline_covered_lvl_name ,EDW1.sequence_nbr ,EDW1.billing_stream_end_date ,EDW1.billing_stream_start_date ,EDW1.contr_bill_stream_id ,EDW1.interface_offset_day_nbr ,EDW1.period_uom_cnt ,EDW1.period_uom_code ,EDW1.sl_discount_amt_ent          ,EDW1.sl_surcharge_amt_ent         ,EDW1.sl_top_lvl_adj_prc_amt_ent   ,EDW1.sl_top_lvl_oprnd_prc_amt_ent ,EDW1.sl_top_lvl_pricing_uom_qty ,EDW1.sl_unbilled_amt_ent     ,EDW1.credit_amt_ent         <WM_COMMENT_START> interface date <WM_COMMENT_END>  ,CAST(EDW2.interface_date_time AS DATE) <WM_COMMENT_START> level_sequence_nbr <WM_COMMENT_END> ,EDW2.sequence_nbr <WM_COMMENT_START> period_end_date <WM_COMMENT_END> ,EDW2.period_end_date <WM_COMMENT_START> period_start_date <WM_COMMENT_END> ,EDW2.period_start_date <WM_COMMENT_START> completed_date <WM_COMMENT_END> ,CAST(EDW2.completed_date_time AS DATE) <WM_COMMENT_START>level_element_amt_ent <WM_COMMENT_END> ,EDW2.level_element_amt_ent	        FROM	${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42 EDW1,         ${EEDDWWDDBB}.contract_lvl_element_ld EDW2,         ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t43_trg1 EDW3 WHERE   EDW1.contr_bill_stream_id = EDW2.rule_id AND     EDW1.instance_id	 = EDW2.instance_id AND     EDW1.contract_header_id	 = EDW3.contract_header_id AND     EDW1.instance_id	 = EDW3.instance_id AND     EDW1.sequence_nbr	 = EDW3.sequence_nbr AND     EDW1.service_line_id	 = EDW3.service_line_id AND     EDW1.service_subline_id  = EDW3.service_subline_id AND     EDW2.level_element_id    = EDW3.level_element_id AND     EDW2.instance_id	 = EDW3.instance_id  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t43           SELECT
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
            CAST (EDW2.interface_date_time AS date) AS auto_c020,
            EDW2.sequence_nbr,
            EDW2.period_end_date,
            EDW2.period_start_date,
            CAST (EDW2.completed_date_time AS date) AS auto_c024,
            EDW2.level_element_amt_ent  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42    AS EDW1   ,
            ${EEDDWWDDBB}.contract_lvl_element_ld    AS EDW2   ,
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t43_trg1    AS EDW3   
        WHERE
            upper(trim(EDW1.contr_bill_stream_id)) = upper(trim(EDW2.rule_id )) 
            AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id ))  
            AND upper(trim(EDW1.contract_header_id)) = upper(trim(EDW3.contract_header_id  )) 
            AND upper(trim(EDW1.instance_id)) = upper(trim(EDW3.instance_id  )) 
            AND EDW1.sequence_nbr = EDW3.sequence_nbr 
            AND upper(trim(EDW1.service_line_id)) = upper(trim(EDW3.service_line_id ))  
            AND upper(trim(EDW1.service_subline_id)) = upper(trim(EDW3.service_subline_id))   
            AND upper(trim(EDW2.level_element_id)) = upper(trim(EDW3.level_element_id ))  
            AND upper(trim(EDW2.instance_id)) = upper(trim(EDW3.instance_id));
