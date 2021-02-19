----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:55 
--Script Name: d8_contract_bill_sched_dn_46.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB}.contract_line_xref_ld FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t45 FOR ACCESS   INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46 (  instance_id ,contract_header_id ,service_line_id ,service_subline_id ,sline_line_style_id ,sline_covered_lvl_name ,sequence_nbr ,billing_stream_end_date ,billing_stream_start_date ,contr_bill_stream_id ,interface_offset_day_nbr ,period_uom_cnt ,period_uom_code ,sl_discount_amt_ent          ,sl_surcharge_amt_ent         ,sl_top_lvl_adj_prc_amt_ent   ,sl_top_lvl_oprnd_prc_amt_ent ,sl_top_lvl_pricing_uom_qty ,sl_unbilled_amt_ent     ,credit_amt_ent ,interface_date ,level_sequence_nbr ,period_end_date ,period_start_date ,bill_on_date ,invoice_type_code ,bill_amt_ent ,completed_date ,level_element_amt_ent ,invoice_currency_code ,invoice_date ,invoice_nbr ,line_credit_amt_ent  ,sline_credit_amt_ent ,install_location_id ,item_instance_id ) SELECT  EDW1.instance_id ,EDW1.contract_header_id ,EDW1.service_line_id ,EDW1.service_subline_id ,EDW1.sline_line_style_id ,EDW1.sline_covered_lvl_name ,EDW1.sequence_nbr ,EDW1.billing_stream_end_date ,EDW1.billing_stream_start_date ,EDW1.contr_bill_stream_id ,EDW1.interface_offset_day_nbr ,EDW1.period_uom_cnt ,EDW1.period_uom_code ,EDW1.sl_discount_amt_ent          ,EDW1.sl_surcharge_amt_ent         ,EDW1.sl_top_lvl_adj_prc_amt_ent   ,EDW1.sl_top_lvl_oprnd_prc_amt_ent ,EDW1.sl_top_lvl_pricing_uom_qty ,EDW1.sl_unbilled_amt_ent     ,EDW1.credit_amt_ent ,EDW1.interface_date ,EDW1.level_sequence_nbr ,EDW1.period_end_date ,EDW1.period_start_date ,EDW1.bill_on_date ,EDW1.invoice_type_code ,EDW1.bill_amt_ent ,EDW1.completed_date ,EDW1.level_element_amt_ent ,EDW1.invoice_currency_code ,EDW1.invoice_date ,EDW1.invoice_nbr ,EDW1.line_credit_amt_ent  ,EDW1.sline_credit_amt_ent <WM_COMMENT_START>install_location_id  <WM_COMMENT_END> ,CASE   WHEN EDW1.sline_line_style_id = 10       AND EDW2.object1_code = 'OKX_PARTYSITE'  THEN CAST(EDW2.object1_id1 as DECIMAL(18, 0))  END <WM_COMMENT_START> item_instance_id  <WM_COMMENT_END> ,CASE   WHEN EDW1.sline_line_style_id = 9       AND EDW2.object1_code = 'OKX_CUSTPROD'  THEN CAST(EDW2.object1_id1 as DECIMAL(18, 0))  END FROM  ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t45 EDW1 LEFT OUTER JOIN   ${EEDDWWDDBB}.contract_line_xref_ld EDW2  ON     EDW1.service_subline_id = EDW2.contract_line_id AND    EDW1.instance_id = EDW2.instance_id  

--Translated Query: STATUS SUCCESS (Added trim+upper on join)

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t46           SELECT
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
            CASE 
                WHEN EDW1.sline_line_style_id = 9  
              --AND upper(trim(EDW2.object1_code)) = upper(trim('OKX_CUSTPROD'))   THEN FORMAT_DECIMAL( EDW2.object1_id1 ,'decimal(18,0)' ) 
				AND upper(trim(EDW2.object1_code)) = upper(trim('OKX_CUSTPROD'))   THEN SUBSTR( ROUND(INT(EDW2.object1_id1)),1,18)				
            END AS auto_c035,
            CASE 
                WHEN EDW1.sline_line_style_id = 10  
              --  AND upper(trim(EDW2.object1_code)) = upper(trim('OKX_PARTYSITE'))   THEN FORMAT_DECIMAL( EDW2.object1_id1 ,'decimal(18,0)' )   
			    AND upper(trim(EDW2.object1_code)) = upper(trim('OKX_PARTYSITE'))   THEN SUBSTR( ROUND(INT(EDW2.object1_id1)),1,18)  
            END AS auto_c034  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t45    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB}.contract_line_xref_ld    AS EDW2    
                ON trim(upper(EDW1.service_subline_id)) = trim(upper(EDW2.contract_line_id))  
                AND trim(upper(EDW1.instance_id)) = trim(upper(EDW2.instance_id));
