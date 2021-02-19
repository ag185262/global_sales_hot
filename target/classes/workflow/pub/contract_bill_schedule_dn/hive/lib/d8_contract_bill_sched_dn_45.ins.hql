----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:55 
--Script Name: d8_contract_bill_sched_dn_45.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t45 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t45;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB}.contract_line_ld FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t45 (  instance_id ,contract_header_id ,service_line_id ,service_subline_id ,sline_line_style_id ,sline_covered_lvl_name ,sequence_nbr ,billing_stream_end_date ,billing_stream_start_date ,contr_bill_stream_id ,interface_offset_day_nbr ,period_uom_cnt ,period_uom_code ,sl_discount_amt_ent          ,sl_surcharge_amt_ent         ,sl_top_lvl_adj_prc_amt_ent   ,sl_top_lvl_oprnd_prc_amt_ent ,sl_top_lvl_pricing_uom_qty ,sl_unbilled_amt_ent     ,credit_amt_ent ,level_sequence_nbr ,period_end_date ,period_start_date ,bill_on_date ,interface_date ,invoice_type_code ,bill_amt_ent ,completed_date ,level_element_amt_ent ,invoice_currency_code ,invoice_date ,invoice_nbr ,line_credit_amt_ent  ,sline_credit_amt_ent    ) SELECT  EDW1.instance_id ,EDW1.contract_header_id ,EDW1.service_line_id ,EDW1.service_subline_id ,EDW1.sline_line_style_id ,EDW1.sline_covered_lvl_name ,EDW1.sequence_nbr ,EDW1.billing_stream_end_date ,EDW1.billing_stream_start_date ,EDW1.contr_bill_stream_id ,EDW1.interface_offset_day_nbr ,EDW1.period_uom_cnt ,EDW1.period_uom_code ,EDW1.sl_discount_amt_ent          ,EDW1.sl_surcharge_amt_ent         ,EDW1.sl_top_lvl_adj_prc_amt_ent   ,EDW1.sl_top_lvl_oprnd_prc_amt_ent ,EDW1.sl_top_lvl_pricing_uom_qty ,EDW1.sl_unbilled_amt_ent     ,EDW1.credit_amt_ent ,EDW1.level_sequence_nbr ,EDW1.period_end_date ,EDW1.period_start_date ,EDW1.bill_on_date ,EDW1.interface_date ,EDW1.invoice_type_code ,EDW1.bill_amt_ent ,EDW1.completed_date ,EDW1.level_element_amt_ent ,EDW1.invoice_currency_code ,EDW1.invoice_date ,EDW1.invoice_nbr <WM_COMMENT_START>line_credit_amt_ent<WM_COMMENT_END>  ,CASE WHEN EDW2.contract_line_style_id = 1  THEN EDW1.credit_amt_ent  ELSE 0  END  <WM_COMMENT_START>sline_credit_amt_ent <WM_COMMENT_END> ,CASE WHEN EDW2.contract_line_style_id in (9,10)  THEN EDW1.credit_amt_ent  ELSE 0  END FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 EDW1 LEFT OUTER JOIN (SELECT contract_line_style_id,contract_line_id, instance_id,tran_code FROM ${EEDDWWDDBB}.contract_line_ld group by 1,2,3,4)EDW2 ON    EDW1.service_subline_id = EDW2.contract_line_id AND   EDW1.instance_id        = EDW2.instance_id AND   EDW2.tran_code<>'D'  

--Translated Query: STATUS SUCCESS (Modified, Comment: Added upper+trim on join condition)

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t45           SELECT
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
            CASE 
                WHEN EDW2.contract_line_style_id = 1  THEN EDW1.credit_amt_ent  
                ELSE 0  
            END AS auto_c032,
            CASE 
                WHEN EDW2.contract_line_style_id  IN (  9,
                10  )  THEN EDW1.credit_amt_ent  
                ELSE 0  
            END AS auto_c033  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44    AS EDW1   
        LEFT OUTER  JOIN
            (
                SELECT
                    contract_line_style_id,
                    contract_line_id,
                    instance_id,
                    tran_code  
                FROM
                    ${EEDDWWDDBB}.contract_line_ld      
                GROUP BY
                    contract_line_style_id ,
                    contract_line_id ,
                    instance_id ,
                    tran_code      
            )    AS EDW2    
                ON UPPER(TRIM(EDW1.service_subline_id)) = UPPER(TRIM(EDW2.contract_line_id))  
                AND UPPER(TRIM(EDW1.instance_id)) = UPPER(TRIM(EDW2.instance_id))   
                AND UPPER(TRIM(EDW2.tran_code)) <> 'D';
