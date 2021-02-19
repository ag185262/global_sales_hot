----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:50 
--Script Name: d8_contract_bill_sched_dn_42.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42;

--Original Query:
  --COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41 COLUMN(instance_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41 COLUMN(service_subline_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41 COMPUTE STATISTICS  FOR COLUMNS service_subline_id;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB}.contract_line_svc_supp_ld FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42   (  instance_id ,contract_header_id ,service_line_id ,service_subline_id ,sline_line_style_id ,sline_covered_lvl_name ,sequence_nbr ,billing_stream_end_date ,billing_stream_start_date ,contr_bill_stream_id ,interface_offset_day_nbr ,period_uom_cnt ,period_uom_code ,sl_discount_amt_ent          ,sl_surcharge_amt_ent         ,sl_top_lvl_adj_prc_amt_ent   ,sl_top_lvl_oprnd_prc_amt_ent ,sl_top_lvl_pricing_uom_qty ,sl_unbilled_amt_ent     ,credit_amt_ent         ) SELECT  EDW1.instance_id ,EDW1.contract_header_id ,EDW1.service_line_id ,EDW1.service_subline_id ,EDW1.sline_line_style_id ,EDW1.sline_covered_lvl_name  ,EDW1.sequence_nbr  ,EDW1.billing_stream_end_date ,EDW1.billing_stream_start_date ,EDW1.contr_bill_stream_id ,EDW1.interface_offset_day_nbr ,EDW1.period_uom_cnt  ,EDW1.period_uom_code <WM_COMMENT_START> sl_discount_amt_ent <WM_COMMENT_END> ,CASE WHEN        (EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent) > 0       THEN (EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent)       ELSE 0.0  END <WM_COMMENT_START> sl_surcharge_amt_ent <WM_COMMENT_END>         ,CASE WHEN        (EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent) < 0       THEN (EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent)       ELSE 0.0  END <WM_COMMENT_START> sl_top_lvl_adj_prc_amt_ent <WM_COMMENT_END>   ,EDW2.top_lvl_adj_prc_amt_ent <WM_COMMENT_START> sl_top_lvl_oprnd_prc_amt_ent <WM_COMMENT_END> ,EDW2.top_lvl_oprnd_prc_amt_ent <WM_COMMENT_START> sl_top_lvl_pricing_uom_qty <WM_COMMENT_END> ,EDW2.top_lvl_pricing_uom_qty <WM_COMMENT_START> sl_unbilled_amt_ent <WM_COMMENT_END>     ,EDW2.unbilled_amt_ent <WM_COMMENT_START> credit_amt_ent <WM_COMMENT_END> ,EDW2.credit_amt_ent FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41 EDW1 LEFT OUTER JOIN                                                 (  SELECT    a.contract_line_id  ,a.instance_id  ,a.top_lvl_oprnd_prc_amt_ent  ,a.top_lvl_adj_prc_amt_ent  ,a.top_lvl_pricing_uom_qty  ,a.unbilled_amt_ent  ,a.credit_amt_ent  FROM (SELECT contract_line_id                                          ,instance_id                                                  ,top_lvl_oprnd_prc_amt_ent                                    ,top_lvl_adj_prc_amt_ent                                      ,top_lvl_pricing_uom_qty                                      ,unbilled_amt_ent                                             ,credit_amt_ent                                               FROM ${EEDDWWDDBB}.contract_line_svc_supp_ld        WHERE tran_code <> 'D'        AND contract_line_id <> ' ' ) a  JOIN         (SELECT service_subline_id                                        ,instance_id                                         FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41             GROUP BY 1,2) b                                      ON  a.contract_line_id = b.service_subline_id                  AND a.instance_id = b.instance_id ) EDW2 ON  EDW1.service_subline_id = EDW2.contract_line_id             AND EDW1.instance_id = EDW2.instance_id                          

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42           SELECT
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
            CASE 
                WHEN ( EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent ) > 0  THEN ( EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent )  
                ELSE 0.0  
            END AS auto_c013,
            CASE 
                WHEN ( EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent ) < 0  THEN ( EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent )  
                ELSE 0.0  
            END AS auto_c014,
            EDW2.top_lvl_adj_prc_amt_ent,
            EDW2.top_lvl_oprnd_prc_amt_ent,
            EDW2.top_lvl_pricing_uom_qty,
            EDW2.unbilled_amt_ent,
            EDW2.credit_amt_ent  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41    AS EDW1   
        LEFT OUTER  JOIN
            (
                SELECT
                    a.contract_line_id,
                    a.instance_id,
                    a.top_lvl_oprnd_prc_amt_ent,
                    a.top_lvl_adj_prc_amt_ent,
                    a.top_lvl_pricing_uom_qty,
                    a.unbilled_amt_ent,
                    a.credit_amt_ent  
                FROM
                    (   SELECT
                        contract_line_id,
                        instance_id,
                        top_lvl_oprnd_prc_amt_ent,
                        top_lvl_adj_prc_amt_ent,
                        top_lvl_pricing_uom_qty,
                        unbilled_amt_ent,
                        credit_amt_ent  
                    FROM
                        ${EEDDWWDDBB}.contract_line_svc_supp_ld     
                    WHERE
                        upper(trim(tran_code)) <> upper(trim('D'))  
                        AND upper(trim(contract_line_id)) <> upper(trim(' '))         )    AS a    
                JOIN
                    (
                        SELECT
                            service_subline_id,
                            instance_id  
                        FROM
                            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41      
                        GROUP BY
                            service_subline_id ,
                            instance_id      
                    )    AS b    
                        ON upper(trim(a.contract_line_id)) = upper(trim(b.service_subline_id))  
                        AND upper(trim(a.instance_id)) = upper(trim(b.instance_id))           
                    )    AS EDW2    
                        ON upper(trim(EDW1.service_subline_id)) = upper(trim(EDW2.contract_line_id))  
                        AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id));
