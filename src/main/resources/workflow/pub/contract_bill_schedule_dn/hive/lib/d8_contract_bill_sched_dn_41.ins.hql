----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:48 
--Script Name: d8_contract_bill_sched_dn_41.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 column (instance_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 column (service_subline_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 COMPUTE STATISTICS  FOR COLUMNS service_subline_id;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 column (sline_line_style_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 COMPUTE STATISTICS  FOR COLUMNS sline_line_style_id;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 FOR ACCESS LOCK TABLE ${EEDDWWDDBB}.contract_line_style_lk_ld FOR ACCESS  CREATE VOLATILE table EDW1 AS (SELECT                       V1.contract_header_id                                     ,V1.instance_id                                            ,V1.service_line_id                                        ,V1.service_subline_id                                        ,V1.sline_line_style_id                                  ,V2.line_style_name                                        FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 V1           LEFT OUTER JOIN                                                 ${EEDDWWDDBB}.contract_line_style_lk_ld V2                   ON     V1.instance_id = V2.instance_id                         AND    V1.sline_line_style_id = V2.contract_line_style_id   WHERE  V2.language_code = 'US'                                AND    V2.tran_code <> 'D') WITH DATA                         PRIMARY INDEX (service_subline_id, instance_id)  ON COMMIT preserve rows 

--Translated Query: STATUS SUCCESS

	DROP TABLE IF EXISTS  ${TTMMPPDDBB1}.EDW1;

    CREATE  TABLE IF NOT EXISTS  ${TTMMPPDDBB1}.EDW1 STORED AS ORC        AS     SELECT
        V1.contract_header_id,
        V1.instance_id,
        V1.service_line_id,
        V1.service_subline_id,
        V1.sline_line_style_id,
        V2.line_style_name  
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21    AS V1   
    LEFT OUTER  JOIN
        ${EEDDWWDDBB}.contract_line_style_lk_ld    AS V2    
            ON V1.instance_id = V2.instance_id  
            AND V1.sline_line_style_id = V2.contract_line_style_id    
    WHERE
        upper(trim(V2.language_code)) = upper(trim('US'))  
        AND upper(trim(V2.tran_code)) <> upper(trim('D'));

--Original Query:
  --INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41 (  instance_id ,contract_header_id ,service_line_id ,service_subline_id ,sline_line_style_id ,sline_covered_lvl_name ,sequence_nbr ,billing_stream_end_date ,billing_stream_start_date ,contr_bill_stream_id ,interface_offset_day_nbr ,period_uom_cnt ,period_uom_code ) SELECT	  V3.instance_id ,V3.contract_header_id ,V3.service_line_id ,V3.service_subline_id ,V3.sline_line_style_id ,V3.line_style_name <WM_COMMENT_START> sequence_nbr <WM_COMMENT_END> ,EDW3.billing_seq_nbr <WM_COMMENT_START> billing_stream_end_date <WM_COMMENT_END> ,EDW3.billing_stream_end_date <WM_COMMENT_START> billing_stream_start_date <WM_COMMENT_END> ,EDW3.billing_stream_start_date <WM_COMMENT_START> contr_bill_stream_id <WM_COMMENT_END> ,EDW3.contr_bill_stream_id <WM_COMMENT_START> interface_offset_day_nbr <WM_COMMENT_END> ,EDW3.interface_offset_day_nbr <WM_COMMENT_START> period_uom_cnt <WM_COMMENT_END> ,EDW3.billing_period_uom_cnt <WM_COMMENT_START> period_uom_code <WM_COMMENT_END> ,EDW3.billing_period_uom_code FROM	EDW1 V3 LEFT OUTER JOIN      (SELECT  contract_line_id              ,instance_id              ,billing_seq_nbr              ,billing_stream_end_date              ,billing_stream_start_date              ,contr_bill_stream_id              ,interface_offset_day_nbr              ,billing_period_uom_cnt              ,billing_period_uom_code FROM ${EEDDWWDDBB}.contr_bill_stream_ld CBS       WHERE tran_code <> 'D'     AND (CBS.instance_id, CBS.contract_line_id,CBS.billing_seq_nbr,CBS.contr_bill_stream_id)         IN         (SELECT instance_id, contract_line_id,billing_seq_nbr,MAX(contr_bill_stream_id)         FROM ${EEDDWWDDBB}.contr_bill_stream_ld WHERE tran_code <> 'D'         GROUP BY 1,2,3)   ) EDW3 ON   V3.service_subline_id = EDW3.contract_line_id AND  V3.instance_id        = EDW3.instance_id  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t41           SELECT
            V3.contract_header_id,
            V3.instance_id,
            V3.service_line_id,
            V3.service_subline_id,
            V3.sline_line_style_id,
            V3.line_style_name,
            EDW3.billing_seq_nbr,
            EDW3.billing_stream_end_date,
            EDW3.billing_stream_start_date,
            EDW3.contr_bill_stream_id,
            EDW3.interface_offset_day_nbr,
            EDW3.billing_period_uom_cnt,
            EDW3.billing_period_uom_code  
        FROM
            ${TTMMPPDDBB1}.EDW1    AS V3   
        LEFT OUTER  JOIN
            (
                SELECT
                    contract_line_id,
                    instance_id,
                    billing_seq_nbr,
                    billing_stream_end_date,
                    billing_stream_start_date,
                    contr_bill_stream_id,
                    interface_offset_day_nbr,
                    billing_period_uom_cnt,
                    billing_period_uom_code  
                FROM
                    ${EEDDWWDDBB}.contr_bill_stream_ld    AS CBS   
                WHERE
                    upper(trim(tran_code)) <> upper(trim('D'))        
            )    AS EDW3    
                ON upper(trim( V3.service_subline_id)) = upper(trim( EDW3.contract_line_id )) 
                AND upper(trim( V3.instance_id)) = upper(trim( EDW3.instance_id));
