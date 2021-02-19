----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:42 
--Script Name: d8_contract_bill_sched_dn_21.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB2}.contract_line_ld FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 (  contract_header_id ,instance_id ,service_line_id ,service_subline_id ,service_subline_end_date ,service_subline_nbr ,service_subline_start_date ,service_subline_term_date ,service_subline_status_code ,sline_line_style_id ,serviced_qty   ) SELECT  EDW1.contract_header_id ,EDW1.instance_id <WM_COMMENT_START> service_line_id <WM_COMMENT_END> ,CL.parent_contract_line_id <WM_COMMENT_START> service_subline_id <WM_COMMENT_END> ,CL.contract_line_id <WM_COMMENT_START> service_subline_end_date <WM_COMMENT_END> ,CAST(CL.end_date_time AS DATE) <WM_COMMENT_START> service_subline_nbr <WM_COMMENT_END> ,COALESCE(CL.contract_line_nbr, ' ') <WM_COMMENT_START> service_subline_start_date <WM_COMMENT_END> ,CAST(CL.start_date_time AS DATE) <WM_COMMENT_START> service_subline_term_date <WM_COMMENT_END> ,CAST(CL.termination_date_time AS DATE) <WM_COMMENT_START> service_subline_status_code <WM_COMMENT_END> ,CL.status_code <WM_COMMENT_START> sline_line_style_id <WM_COMMENT_END> ,CL.contract_line_style_id ,CL.serviced_qty  FROM    ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1 EDW1,         ${EEDDWWDDBB2}.contract_line_ld CL WHERE 	EDW1.contract_header_id = CL.alt_contract_header_id AND	EDW1.instance_id = CL.instance_id AND	CL.contract_line_style_id IN (9,10) AND     CL.tran_code <> 'D'  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
            CL.parent_contract_line_id,
            CL.contract_line_id,
            CAST (CL.end_date_time AS date) AS auto_c04,
            COALESCE( CL.contract_line_nbr ,
            ' ' ) AS auto_c05,
            CAST (CL.start_date_time AS date) AS auto_c06,
            CAST (CL.termination_date_time AS date) AS auto_c07,
            CL.status_code,
            CL.contract_line_style_id,
            CL.serviced_qty  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1    AS EDW1   ,
            ${EEDDWWDDBB2}.contract_line_ld    AS CL   
        WHERE
            EDW1.contract_header_id = CL.alt_contract_header_id  
            AND EDW1.instance_id = CL.instance_id   
            AND CL.contract_line_style_id  IN (
                9,10  
            )   
            AND upper(trim(CL.tran_code)) <> upper(trim('D'))   
            AND CL.contract_line_style_id  IN (
                9,10  
            );
