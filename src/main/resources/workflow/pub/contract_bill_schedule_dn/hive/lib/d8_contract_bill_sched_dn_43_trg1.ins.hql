----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:50 
--Script Name: d8_contract_bill_sched_dn_43_trg1.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t43_trg1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t43_trg1;

--Original Query:
  --COLLECT STATISTICS    COLUMN (contr_bill_stream_id) , COLUMN (instance_id)  , COLUMN (instance_id , contr_bill_stream_id)  , COLUMN (CONTRACT_HEADER_ID ,INSTANCE_ID ,SERVICE_LINE_ID ,SERVICE_SUBLINE_ID ,SEQUENCE_NBR) ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42 COMPUTE STATISTICS  FOR COLUMNS contr_bill_stream_id  , instance_id  , instance_id,contr_bill_stream_id  , CONTRACT_HEADER_ID,INSTANCE_ID,SERVICE_LINE_ID,SERVICE_SUBLINE_ID,SEQUENCE_NBR;

--Original Query:
  --COLLECT STATISTICS    COLUMN (LEVEL_ELEMENT_AMT_ENT) , COLUMN (INTERFACE_DATE_TIME ,PERIOD_START_DATE ,SEQUENCE_NBR, PERIOD_END_DATE) , COLUMN (TRAN_CODE) ON ${EEDDWWDDBB2}.contract_lvl_element 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB2}.contract_lvl_element COMPUTE STATISTICS  FOR COLUMNS LEVEL_ELEMENT_AMT_ENT  , INTERFACE_DATE_TIME,PERIOD_START_DATE,SEQUENCE_NBR,PERIOD_END_DATE  , TRAN_CODE;

--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB2}.contract_lvl_element_ld from ${EEDDWWDDBB2}.contract_lvl_element 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB2}.contract_lvl_element_ld COMPUTE STATISTICS;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42 FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.contract_lvl_element_ld FOR ACCESS   INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t43_trg1 (  contract_header_id ,instance_id ,interface_date ,level_sequence_nbr ,period_end_date ,period_start_date ,sequence_nbr ,service_line_id ,service_subline_id ,level_element_id  ) SELECT  EDW1.contract_header_id  ,EDW1.instance_id  ,EDW2.interface_date_time ,EDW2.sequence_nbr ,EDW2.period_end_date  ,EDW2.period_start_date  ,EDW1.sequence_nbr  ,EDW1.service_line_id  ,EDW1.service_subline_id ,MIN(EDW2.level_element_id) FROM 	 ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42 EDW1, ${EEDDWWDDBB2}.contract_lvl_element_ld EDW2  WHERE    EDW1.contr_bill_stream_id = EDW2.rule_id AND      EDW1.instance_id = EDW2.instance_id AND      EDW1.contr_bill_stream_id IS NOT NULL AND      EDW2.level_element_amt_ent <> '0' AND      EDW2.tran_code <> 'D' AND      EDW2.rule_id <> ' ' GROUP BY 1,2,3,4,5,6,7,8,9  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t43_trg1           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
            EDW2.interface_date_time,
            EDW2.sequence_nbr,
            EDW2.period_end_date,
            EDW2.period_start_date,
            EDW1.sequence_nbr,
            EDW1.service_line_id,
            EDW1.service_subline_id,
            MIN( EDW2.level_element_id ) AS auto_c09  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t42    AS EDW1   ,
            ${EEDDWWDDBB2}.contract_lvl_element_ld    AS EDW2   
        WHERE
            upper(trim(EDW1.contr_bill_stream_id)) = upper(trim(EDW2.rule_id))  
            AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))   
            AND EDW1.contr_bill_stream_id  IS NOT NULL   
            AND EDW2.level_element_amt_ent <> '0'   
            AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))   
            AND upper(trim(EDW2.rule_id)) <> upper(trim(' '))   
        GROUP BY
            EDW1.contract_header_id ,
            EDW1.instance_id ,
            EDW2.interface_date_time ,
            EDW2.sequence_nbr ,
            EDW2.period_end_date ,
            EDW2.period_start_date ,
            EDW1.sequence_nbr ,
            EDW1.service_line_id ,
            EDW1.service_subline_id;
