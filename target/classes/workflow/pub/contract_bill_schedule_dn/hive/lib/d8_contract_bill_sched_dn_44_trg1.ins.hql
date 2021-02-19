----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:53 
--Script Name: d8_contract_bill_sched_dn_44_trg1.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${EEDDWWDDBB2}  

--Translated Query: STATUS SUCCESS

    use ${EEDDWWDDBB2} ;

--Original Query:
  --DELETE FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1;

--Original Query:
  --LOCK TABLE contract_bill_invc_line_ld FOR ACCESS LOCK TABLE contract_line_bill_ld FOR ACCESS LOCK TABLE contract_subline_bill_ld FOR ACCESS  INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1 (  instance_id ,bill_instance_nbr ,creation_date ,service_subline_id ,bill_from_date ,bill_to_date ,bill_amt_ent  ) SELECT  EDW1.instance_id ,EDW1.bill_instance_nbr ,CAST(EDW2.creation_date_time AS DATE) ,EDW3.contract_line_id ,CAST(EDW3.bill_from_date_time AS DATE) ,CAST(EDW3.bill_to_date_time AS DATE) ,EDW3.bill_amt_ent  FROM    contract_bill_invc_line_ld EDW1,         contract_line_bill_ld EDW2,         contract_subline_bill_ld EDW3	 WHERE	EDW1.instance_id = EDW2.instance_id AND	EDW1.contract_line_bill_id = EDW2.contract_line_bill_id AND	EDW2.instance_id = EDW3.instance_id AND	EDW2.contract_line_bill_id = EDW3.contract_line_bill_id AND	EDW1.tran_code <> 'D' AND	EDW2.tran_code <> 'D' AND	EDW3.tran_code <> 'D' GROUP BY 1,2,3,4,5,6,7  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1           SELECT
            EDW1.instance_id,
            EDW1.bill_instance_nbr,
            CAST (EDW2.creation_date_time AS date) AS auto_c02,
            EDW3.contract_line_id,
            CAST (EDW3.bill_from_date_time AS date) AS auto_c04,
            CAST (EDW3.bill_to_date_time AS date) AS auto_c05,
            EDW3.bill_amt_ent  
        FROM
            ${EEDDWWDDBB2}.contract_bill_invc_line_ld    AS EDW1   ,
            ${EEDDWWDDBB2}.contract_line_bill_ld    AS EDW2   ,
            ${EEDDWWDDBB2}.contract_subline_bill_ld    AS EDW3   
        WHERE
            upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))  
            AND upper(trim(EDW1.contract_line_bill_id)) = upper(trim(EDW2.contract_line_bill_id))   
            AND upper(trim(EDW2.instance_id)) = upper(trim(EDW3.instance_id))   
            AND upper(trim(EDW2.contract_line_bill_id)) = upper(trim(EDW3.contract_line_bill_id))   
            AND upper(trim(EDW1.tran_code)) <> upper(trim('D'))   
            AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))   
            AND upper(trim(EDW3.tran_code)) <> upper(trim('D'))   
        GROUP BY
            EDW1.instance_id ,
            EDW1.bill_instance_nbr ,
            CAST (EDW2.creation_date_time AS date) ,
            EDW3.contract_line_id ,
            CAST (EDW3.bill_from_date_time AS date) ,
            CAST (EDW3.bill_to_date_time AS date) ,
            EDW3.bill_amt_ent;

--Original Query:
  --COLLECT STATISTICS ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1 COLUMN (INSTANCE_ID ,BILL_INSTANCE_NBR) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1 COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,BILL_INSTANCE_NBR;

--Original Query:
  --COLLECT STATISTICS ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1 COLUMN (INSTANCE_ID ,BILL_INSTANCE_NBR ,CREATION_DATE ,SERVICE_SUBLINE_ID ,BILL_FROM_DATE ,BILL_TO_DATE) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1 COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,BILL_INSTANCE_NBR,CREATION_DATE,SERVICE_SUBLINE_ID,BILL_FROM_DATE,BILL_TO_DATE;
