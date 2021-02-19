----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:43 
--Script Name: d8_contract_bill_sched_dn_22.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 COLUMN (instance_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 COLUMN (service_line_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 COMPUTE STATISTICS  FOR COLUMNS service_line_id;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21 FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.contract_line_ld FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 (  contract_header_id ,instance_id ,service_line_id ,service_line_nbr ,service_line_style_id ,serviced_qty ) SELECT   EDW1.contract_header_id ,EDW1.instance_id ,EDW1.service_line_id <WM_COMMENT_START> service_line_nbr <WM_COMMENT_END> ,COALESCE(EDW2.contract_line_nbr, ' ') <WM_COMMENT_START> service_line_style_id <WM_COMMENT_END> ,COALESCE(EDW2.contract_line_style_id, 0) ,EDW2.serviced_qty  FROM       (SELECT contract_header_id,             service_line_id,             instance_id       FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21      GROUP BY 1,2,3     ) EDW1,     ${EEDDWWDDBB2}.contract_line_ld EDW2  WHERE EDW1.service_line_id = EDW2.contract_line_id AND   EDW1.instance_id = EDW2.instance_id AND   EDW2.tran_code <> 'D'  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
            EDW1.service_line_id,
            COALESCE( EDW2.contract_line_nbr ,
            ' ' ) AS auto_c03,
            COALESCE( EDW2.contract_line_style_id ,
            0 ) AS auto_c04,
            EDW2.serviced_qty  
        FROM
            (   SELECT
                contract_header_id,
                service_line_id,
                instance_id  
            FROM
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t21      
            GROUP BY
                contract_header_id ,
                service_line_id ,
                instance_id      )    AS EDW1   ,
            ${EEDDWWDDBB2}.contract_line_ld    AS EDW2   
        WHERE
            upper(trim(EDW1.service_line_id)) = upper(trim(EDW2.contract_line_id))  
            AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))   
            AND upper(trim(EDW2.tran_code)) <> upper(trim('D'));
