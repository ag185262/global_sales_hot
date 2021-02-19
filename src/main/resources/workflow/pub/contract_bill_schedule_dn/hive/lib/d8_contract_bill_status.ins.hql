----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:59 
--Script Name: d8_contract_bill_status.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${EEDDWWDDBB}  

--Translated Query: STATUS SUCCESS

    use ${EEDDWWDDBB} ;

--Original Query:
  --DELETE FROM contract_bill_status ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${EEDDWWDDBB}.contract_bill_status;

--Original Query:
  --LOCK TABLE contract_bill_sched_dn_ld FOR ACCESS  INSERT INTO contract_bill_status   ( 	 status_code 	,update_date_time   )  SELECT 	 COALESCE(status_code, ' ') 	,update_date_time  FROM contract_bill_sched_dn_ld GROUP BY 1,2  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB}.contract_bill_status           SELECT
            COALESCE( status_code ,
            ' ' ) AS auto_c00,
            update_date_time  
        FROM
            ${EEDDWWDDBB}.contract_bill_sched_dn_ld      
        GROUP BY
            COALESCE( status_code ,
            ' ' ) ,
            update_date_time;
