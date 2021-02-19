----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:39 
--Script Name: d8_contract_bill_group.ins.sql
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
  --DELETE FROM contract_bill_group ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${EEDDWWDDBB}.contract_bill_group;

--Original Query:
  --LOCK TABLE contract_bill_sched_dn_ld FOR ACCESS  INSERT INTO contract_bill_group   ( 	 contract_group_name 	,update_date_time   )  SELECT 	 COALESCE(contract_group_name, ' ') 	,update_date_time  FROM contract_bill_sched_dn_ld GROUP BY 1,2  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB}.contract_bill_group           SELECT
            COALESCE( contract_group_name ,
            ' ' ) AS auto_c00,
            update_date_time  
        FROM
            ${EEDDWWDDBB}.contract_bill_sched_dn_ld      
        GROUP BY
            COALESCE( contract_group_name ,
            ' ' ) ,
            update_date_time;
