----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:57 
--Script Name: d8_contract_bill_sched_dn_52.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t52 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t52;

--Original Query:
  --LOCK TABLE ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn                 FOR ACCESS LOCK TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25  FOR ACCESS  INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t52 (  contract_header_id ,instance_id ,service_line_id ,bill_to_site_use_id ,bill_to_cust_indstry_code ,customer_nbr ,country_code ,bill_to_cust_sales_chnl_code )  SELECT   EDW1.contract_header_id ,EDW1.instance_id ,EDW1.service_line_id ,EDW1.bill_to_site_use_id ,EDW1.bill_to_cust_indstry_code ,EDW1.customer_nbr ,EDW1.country_code ,EDW2.cust_sales_channel_code  FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25 EDW1 LEFT OUTER JOIN      ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn  EDW2 ON   EDW1.instance_id         = EDW2.instance_id  AND  EDW1.bill_to_site_use_id = EDW2.cust_acct_site_use_id  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t52           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
            EDW1.service_line_id,
            EDW1.bill_to_site_use_id,
            EDW1.bill_to_cust_indstry_code,
            EDW2.cust_sales_channel_code,
            EDW1.customer_nbr,
            EDW1.country_code  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t25    AS EDW1   
        LEFT OUTER  JOIN
            ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn    AS EDW2    
                ON upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))  
                AND EDW1.bill_to_site_use_id = EDW2.cust_acct_site_use_id;
