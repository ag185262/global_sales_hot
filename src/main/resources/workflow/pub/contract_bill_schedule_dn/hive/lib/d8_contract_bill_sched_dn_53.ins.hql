----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:57 
--Script Name: d8_contract_bill_sched_dn_53.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t53 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t53; 

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB1}.cust_industry_supp            FOR ACCESS LOCK TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t52 FOR ACCESS  INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t53 (  contract_header_id ,instance_id ,service_line_id ,bill_to_site_use_id ,bill_to_cust_indstry_code ,bill_to_cust_sales_chnl_code ,country_code ,customer_nbr ,bill_to_pnt_cust_ind_code )  SELECT   EDW1.contract_header_id ,EDW1.instance_id ,EDW1.service_line_id ,EDW1.bill_to_site_use_id ,EDW1.bill_to_cust_indstry_code ,EDW1.bill_to_cust_sales_chnl_code ,EDW1.country_code ,EDW1.customer_nbr ,EDW2.parent_cust_ind_code  FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t52 EDW1 LEFT OUTER JOIN      ${EEDDWWDDBB1}.cust_industry_supp       EDW2 ON   EDW1.bill_to_cust_indstry_code = EDW2.cust_industry_code  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t53           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
            EDW1.service_line_id,
            EDW1.bill_to_site_use_id,
            EDW1.bill_to_cust_indstry_code,
            EDW1.bill_to_cust_sales_chnl_code,
            EDW2.parent_cust_ind_code,
            EDW1.customer_nbr,
            EDW1.country_code  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t52    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB1}.cust_industry_supp    AS EDW2    
                ON upper(trim( EDW1.bill_to_cust_indstry_code)) = upper(trim( EDW2.cust_industry_code));
