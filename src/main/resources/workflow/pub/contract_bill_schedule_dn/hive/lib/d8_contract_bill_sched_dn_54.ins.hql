----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:57 
--Script Name: d8_contract_bill_sched_dn_54.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --COLLECT STATISTICS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54  COLUMN (COUNTRY_CODE ,CUSTOMER_NBR) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS COUNTRY_CODE,CUSTOMER_NBR;

--Original Query:
  --DELETE FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB1}.parent_cust_industry            FOR ACCESS LOCK TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t53 FOR ACCESS  INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 (  contract_header_id ,instance_id ,service_line_id ,bill_to_cust_indstry_code ,bill_to_cust_sales_chnl_code ,bill_to_pnt_cust_ind_code ,bill_to_site_use_id ,country_code ,customer_nbr ,bill_to_pnt_cust_ind_name )  SELECT   EDW1.contract_header_id ,EDW1.instance_id ,EDW1.service_line_id ,EDW1.bill_to_cust_indstry_code ,EDW1.bill_to_cust_sales_chnl_code ,EDW1.bill_to_pnt_cust_ind_code ,EDW1.bill_to_site_use_id ,EDW1.country_code ,EDW1.customer_nbr ,EDW2.parent_cust_ind_name  FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t53 EDW1 LEFT OUTER JOIN      ${EEDDWWDDBB1}.parent_cust_industry        EDW2 ON   EDW1.bill_to_pnt_cust_ind_code = EDW2.parent_cust_ind_code  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
            EDW1.service_line_id,
            EDW1.bill_to_cust_indstry_code,
            EDW1.bill_to_cust_sales_chnl_code,
            EDW1.bill_to_site_use_id,
            EDW1.bill_to_pnt_cust_ind_code,
            EDW2.parent_cust_ind_name,
            EDW1.country_code,
            EDW1.customer_nbr  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t53    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB1}.parent_cust_industry    AS EDW2    
                ON upper(trim( EDW1.bill_to_pnt_cust_ind_code)) = upper(trim( EDW2.parent_cust_ind_code));
