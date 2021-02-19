----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:58 
--Script Name: d8_contract_bill_sched_dn_56.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB1}.geo_rollup_xref               FOR ACCESS LOCK TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t55 FOR ACCESS  INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56 (  contract_header_id ,instance_id ,service_line_id ,bill_to_cust_indstry_code ,bill_to_cust_sales_chnl_code ,bill_to_pnt_cust_ind_code ,bill_to_pnt_cust_ind_name ,bill_to_site_use_id ,country_code ,customer_nbr ,naics_code ,naics_desc ,sub_region_name ,sub_region_code )  SELECT   EDW1.contract_header_id ,EDW1.instance_id ,EDW1.service_line_id ,EDW1.bill_to_cust_indstry_code ,EDW1.bill_to_cust_sales_chnl_code ,EDW1.bill_to_pnt_cust_ind_code ,EDW1.bill_to_pnt_cust_ind_name ,EDW1.bill_to_site_use_id ,EDW1.country_code ,EDW1.customer_nbr ,EDW1.naics_code ,EDW1.naics_desc ,EDW2.sub_region_name ,EDW2.sub_region_code  FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t55 EDW1 LEFT OUTER JOIN      ${EEDDWWDDBB1}.geo_rollup_xref EDW2 ON   EDW1.country_code = EDW2.country_code AND  EDW2.business_unit_code = 'B4'  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t56           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
            EDW1.service_line_id,
            EDW1.bill_to_cust_indstry_code,
            EDW1.bill_to_cust_sales_chnl_code,
            EDW1.bill_to_pnt_cust_ind_code,
            EDW1.bill_to_pnt_cust_ind_name,
            EDW1.bill_to_site_use_id,
            EDW1.country_code,
            EDW1.customer_nbr,
            EDW1.naics_code,
            EDW1.naics_desc,
            EDW2.sub_region_code,
            EDW2.sub_region_name  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t55    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB1}.geo_rollup_xref    AS EDW2    
                ON upper(trim(EDW1.country_code)) = upper(trim(EDW2.country_code )) 
                AND upper(trim(EDW2.business_unit_code)) = upper(trim('B4'));
