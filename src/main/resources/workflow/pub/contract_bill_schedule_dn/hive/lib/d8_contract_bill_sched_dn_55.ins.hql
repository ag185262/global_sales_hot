----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:58 
--Script Name: d8_contract_bill_sched_dn_55.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t55 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t55;

--Original Query:
  --DELETE from ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp  

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB1}.customer_dn FOR ACCESS  INSERT INTO  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp (naics_code, naics_desc, customer_nbr, oper_unit_country_code, data_source_code, eff_start_date_time, eff_end_date_time) SELECT naics_code, naics_desc, customer_nbr, oper_unit_country_code, data_source_code, eff_start_date_time, eff_end_date_time FROM ${EEDDWWDDBB1}.customer_dn WHERE data_source_code = 'ERP' AND current_timestamp BETWEEN eff_start_date_time AND eff_end_date_time 

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp           SELECT
            customer_nbr,
            data_source_code,
            oper_unit_country_code,
            eff_start_date_time,
            eff_end_date_time,
            naics_code,
            naics_desc  
        FROM
            ${EEDDWWDDBB1}.customer_dn     
        WHERE
            upper(trim(data_source_code)) = upper(trim('ERP'))  
            AND  (
                CURRENT_TIMESTAMP() BETWEEN eff_start_date_time AND eff_end_date_time 
            );

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COLUMN contract_header_id 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS contract_header_id;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COLUMN instance_id 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COLUMN service_line_id 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS service_line_id;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COLUMN country_code 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS country_code;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COLUMN customer_nbr 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS customer_nbr;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COLUMN customer_nbr 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS customer_nbr;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp COLUMN customer_nbr 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp COMPUTE STATISTICS  FOR COLUMNS customer_nbr;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp COLUMN data_source_code 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp COMPUTE STATISTICS  FOR COLUMNS data_source_code;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp COLUMN oper_unit_country_code 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp COMPUTE STATISTICS  FOR COLUMNS oper_unit_country_code;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp COLUMN eff_start_date_time 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp COMPUTE STATISTICS  FOR COLUMNS eff_start_date_time;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp COLUMN eff_end_date_time 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp COMPUTE STATISTICS  FOR COLUMNS eff_end_date_time;

--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp   FOR ACCESS LOCK TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 FOR ACCESS  INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t55 (  contract_header_id ,instance_id ,service_line_id ,bill_to_cust_indstry_code ,bill_to_cust_sales_chnl_code ,bill_to_pnt_cust_ind_code ,bill_to_pnt_cust_ind_name ,bill_to_site_use_id ,country_code ,customer_nbr ,naics_code ,naics_desc )  SELECT   EDW1.contract_header_id ,EDW1.instance_id ,EDW1.service_line_id ,EDW1.bill_to_cust_indstry_code ,EDW1.bill_to_cust_sales_chnl_code ,EDW1.bill_to_pnt_cust_ind_code ,EDW1.bill_to_pnt_cust_ind_name ,EDW1.bill_to_site_use_id ,EDW1.country_code ,EDW1.customer_nbr ,EDW2.naics_code ,EDW2.naics_desc   FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54 EDW1 LEFT OUTER JOIN ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp EDW2 ON   EDW1.customer_nbr = EDW2.customer_nbr  AND  EDW1.country_code= EDW2.oper_unit_country_code   

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t55           SELECT
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
            EDW2.naics_code,
            EDW2.naics_desc  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t54    AS EDW1   
        LEFT OUTER  JOIN
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_customer_dn_tmp    AS EDW2    
                ON upper(trim( EDW1.customer_nbr)) = upper(trim( EDW2.customer_nbr))  
                AND upper(trim( EDW1.country_code)) = upper(trim( EDW2.oper_unit_country_code));
