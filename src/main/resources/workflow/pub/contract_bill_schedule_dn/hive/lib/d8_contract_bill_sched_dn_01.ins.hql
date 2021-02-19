----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:41 
--Script Name: d8_contract_bill_sched_dn_01.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB2}.contract_header_ld FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1   (  contract_header_id ,instance_id ,author_oper_unit_country_code ,bill_to_site_use_id ,contract_end_date ,contract_est_amt_ent ,contract_nbr ,contract_nbr_modifier ,contract_start_date ,curr_code ,cust_po_nbr ,invoice_rule_id ,migration_contract_nbr ,renew_date ,renewal_type_code ,solution_portfolio_id ,status_code ,tran_code   ) SELECT  EDW1.contract_header_id ,EDW1.instance_id ,EDW1.author_oper_unit_country_code ,EDW1.bill_to_site_use_id <WM_COMMENT_START> contract_end_date <WM_COMMENT_END> ,CAST(EDW1.end_date_time AS DATE) ,EDW1.contract_est_amt_ent ,EDW1.contract_nbr ,EDW1.contract_nbr_modifier <WM_COMMENT_START> contract_start_date <WM_COMMENT_END> ,CAST(EDW1.start_date_time AS DATE) ,EDW1.curr_code ,EDW1.cust_po_nbr ,EDW1.invoice_rule_id ,EDW1.migration_contract_nbr <WM_COMMENT_START> renew_date <WM_COMMENT_END> ,CAST(EDW1.renew_date_time AS DATE) ,EDW1.renewal_type_code ,EDW1.solution_portfolio_id ,EDW1.status_code ,EDW1.tran_code FROM ${EEDDWWDDBB2}.contract_header_ld EDW1 WHERE EDW1.tran_code <> 'D' AND EDW1.subclass_code = 'SERVICE' AND EDW1.status_code IN (   'ACTIVE',    'EXPIRED',   'HOLD',   'QA_HOLD',   'SIGNED',   'TERMINATED' )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t1           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
            EDW1.author_oper_unit_country_code,
            EDW1.bill_to_site_use_id,
            CAST (EDW1.end_date_time AS date) AS auto_c04,
            EDW1.contract_est_amt_ent,
            EDW1.contract_nbr,
            EDW1.contract_nbr_modifier,
            CAST (EDW1.start_date_time AS date) AS auto_c08,
            EDW1.curr_code,
            EDW1.cust_po_nbr,
            EDW1.invoice_rule_id,
            EDW1.migration_contract_nbr,
            CAST (EDW1.renew_date_time AS date) AS auto_c013,
            EDW1.renewal_type_code,
            EDW1.solution_portfolio_id,
            EDW1.status_code,
            EDW1.tran_code  
        FROM
            ${EEDDWWDDBB2}.contract_header_ld    AS EDW1   
        WHERE
            upper(trim(EDW1.tran_code)) <> upper(trim('D'))  
            AND upper(trim(EDW1.subclass_code)) = upper(trim('SERVICE'))   
            AND upper(trim(EDW1.status_code))  IN (
                upper(trim('ACTIVE')) , upper(trim('EXPIRED')) , upper(trim('HOLD')) , upper(trim('QA_HOLD')) , upper(trim('SIGNED')) , upper(trim('TERMINATED'))  
            )   
            AND upper(trim(EDW1.status_code))  IN (
                upper(trim('ACTIVE')) , upper(trim('EXPIRED')) , upper(trim('HOLD')) , upper(trim('QA_HOLD')) , upper(trim('SIGNED')) , upper(trim('TERMINATED'))  
            );
