----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:26 
--Script Name: d8_contract_lvl_element_03.ins.sql
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
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t3 ( 	instance_id, 	level_element_id, 	amount_due_date_time, 	completed_date_time, 	created_by_name, 	creation_date_time, 	gl_receivable_date_time, 	interface_date_time, 	last_update_date_time, 	last_updated_by_name, 	level_element_amt_ent, 	period_start_date, 	print_date_time, 	rule_id, 	rule_start_date_time, 	sequence_nbr, 	tran_code, 	transaction_date_time, 	alt_contract_header_id, 	contract_line_id, 	parent_contract_line_id, 	period_end_date ) SELECT 	instance_id, 	level_element_id, 	amount_due_date_time, 	completed_date_time, 	created_by_name, 	creation_date_time, 	gl_receivable_date_time, 	interface_date_time, 	last_update_date_time, 	last_updated_by_name, 	level_element_amt_ent, 	period_start_date, 	print_date_time, 	rule_id, 	rule_start_date_time, 	sequence_nbr, 	tran_code, 	transaction_date_time, 	alt_contract_header_id, 	contract_line_id, 	parent_contract_line_id, 	period_end_date FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 WHERE (level_element_id, instance_id) IN (SELECT      level_element_id,     instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t3           SELECT
            instance_id,
            level_element_id,
            amount_due_date_time,
            completed_date_time,
            created_by_name,
            creation_date_time,
            gl_receivable_date_time,
            interface_date_time,
            last_update_date_time,
            last_updated_by_name,
            level_element_amt_ent,
            period_start_date,
            print_date_time,
            rule_id,
            rule_start_date_time,
            sequence_nbr,
            tran_code,
            transaction_date_time,
            alt_contract_header_id,
            contract_line_id,
            parent_contract_line_id,
            period_end_date,
            null  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 
        INNER JOIN
            (
                SELECT
                    DISTINCT level_element_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 
            ) AS autoAlias_203 
                ON (
                    upper(trim(level_element_id)) = upper(trim(autoAlias_203.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_203.AUTO_C01)) 
                );
