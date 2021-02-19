----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:25 
--Script Name: d8_contract_lvl_element_02.ins.sql
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
  --COLLECT STATISTICS    COLUMN (level_element_id,instance_id)  ON ${EEDDWWDDBB1}.contract_lvl_element 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB1}.contract_lvl_element COMPUTE STATISTICS  FOR COLUMNS level_element_id,instance_id;

--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB1}.contract_lvl_element_ld from ${EEDDWWDDBB1}.contract_lvl_element 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB1}.contract_lvl_element_ld COMPUTE STATISTICS;

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 FOR ACCESS LOCK TABLE  ${EEDDWWDDBB1}.contract_lvl_element_ld FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 ( 	instance_id, 	level_element_id, 	alt_as_of_date_time, 	alt_batch_nbr, 	amount_due_date_time, 	as_of_date_time, 	batch_nbr, 	completed_date_time, 	created_by_name, 	creation_date_time, 	current_record_ind, 	gl_receivable_date_time, 	interface_date_time, 	last_update_date_time, 	last_updated_by_name, 	level_element_amt_ent, 	period_start_date, 	print_date_time, 	rule_id, 	rule_start_date_time, 	sequence_nbr, 	tran_code, 	transaction_date_time, 	alt_contract_header_id, 	contract_line_id, 	parent_contract_line_id, 	period_end_date ) SELECT  	instance_id, 	level_element_id, 	alt_as_of_date_time, 	alt_batch_nbr, 	amount_due_date_time, 	as_of_date_time, 	batch_nbr, 	completed_date_time, 	created_by_name, 	creation_date_time, 	'Y', 	gl_receivable_date_time, 	interface_date_time, 	last_update_date_time, 	last_updated_by_name, 	level_element_amt_ent, 	period_start_date, 	print_date_time, 	rule_id, 	rule_start_date_time, 	sequence_nbr, 	tran_code, 	transaction_date_time, 	alt_contract_header_id, 	contract_line_id, 	parent_contract_line_id, 	period_end_date FROM ${EEDDWWDDBB1}.contract_lvl_element_ld WHERE  EXISTS  (SEL 1 FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1    WHERE ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1.level_element_id = ${EEDDWWDDBB1}.contract_lvl_element_ld.level_element_id   AND   ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1.instance_id = ${EEDDWWDDBB1}.contract_lvl_element_ld.instance_id )  AND ${EEDDWWDDBB1}.contract_lvl_element_ld.tran_code <> 'D'  

--Translated Query: STATUS MANUAL
--Added the upper and trim function in join condition used in where clause
    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2           SELECT
            instance_id,
            level_element_id,
            amount_due_date_time,
            alt_as_of_date_time,
            alt_batch_nbr,
            as_of_date_time,
            batch_nbr,
            completed_date_time,
            created_by_name,
            creation_date_time,
            'Y',
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
            ${EEDDWWDDBB1}.contract_lvl_element_ld     
        WHERE
            EXISTS  (
                SELECT
                    1  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1     
                WHERE
                    upper(trim(${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1.level_element_id)) =     upper(trim(contract_lvl_element_ld.level_element_id )) 
                    AND     upper(trim(${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1.instance_id)) =     upper(trim(contract_lvl_element_ld.instance_id ))     
            )  
            AND upper(trim(contract_lvl_element_ld.tran_code)) <> upper(trim('D'));

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t3 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t3;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t3 ( 	instance_id, 	level_element_id, 	amount_due_date_time, 	completed_date_time, 	created_by_name, 	creation_date_time, 	gl_receivable_date_time, 	interface_date_time, 	last_update_date_time, 	last_updated_by_name, 	level_element_amt_ent, 	period_start_date, 	print_date_time, 	rule_id, 	rule_start_date_time, 	sequence_nbr, 	tran_code, 	transaction_date_time, 	alt_contract_header_id, 	contract_line_id, 	parent_contract_line_id, 	period_end_date ) SELECT 	instance_id, 	level_element_id, 	amount_due_date_time, 	completed_date_time, 	created_by_name, 	creation_date_time, 	gl_receivable_date_time, 	interface_date_time, 	last_update_date_time, 	last_updated_by_name, 	level_element_amt_ent, 	period_start_date, 	print_date_time, 	rule_id, 	rule_start_date_time, 	sequence_nbr, 	tran_code, 	transaction_date_time, 	alt_contract_header_id, 	contract_line_id, 	parent_contract_line_id, 	period_end_date FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 WHERE (level_element_id, instance_id) IN (SELECT      level_element_id,     instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1     )  

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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 
        INNER JOIN
            (
                SELECT
                    DISTINCT level_element_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 
            ) AS autoAlias_201 
                ON (
                    upper(trim(level_element_id)) = upper(trim(autoAlias_201.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_201.AUTO_C01)) 
                );
