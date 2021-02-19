----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:33 
--Script Name: d8_contract_subline_bill_02.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1 FOR ACCESS LOCK TABLE  ${EEDDWWDDBB1}.contract_subline_bill_ld FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2 ( 	contract_subline_bill_id, 	instance_id, 	alt_as_of_date_time, 	alt_batch_nbr, 	as_of_date_time, 	batch_nbr, 	bill_amt_ent, 	bill_avg_amt_ent, 	bill_from_date_time, 	bill_to_date_time, 	contract_line_bill_id, 	contract_line_id, 	created_by_name, 	creation_date_time, 	current_record_ind, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	tran_code ) SELECT  	contract_subline_bill_id, 	instance_id, 	alt_as_of_date_time, 	alt_batch_nbr, 	as_of_date_time, 	batch_nbr, 	bill_amt_ent, 	bill_avg_amt_ent, 	bill_from_date_time, 	bill_to_date_time, 	contract_line_bill_id, 	contract_line_id, 	created_by_name, 	creation_date_time, 	'Y', 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	tran_code  FROM ${EEDDWWDDBB1}.contract_subline_bill_ld WHERE (contract_subline_bill_id,instance_id) IN (SELECT      contract_subline_bill_id,     instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2           SELECT
            contract_subline_bill_id,
            instance_id,
            alt_as_of_date_time,
            alt_batch_nbr,
            as_of_date_time,
            batch_nbr,
            bill_amt_ent,
            bill_avg_amt_ent,
            bill_from_date_time,
            bill_to_date_time,
            contract_line_bill_id,
            contract_line_id,
            created_by_name,
            creation_date_time,
            'Y',
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code  
        FROM
            ${EEDDWWDDBB1}.contract_subline_bill_ld 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_subline_bill_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1 
            ) AS autoAlias_235 
                ON (
                    upper(trim(contract_subline_bill_id)) = upper(trim(autoAlias_235.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_235.AUTO_C01)) 
                );

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_subline_bill_t3 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t3;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_subline_bill_t3 ( 	contract_subline_bill_id, 	instance_id, 	bill_amt_ent, 	bill_avg_amt_ent, 	bill_from_date_time, 	bill_to_date_time, 	contract_line_bill_id, 	contract_line_id, 	created_by_name, 	creation_date_time, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	tran_code ) SELECT 	contract_subline_bill_id, 	instance_id, 	bill_amt_ent, 	bill_avg_amt_ent, 	bill_from_date_time, 	bill_to_date_time, 	contract_line_bill_id, 	contract_line_id, 	created_by_name, 	creation_date_time, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	tran_code FROM ${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2 WHERE (contract_subline_bill_id,instance_id) IN (SELECT      contract_subline_bill_id,     instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t3           SELECT
            contract_subline_bill_id,
            instance_id,
            bill_amt_ent,
            bill_avg_amt_ent,
            bill_from_date_time,
            bill_to_date_time,
            contract_line_bill_id,
            contract_line_id,
            created_by_name,
            creation_date_time,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_subline_bill_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1 
            ) AS autoAlias_237 
                ON (
                    upper(trim(contract_subline_bill_id)) = upper(trim(autoAlias_237.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_237.AUTO_C01)) 
                );
