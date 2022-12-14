----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:00 
--Script Name: d8_contract_group_lk_02.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 FOR ACCESS LOCK TABLE  ${EEDDWWDDBB1}.contract_group_lk_ld FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 ( 	contract_group_id, 	instance_id, 	language_code, 	alt_as_of_date_time, 	alt_batch_nbr, 	as_of_date_time, 	batch_nbr, 	contract_group_name, 	created_by_name, 	creation_date_time, 	current_record_ind, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	contract_group_short_desc, 	source_language_code, 	tran_code ) SELECT  	contract_group_id, 	instance_id, 	language_code, 	alt_as_of_date_time, 	alt_batch_nbr, 	as_of_date_time, 	batch_nbr, 	contract_group_name, 	created_by_name, 	creation_date_time, 	'Y', 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	contract_group_short_desc, 	source_language_code, 	tran_code FROM ${EEDDWWDDBB1}.contract_group_lk_ld WHERE (contract_group_id,instance_id,language_code) IN (SELECT  	contract_group_id, 	instance_id, 	language_code     FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_t1     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2           SELECT
            contract_group_id,
            instance_id,
            language_code,
            alt_as_of_date_time,
            alt_batch_nbr,
            as_of_date_time,
            batch_nbr,
            contract_group_name,
            created_by_name,
            creation_date_time,
            'Y',
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            contract_group_short_desc,
            source_language_code,
            tran_code  
        FROM
            ${EEDDWWDDBB1}.contract_group_lk_ld 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_group_id AS auto_c00,
                    instance_id AS auto_c01,
                    language_code AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 
            ) AS autoAlias_45 
                ON (
                    contract_group_id = autoAlias_45.AUTO_C00 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_45.AUTO_C01)) 
                    AND upper(trim(language_code)) = upper(trim(autoAlias_45.AUTO_C02)) 
                );

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_t3 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t3;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_group_lk_t3 ( 	contract_group_id, 	instance_id, 	language_code, 	contract_group_name, 	created_by_name, 	creation_date_time, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	contract_group_short_desc, 	source_language_code, 	tran_code ) SELECT 	contract_group_id, 	instance_id, 	language_code, 	contract_group_name, 	created_by_name, 	creation_date_time, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	contract_group_short_desc, 	source_language_code, 	tran_code FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 WHERE (contract_group_id,instance_id,language_code) IN (SELECT  	contract_group_id, 	instance_id, 	language_code     FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_t1     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t3           SELECT
            contract_group_id,
            instance_id,
            language_code,
            contract_group_name,
            created_by_name,
            creation_date_time,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            contract_group_short_desc,
            source_language_code,
            tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_group_id AS auto_c00,
                    instance_id AS auto_c01,
                    language_code AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 
            ) AS autoAlias_47 
                ON (
                    contract_group_id = autoAlias_47.AUTO_C00 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_47.AUTO_C01)) 
                    AND upper(trim(language_code)) = upper(trim(autoAlias_47.AUTO_C02)) 
                );
