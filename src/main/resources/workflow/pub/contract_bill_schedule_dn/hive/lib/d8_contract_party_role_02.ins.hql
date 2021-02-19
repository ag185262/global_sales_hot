----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:29 
--Script Name: d8_contract_party_role_02.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_party_role_t2 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_party_role_t1 FOR ACCESS LOCK TABLE  ${EEDDWWDDBB1}.contract_party_role_ld FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_party_role_t2 ( 	instance_id, 	party_role_id, 	alt_as_of_date_time, 	alt_batch_nbr, 	alt_contract_header_id, 	as_of_date_time, 	batch_nbr, 	created_by_name, 	contract_line_id, 	contract_header_id, 	creation_date_time, 	current_record_ind, 	facility_code, 	government_code, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	minority_group_code, 	object1_code, 	object1_id1, 	object1_id2, 	role_code, 	parent_party_role_id, 	primary_party_flag, 	tran_code, 	small_business_flag, 	women_owned_flag ) SELECT  	instance_id, 	party_role_id, 	alt_as_of_date_time, 	alt_batch_nbr, 	alt_contract_header_id, 	as_of_date_time, 	batch_nbr, 	created_by_name, 	contract_line_id, 	contract_header_id, 	creation_date_time, 	'Y', 	facility_code, 	government_code, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	minority_group_code, 	object1_code, 	object1_id1, 	object1_id2, 	role_code, 	parent_party_role_id, 	primary_party_flag, 	tran_code, 	small_business_flag, 	women_owned_flag FROM ${EEDDWWDDBB1}.contract_party_role_ld WHERE (party_role_id, instance_id) IN (SELECT      party_role_id,     instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_party_role_t1     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2           SELECT
            instance_id,
            party_role_id,
            alt_as_of_date_time,
            alt_batch_nbr,
            alt_contract_header_id,
            as_of_date_time,
            batch_nbr,
            created_by_name,
            contract_line_id,
            contract_header_id,
            creation_date_time,
            'Y',
            facility_code,
            government_code,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            minority_group_code,
            object1_code,
            object1_id1,
            object1_id2,
            role_code,
            parent_party_role_id,
            primary_party_flag,
            tran_code,
            small_business_flag,
            women_owned_flag  
        FROM
            ${EEDDWWDDBB1}.contract_party_role_ld 
        INNER JOIN
            (
                SELECT
                    DISTINCT party_role_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t1 
            ) AS autoAlias_217 
                ON (
                    upper(trim(party_role_id)) = upper(trim(autoAlias_217.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_217.AUTO_C01)) 
                );

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_party_role_t3 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t3;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_party_role_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_party_role_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_party_role_t3 ( 	instance_id, 	party_role_id, 	alt_contract_header_id, 	created_by_name, 	contract_line_id, 	contract_header_id, 	creation_date_time, 	facility_code, 	government_code, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	minority_group_code, 	object1_code, 	object1_id1, 	object1_id2, 	role_code, 	parent_party_role_id, 	primary_party_flag, 	tran_code, 	small_business_flag, 	women_owned_flag ) SELECT 	instance_id, 	party_role_id, 	alt_contract_header_id, 	created_by_name, 	contract_line_id, 	contract_header_id, 	creation_date_time, 	facility_code, 	government_code, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	minority_group_code, 	object1_code, 	object1_id1, 	object1_id2, 	role_code, 	parent_party_role_id, 	primary_party_flag, 	tran_code, 	small_business_flag, 	women_owned_flag FROM ${AAPPLLIIDD1EENNVV}_contract_party_role_t2 WHERE (party_role_id, instance_id) IN (SELECT      party_role_id,     instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_party_role_t1     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t3           SELECT
            instance_id,
            party_role_id,
            alt_contract_header_id,
            created_by_name,
            contract_line_id,
            contract_header_id,
            creation_date_time,
            facility_code,
            government_code,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            minority_group_code,
            object1_code,
            object1_id1,
            object1_id2,
            role_code,
            parent_party_role_id,
            primary_party_flag,
            tran_code,
            small_business_flag,
            women_owned_flag  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2 
        INNER JOIN
            (
                SELECT
                    DISTINCT party_role_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t1 
            ) AS autoAlias_219 
                ON (
                    upper(trim(party_role_id)) = upper(trim(autoAlias_219.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_219.AUTO_C01)) 
                );
