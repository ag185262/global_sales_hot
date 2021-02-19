----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:37:16 
--Script Name: d8_contract_group_lk_03.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query: 7e58aa2f355917bd712f410b9b1585c8
  --DATABASE ${TTMMPPDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${TTMMPPDDBB1} ;

--Original Query: 657097b0d643ee312432ace4b1af330d
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_group_lk_t3 ( 	contract_group_id, 	instance_id, 	language_code, 	contract_group_name, 	created_by_name, 	creation_date_time, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	contract_group_short_desc, 	source_language_code, 	tran_code ) SELECT 	contract_group_id, 	instance_id, 	language_code, 	contract_group_name, 	created_by_name, 	creation_date_time, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	contract_group_short_desc, 	source_language_code, 	tran_code FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 WHERE (contract_group_id,instance_id,language_code) IN (SELECT  	contract_group_id, 	instance_id, 	language_code     FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_t2     )  

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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_group_id AS auto_c00,
                    instance_id AS auto_c01,
                    language_code AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 
            ) AS autoAlias_293 
                ON (
                    contract_group_id = autoAlias_293.AUTO_C00 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_293.AUTO_C01)) 
                    AND upper(trim(language_code)) = upper(trim(autoAlias_293.AUTO_C02)) 
                );
