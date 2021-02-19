----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:41:12 
--Script Name: d8_contract_line_style_lk_03.ins.sql
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

--Original Query: 8afac8dcc5c5ed059e01691476f4202b
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t3 ( 	contract_line_style_id, 	instance_id, 	language_code, 	created_by_name, 	creation_date_time, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	line_style_desc, 	line_style_name, 	source_language_code, 	tran_code ) SELECT 	contract_line_style_id, 	instance_id, 	language_code, 	created_by_name, 	creation_date_time, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	line_style_desc, 	line_style_name, 	source_language_code, 	tran_code FROM ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t1 WHERE (contract_line_style_id,instance_id,language_code) IN (SELECT          contract_line_style_id, 	instance_id, 	language_code     FROM ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t3           SELECT
            contract_line_style_id,
            instance_id,
            language_code,
            null,
            null,
            null,
            null,
            created_by_name,
            creation_date_time,
            null,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            line_style_desc,
            line_style_name,
            source_language_code,
            tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t1 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_line_style_id AS auto_c00,
                    instance_id AS auto_c01,
                    language_code AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2 
            ) AS autoAlias_489 
                ON (
                    contract_line_style_id = autoAlias_489.AUTO_C00 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_489.AUTO_C01)) 
                    AND upper(trim(language_code)) = upper(trim(autoAlias_489.AUTO_C02)) 
                );
