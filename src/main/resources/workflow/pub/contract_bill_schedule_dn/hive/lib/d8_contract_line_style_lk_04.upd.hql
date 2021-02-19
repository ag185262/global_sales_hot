----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:41:14 
--Script Name: d8_contract_line_style_lk_04.upd.sql
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

--Original Query: bd920c8f2faa4c37e0e7040eb2cb1961
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t3 FOR ACCESS  UPDATE ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2 SET current_record_ind = 'D' WHERE (contract_line_style_id,instance_id,language_code) IN     (SELECT contract_line_style_id,instance_id,language_code     FROM ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t3     GROUP BY contract_line_style_id,instance_id,language_code     HAVING COUNT(*) = 1     )  

--Translated Query: STATUS SUCCESS

    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2 SELECT
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.contract_line_style_id AS contract_line_style_id ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.language_code AS language_code ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.alt_as_of_date_time AS alt_as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.alt_batch_nbr AS alt_batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.as_of_date_time AS as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.batch_nbr AS batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.created_by_name AS created_by_name ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.creation_date_time AS creation_date_time ,
            CASE 
                WHEN autoAlias_491.auto_c00 IS not null THEN 'D' 
                ELSE AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.current_record_ind 
            END AS current_record_ind ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.last_update_date_time AS last_update_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.last_update_login_name AS last_update_login_name ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.last_updated_by_name AS last_updated_by_name ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.line_style_desc AS line_style_desc ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.line_style_name AS line_style_name ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.source_language_code AS source_language_code ,
            AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.tran_code AS tran_code 
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2 AS AAPPLLIIDD1EENNVV_contract_line_style_lk_t2 
        Left Outer Join
            (
                SELECT
                    contract_line_style_id AS auto_c00,
                    instance_id AS auto_c01,
                    language_code AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t3      
                GROUP BY
                    contract_line_style_id,
                    instance_id,
                    language_code 
                HAVING
                    COUNT(*) = 1
            )autoAlias_491 
                ON (
                    autoAlias_491.auto_c00 = AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.contract_line_style_id 
                    AND upper(trim(autoAlias_491.auto_c01)) = upper(trim(AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.instance_id)) 
                    AND upper(trim(autoAlias_491.auto_c02)) = upper(trim(AAPPLLIIDD1EENNVV_contract_line_style_lk_t2.language_code))
                );
