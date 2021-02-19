----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:00 
--Script Name: d8_contract_group_lk_04.upd.sql
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
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_lk_t3 FOR ACCESS  UPDATE ${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 SET current_record_ind = 'D' WHERE (contract_group_id,instance_id,language_code) IN     (SELECT contract_group_id,instance_id,language_code     FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_t3     GROUP BY contract_group_id,instance_id,language_code     HAVING COUNT(*) = 1     )  

--Translated Query: STATUS SUCCESS (Slightly modified, added 2 more not null checks to the case statement)

    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 SELECT
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.contract_group_id AS contract_group_id ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.language_code AS language_code ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.alt_as_of_date_time AS alt_as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.alt_batch_nbr AS alt_batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.as_of_date_time AS as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.batch_nbr AS batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.contract_group_name AS contract_group_name ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.created_by_name AS created_by_name ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.creation_date_time AS creation_date_time ,
            CASE 
                WHEN autoAlias_49.auto_c00 IS not null and autoAlias_49.auto_c01 is not null and autoAlias_49.auto_c02 is not null THEN 'D' 
                ELSE AAPPLLIIDD1EENNVV_contract_group_lk_t2.current_record_ind 
            END AS current_record_ind ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.last_update_date_time AS last_update_date_time ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.last_update_login_name AS last_update_login_name ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.last_updated_by_name AS last_updated_by_name ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.contract_group_short_desc AS contract_group_short_desc ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.source_language_code AS source_language_code ,
            AAPPLLIIDD1EENNVV_contract_group_lk_t2.tran_code AS tran_code 
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 AS AAPPLLIIDD1EENNVV_contract_group_lk_t2 
        Left Outer Join
            (
                SELECT
                    contract_group_id AS auto_c00,
                    instance_id AS auto_c01,
                    language_code AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t3      
                GROUP BY
                    contract_group_id,
                    instance_id,
                    language_code 
                HAVING
                    COUNT(*) = 1
            )autoAlias_49 
                ON (
                    autoAlias_49.auto_c00 = AAPPLLIIDD1EENNVV_contract_group_lk_t2.contract_group_id 
                    AND upper(trim( autoAlias_49.auto_c01)) = upper(trim( AAPPLLIIDD1EENNVV_contract_group_lk_t2.instance_id)) 
                    AND upper(trim( autoAlias_49.auto_c02)) = upper(trim( AAPPLLIIDD1EENNVV_contract_group_lk_t2.language_code))
                );
