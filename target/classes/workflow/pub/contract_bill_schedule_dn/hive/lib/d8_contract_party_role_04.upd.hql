----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:30 
--Script Name: d8_contract_party_role_04.upd.sql
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
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_party_role_t3 FOR ACCESS  UPDATE ${AAPPLLIIDD1EENNVV}_contract_party_role_t2 SET current_record_ind = 'D' WHERE (party_role_id, instance_id) IN     (SELECT party_role_id, instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_party_role_t3     GROUP BY party_role_id, instance_id     HAVING COUNT(*) = 1     )  

--Translated Query: STATUS MANUAL
--(Nikhil: Corrected the case statement used in translated query)
    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2 SELECT
            AAPPLLIIDD1EENNVV_contract_party_role_t2.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.party_role_id AS party_role_id ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.alt_as_of_date_time AS alt_as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.alt_batch_nbr AS alt_batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.alt_contract_header_id AS alt_contract_header_id ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.as_of_date_time AS as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.batch_nbr AS batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.created_by_name AS created_by_name ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.contract_line_id AS contract_line_id ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.contract_header_id AS contract_header_id ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.creation_date_time AS creation_date_time ,
            CASE 
                --WHEN autoAlias_223.auto_c00 IS not null THEN 'D' 
                WHEN autoAlias_223.auto_c00 IS not null and autoAlias_223.auto_c01 IS not null THEN 'D' 
                ELSE AAPPLLIIDD1EENNVV_contract_party_role_t2.current_record_ind 
            END AS current_record_ind ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.facility_code AS facility_code ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.government_code AS government_code ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.last_update_date_time AS last_update_date_time ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.last_update_login_name AS last_update_login_name ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.last_updated_by_name AS last_updated_by_name ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.minority_group_code AS minority_group_code ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.object1_code AS object1_code ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.object1_id1 AS object1_id1 ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.object1_id2 AS object1_id2 ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.role_code AS role_code ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.parent_party_role_id AS parent_party_role_id ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.primary_party_flag AS primary_party_flag ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.tran_code AS tran_code ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.small_business_flag AS small_business_flag ,
            AAPPLLIIDD1EENNVV_contract_party_role_t2.women_owned_flag AS women_owned_flag 
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2 AS AAPPLLIIDD1EENNVV_contract_party_role_t2 
        Left Outer Join
            (
                SELECT
                    party_role_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t3      
                GROUP BY
                    party_role_id,
                    instance_id 
                HAVING
                    COUNT(*) = 1
            )autoAlias_223 
                ON (
                   upper(trim( autoAlias_223.auto_c00 ))= upper(trim(AAPPLLIIDD1EENNVV_contract_party_role_t2.party_role_id)) 
                    AND upper(trim(autoAlias_223.auto_c01)) = upper(trim(AAPPLLIIDD1EENNVV_contract_party_role_t2.instance_id))
                );
