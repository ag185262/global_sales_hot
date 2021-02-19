----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:32 
--Script Name: d8_contract_party_role_07.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${EEDDWWDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${EEDDWWDDBB1} ;


--Original Query:
  --DELETE FROM contract_party_role_ld  WHERE (party_role_id, instance_id) IN (SELECT party_role_id, instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2     )  

--Translated Query: STATUS MANUAL
--(Nikhil: Corrected the translated query,The table used in join was skipped, added that and also kept only limited columns in corrected query )
    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_party_role_ld SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.contract_party_role_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${EEDDWWDDBB1}.contract_party_role_ld      
            -- ) AS Q2 
                -- ON COALESCE( Q1.instance_id ,
            -- '1' ) = COALESCE( Q2.instance_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.party_role_id ,
            -- '1' ) = COALESCE( Q2.party_role_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.alt_batch_nbr ,
            -- 1) = COALESCE( Q2.alt_batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.alt_contract_header_id ,
            -- '1' ) = COALESCE( Q2.alt_contract_header_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.batch_nbr ,
            -- 1) = COALESCE( Q2.batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.created_by_name ,
            -- '1' ) = COALESCE( Q2.created_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.contract_line_id ,
            -- '1' ) = COALESCE( Q2.contract_line_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.contract_header_id ,
            -- '1' ) = COALESCE( Q2.contract_header_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.facility_code ,
            -- '1' ) = COALESCE( Q2.facility_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.government_code ,
            -- '1' ) = COALESCE( Q2.government_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_update_login_name ,
            -- '1' ) = COALESCE( Q2.last_update_login_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_updated_by_name ,
            -- '1' ) = COALESCE( Q2.last_updated_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.minority_group_code ,
            -- '1' ) = COALESCE( Q2.minority_group_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.object1_code ,
            -- '1' ) = COALESCE( Q2.object1_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.object1_id1 ,
            -- '1' ) = COALESCE( Q2.object1_id1 ,
            -- '1' ) 
            -- AND COALESCE( Q1.object1_id2 ,
            -- '1' ) = COALESCE( Q2.object1_id2 ,
            -- '1' ) 
            -- AND COALESCE( Q1.role_code ,
            -- '1' ) = COALESCE( Q2.role_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.parent_party_role_id ,
            -- '1' ) = COALESCE( Q2.parent_party_role_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.primary_party_flag ,
            -- '1' ) = COALESCE( Q2.primary_party_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.tran_code ,
            -- '1' ) = COALESCE( Q2.tran_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.small_business_flag ,
            -- '1' ) = COALESCE( Q2.small_business_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.women_owned_flag ,
            -- '1' ) = COALESCE( Q2.women_owned_flag ,
            -- '1' ) 
        -- WHERE
            -- Q2.instance_id IS NULL 
            -- AND Q2.party_role_id IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.alt_contract_header_id IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.contract_line_id IS NULL 
            -- AND Q2.contract_header_id IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.facility_code IS NULL 
            -- AND Q2.government_code IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_update_login_name IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.minority_group_code IS NULL 
            -- AND Q2.object1_code IS NULL 
            -- AND Q2.object1_id1 IS NULL 
            -- AND Q2.object1_id2 IS NULL 
            -- AND Q2.role_code IS NULL 
            -- AND Q2.parent_party_role_id IS NULL 
            -- AND Q2.primary_party_flag IS NULL 
            -- AND Q2.tran_code IS NULL 
            -- AND Q2.small_business_flag IS NULL 
            -- AND Q2.women_owned_flag IS NULL;*/
            
--Correct Query:
    INSERT OVERWRITE
            TABLE ${EEDDWWDDBB1}.contract_party_role_ld
    select
    Q1.* 
    From
    ${EEDDWWDDBB1}.contract_party_role_ld Q1 
    Left Join
        (
            SELECT
                party_role_id,
                instance_id 
            FROM
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2
        )
        As Q2 
        On upper(trim(Q1.party_role_id)) = upper(trim(Q2.party_role_id)) 
        And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id)) 
    Where
    Q2.party_role_id Is NULL 
    And Q2.instance_id Is Null ;            

--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2 FOR ACCESS  INSERT INTO contract_party_role_ld (     instance_id,    party_role_id,  alt_as_of_date_time,    alt_batch_nbr,  alt_contract_header_id,     as_of_date_time,    batch_nbr,  created_by_name,    contract_line_id,   contract_header_id,     creation_date_time,     facility_code,  government_code,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   minority_group_code,    object1_code,   object1_id1,    object1_id2,    role_code,  parent_party_role_id,   primary_party_flag,     tran_code,  small_business_flag,    women_owned_flag ) SELECT   instance_id,    party_role_id,  alt_as_of_date_time,    alt_batch_nbr,  alt_contract_header_id,     as_of_date_time,    batch_nbr,  created_by_name,    contract_line_id,   contract_header_id,     creation_date_time,     facility_code,  government_code,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   minority_group_code,    object1_code,   object1_id1,    object1_id2,    role_code,  parent_party_role_id,   primary_party_flag,     tran_code,  small_business_flag,    women_owned_flag FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_party_role_ld           SELECT
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2;
