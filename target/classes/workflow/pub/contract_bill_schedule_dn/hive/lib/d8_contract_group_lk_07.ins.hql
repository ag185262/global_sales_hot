----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:01 
--Script Name: d8_contract_group_lk_07.ins.sql
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

--(Bala: Corrected the translated query,The table used in join was skipped, added that and also kept only limited columns in corrected query )
--Original Query:
  --DELETE FROM contract_group_lk_ld  WHERE (contract_group_id,instance_id,language_code) IN (SELECT contract_group_id,instance_id,language_code     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2     )  

--Translated Query: STATUS SUCCESS

    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_group_lk_ld SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.contract_group_lk_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${EEDDWWDDBB1}.contract_group_lk_ld      
            -- ) AS Q2 
                -- ON COALESCE( Q1.contract_group_id ,
            -- 1) = COALESCE( Q2.contract_group_id ,
            -- 1) 
            -- AND COALESCE( Q1.instance_id ,
            -- '1' ) = COALESCE( Q2.instance_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.language_code ,
            -- '1' ) = COALESCE( Q2.language_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.alt_batch_nbr ,
            -- 1) = COALESCE( Q2.alt_batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.batch_nbr ,
            -- 1) = COALESCE( Q2.batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.contract_group_name ,
            -- '1' ) = COALESCE( Q2.contract_group_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.created_by_name ,
            -- '1' ) = COALESCE( Q2.created_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_update_login_name ,
            -- '1' ) = COALESCE( Q2.last_update_login_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_updated_by_name ,
            -- '1' ) = COALESCE( Q2.last_updated_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.contract_group_short_desc ,
            -- '1' ) = COALESCE( Q2.contract_group_short_desc ,
            -- '1' ) 
            -- AND COALESCE( Q1.source_language_code ,
            -- '1' ) = COALESCE( Q2.source_language_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.tran_code ,
            -- '1' ) = COALESCE( Q2.tran_code ,
            -- '1' ) 
        -- WHERE
            -- Q2.contract_group_id IS NULL 
            -- AND Q2.instance_id IS NULL 
            -- AND Q2.language_code IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.contract_group_name IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_update_login_name IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.contract_group_short_desc IS NULL 
            -- AND Q2.source_language_code IS NULL 
            -- AND Q2.tran_code IS NULL;
			
--Corrected Query:Transformed Query logic is incorrect.In below query resolved.              
        INSERT OVERWRITE
                TABLE ${EEDDWWDDBB1}.contract_group_lk_ld       
        
        SELECT
        Q1.* 
        FROM
        ${EEDDWWDDBB1}.contract_group_lk_ld AS Q1
        Left Join
            (
                SELECT
                    contract_group_id,
                    instance_id,
                    language_code 
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2
            )
            As Q2 
            On  Q1.contract_group_id = Q2.contract_group_id
            And upper(trim( Q1.instance_id ))= upper(trim( Q2.instance_id)) 
            And upper(trim( Q1.language_code)) = upper(trim( Q2.language_code)) 
        Where
        Q2.contract_group_id Is NULL 
        And Q2.instance_id Is Null
        And Q2.language_code Is Null ;
--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 FOR ACCESS  INSERT INTO contract_group_lk_ld (     contract_group_id,  instance_id,    language_code,  alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_group_name,    created_by_name,    creation_date_time,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   contract_group_short_desc,  source_language_code,   tran_code ) SELECT      contract_group_id,  instance_id,    language_code,  alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_group_name,    created_by_name,    creation_date_time,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   contract_group_short_desc,  source_language_code,   tran_code FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_group_lk_ld           SELECT
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
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            contract_group_short_desc,
            source_language_code,
            tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2;
