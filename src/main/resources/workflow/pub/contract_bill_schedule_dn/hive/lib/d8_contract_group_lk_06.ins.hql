----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:01 
--Script Name: d8_contract_group_lk_06.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t1;

--(Nikhil:Replaced the Max function with Row_number function and also removed Inner Join as it is not required)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_lk_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 (   contract_group_id,  instance_id,    language_code,  as_of_date_time,    batch_nbr,  contract_group_name,    created_by_name,    creation_date_time,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   contract_group_short_desc,  source_language_code,   tran_code ) SELECT      contract_group_id,  instance_id,    language_code,  as_of_date_time,    batch_nbr,  contract_group_name,    created_by_name,    creation_date_time,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   contract_group_short_desc,  source_language_code,   tran_code FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,   contract_group_id,  instance_id,    language_code ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),      contract_group_id,  instance_id,    language_code     FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_ml     WHERE tran_code = 'D'     GROUP BY 2,3,4 ) AND tran_code = 'D'  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t1           SELECT
            -- contract_group_id,
            -- instance_id,
            -- language_code,
            -- as_of_date_time,
            -- batch_nbr,
            -- contract_group_name,
            -- created_by_name,
            -- creation_date_time,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- contract_group_short_desc,
            -- source_language_code,
            -- tran_code  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- contract_group_id AS auto_c01,
                    -- instance_id AS auto_c02,
                    -- language_code AS auto_c03  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_ml     
                -- WHERE
                    -- upper(trim(tran_code)) = upper(trim('D'))  
                -- GROUP BY
                    -- contract_group_id ,
                    -- instance_id ,
                    -- language_code 
            -- ) AS autoAlias_55 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_55.AUTO_C00 
                -- AND contract_group_id = autoAlias_55.AUTO_C01 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_55.AUTO_C02)) 
                -- AND upper(trim(language_code)) = upper(trim(autoAlias_55.AUTO_C03)) ) 
            -- WHERE
               -- upper(trim(tran_code)) = upper(trim('D'));
--Translated Query(Corrected Manually):
with qq1 as (
SELECT
                    contract_group_id,
                    instance_id,
                    language_code,
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
                    tran_code,
                    ROW_NUMBER() OVER(PARTITION BY contract_group_id ,instance_id ,language_code  
                    ORDER BY ( CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    batch_nbr)) DESC) AS RNK
                    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_ml    
                    WHERE upper(trim(tran_code)) = upper(trim('D'))) 
INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 
SELECT
                    EDW2.contract_group_id,
                    EDW2.instance_id,
                    EDW2.language_code,
                    EDW2.as_of_date_time,
                    EDW2.batch_nbr,
                    EDW2.contract_group_name,
                    EDW2.created_by_name,
                    EDW2.creation_date_time,
                    EDW2.last_update_date_time,
                    EDW2.last_update_login_name,
                    EDW2.last_updated_by_name,
                    EDW2.contract_group_short_desc,
                    EDW2.source_language_code,
                    EDW2.tran_code FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_ml EDW2 
  INNER JOIN (
    SELECT 
      contract_group_id,
                    instance_id,
                    language_code,
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
            RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND EDW2.contract_group_id =IBVL.contract_group_id 
  AND upper(trim(EDW2.language_code)) =upper(trim(IBVL.language_code))
  AND upper(trim(EDW2.tran_code)) = upper(trim('D'));
--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_group_lk_t2 (   contract_group_id,  instance_id,    language_code,  alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_group_name,    created_by_name,    creation_date_time,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   contract_group_short_desc,  source_language_code,   tran_code ) SELECT  l.contract_group_id,    l.instance_id,  l.language_code,    t.as_of_date_time,  t.batch_nbr,    l.as_of_date_time,  l.batch_nbr,    l.contract_group_name,  l.created_by_name,  l.creation_date_time,   l.last_update_date_time,    l.last_update_login_name,   l.last_updated_by_name,     l.contract_group_short_desc,    l.source_language_code,     t.tran_code FROM ${EEDDWWDDBB1}.contract_group_lk_ld l,       ${AAPPLLIIDD1EENNVV}_contract_group_lk_t1 t WHERE  l.contract_group_id = t.contract_group_id AND    l.instance_id = t.instance_id  AND    l.language_code = t.language_code AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <    CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t2           SELECT
            l.contract_group_id,
            l.instance_id,
            l.language_code,
            t.as_of_date_time,
            t.batch_nbr,
            l.as_of_date_time,
            l.batch_nbr,
            l.contract_group_name,
            l.created_by_name,
            l.creation_date_time,
            null,
            l.last_update_date_time,
            l.last_update_login_name,
            l.last_updated_by_name,
            l.contract_group_short_desc,
            l.source_language_code,
            t.tran_code  
        FROM
            ${EEDDWWDDBB1}.contract_group_lk_ld    AS l   ,
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_lk_t1    AS t   
        WHERE
            l.contract_group_id = t.contract_group_id  
            AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))   
            AND upper(trim(l.language_code)) = upper(trim(t.language_code))   
            AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);
