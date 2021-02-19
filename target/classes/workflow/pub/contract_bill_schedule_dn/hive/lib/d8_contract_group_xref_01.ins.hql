----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:01 
--Script Name: d8_contract_group_xref_01.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_group_xref_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_t1;

--(Nikhil:Replaced the Max function with Row_number function and also removed Inner Join as it is not required)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_xref_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_group_xref_t1 (   contract_group_xref_id,     instance_id,    as_of_date_time,    batch_nbr,  created_by_name,    creation_date_time,     included_contract_group_id,     included_contract_header_id,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   parent_contract_group_id,   subclass_code,  tran_code ) SELECT      contract_group_xref_id,     instance_id,    as_of_date_time,    batch_nbr,  created_by_name,    creation_date_time,     included_contract_group_id,     included_contract_header_id,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   parent_contract_group_id,   subclass_code,  tran_code FROM ${AAPPLLIIDD1EENNVV}_contract_group_xref_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,     contract_group_xref_id,     instance_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),    contract_group_xref_id,     instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_group_xref_ml     WHERE tran_code <> 'D'     GROUP BY 2,3 ) AND tran_code <> 'D'  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_t1           SELECT
            -- contract_group_xref_id,
            -- instance_id,
            -- as_of_date_time,
            -- batch_nbr,
            -- created_by_name,
            -- creation_date_time,
            -- included_contract_group_id,
            -- included_contract_header_id,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- parent_contract_group_id,
            -- subclass_code,
            -- tran_code  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- contract_group_xref_id AS auto_c01,
                    -- instance_id AS auto_c02  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_ml     
                -- WHERE
                    -- upper(trim(tran_code)) <> upper(trim('D'))  
                -- GROUP BY
                    -- contract_group_xref_id ,
                    -- instance_id 
            -- ) AS autoAlias_59 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_59.AUTO_C00 
                -- AND upper(trim(contract_group_xref_id)) = upper(trim(autoAlias_59.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_59.AUTO_C02)) )
              -- WHERE
                  -- upper(trim(tran_code)) <> upper(trim('D'));

             
--Translated Query(Corrected Manually):             
with qq1 as (
SELECT
                    contract_group_xref_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    included_contract_group_id,
                    included_contract_header_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    parent_contract_group_id,
                    subclass_code,
                    tran_code,
                    ROW_NUMBER() OVER(PARTITION BY contract_group_xref_id,instance_id ORDER BY 
                    (CONCAT (CAST (as_of_date_time AS CHAR(26)),batch_nbr)) DESC) AS RNK
                    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_ml              WHERE
                     upper(trim(tran_code)) <> upper(trim('D')) ) 
INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_t1 
SELECT
            EDW2.contract_group_xref_id,
            EDW2.instance_id,
            EDW2.as_of_date_time,
            EDW2.batch_nbr,
            EDW2.created_by_name,
            EDW2.creation_date_time,
            EDW2.included_contract_group_id,
            EDW2.included_contract_header_id,
            EDW2.last_update_date_time,
            EDW2.last_update_login_name,
            EDW2.last_updated_by_name,
            EDW2.parent_contract_group_id,
            EDW2.subclass_code,
            EDW2.tran_code FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_ml EDW2 
  INNER JOIN (
    SELECT 
       contract_group_xref_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    creation_date_time,
                    included_contract_group_id,
                    included_contract_header_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    parent_contract_group_id,
                    subclass_code,
                    tran_code, 
            RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND upper(trim(EDW2.contract_group_xref_id)) = upper(trim(IBVL.contract_group_xref_id)) 
  AND upper(trim(EDW2.tran_code)) <> upper(trim('D'));