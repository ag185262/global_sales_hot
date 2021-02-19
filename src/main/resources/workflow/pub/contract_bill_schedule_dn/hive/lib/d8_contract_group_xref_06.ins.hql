----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:03 
--Script Name: d8_contract_group_xref_06.ins.sql
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

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_xref_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_group_xref_t1 ( 	contract_group_xref_id, 	instance_id, 	as_of_date_time, 	batch_nbr, 	created_by_name, 	creation_date_time, 	included_contract_group_id, 	included_contract_header_id, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	parent_contract_group_id, 	subclass_code, 	tran_code ) SELECT  	contract_group_xref_id, 	instance_id, 	as_of_date_time, 	batch_nbr, 	created_by_name, 	creation_date_time, 	included_contract_group_id, 	included_contract_header_id, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	parent_contract_group_id, 	subclass_code, 	tran_code FROM ${AAPPLLIIDD1EENNVV}_contract_group_xref_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,  	contract_group_xref_id,  	instance_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),  	contract_group_xref_id,  	instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_group_xref_ml     WHERE tran_code = 'D'     GROUP BY 2,3 ) AND tran_code = 'D'  

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
                    -- upper(trim(tran_code)) = upper(trim('D'))  
                -- GROUP BY
                    -- contract_group_xref_id ,
                    -- instance_id 
            -- ) AS autoAlias_73 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_73.AUTO_C00 
                -- AND upper(trim(contract_group_xref_id)) = upper(trim(autoAlias_73.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_73.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) = upper(trim('D'));
            
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
                     upper(trim(tran_code)) = upper(trim('D'))) 
INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_t1 
SELECT
            edw2.contract_group_xref_id,
            edw2.instance_id,
            edw2.as_of_date_time,
            edw2.batch_nbr,
            edw2.created_by_name,
            edw2.creation_date_time,
            edw2.included_contract_group_id,
            edw2.included_contract_header_id,
            edw2.last_update_date_time,
            edw2.last_update_login_name,
            edw2.last_updated_by_name,
            edw2.parent_contract_group_id,
            edw2.subclass_code,
            edw2.tran_code 
FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_ml EDW2 
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
   AND upper(trim(EDW2.contract_group_xref_id)) =upper(trim(IBVL.contract_group_xref_id)) 
  AND upper(trim(EDW2.tran_code)) = upper(trim('D'));
--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_group_xref_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_t2;
--(Replaced Inner join with CTE block)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_xref_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_group_xref_t2 ( 	contract_group_xref_id, 	instance_id, 	alt_as_of_date_time, 	alt_batch_nbr, 	as_of_date_time, 	batch_nbr, 	created_by_name, 	creation_date_time, 	included_contract_group_id, 	included_contract_header_id, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	parent_contract_group_id, 	subclass_code, 	tran_code ) SELECT 	l.contract_group_xref_id, 	l.instance_id, 	t.as_of_date_time, 	t.batch_nbr, 	l.as_of_date_time, 	l.batch_nbr, 	l.created_by_name, 	l.creation_date_time, 	l.included_contract_group_id, 	l.included_contract_header_id, 	l.last_update_date_time, 	l.last_update_login_name, 	l.last_updated_by_name, 	l.parent_contract_group_id, 	l.subclass_code, 	t.tran_code FROM ${EEDDWWDDBB1}.contract_group_xref_ld l,       ${AAPPLLIIDD1EENNVV}_contract_group_xref_t1 t WHERE  l.contract_group_xref_id= t.contract_group_xref_id AND    l.instance_id = t.instance_id  AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <  	CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS SUCCESS
WITH CTE AS 
(
SELECT t.contract_group_xref_id,t.instance_id, CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) AS iCat,
t.as_of_date_time,t.batch_nbr,t.tran_code
FROM ${EEDDWWDDBB1}.contract_group_xref_ld    AS l   
INNER JOIN ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_t1    AS t 
ON l.contract_group_xref_id = t.contract_group_xref_id  
AND l.instance_id = t.instance_id   
WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
)
INSERT 
INTO
TABLE
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_t2           SELECT
l.contract_group_xref_id,
l.instance_id,
t.as_of_date_time,
t.batch_nbr,
l.as_of_date_time,
l.batch_nbr,
l.created_by_name,
l.creation_date_time,
null,
l.included_contract_group_id,
l.included_contract_header_id,
l.last_update_date_time,
l.last_update_login_name,
l.last_updated_by_name,
l.parent_contract_group_id,
l.subclass_code,
t.tran_code  
FROM
${EEDDWWDDBB1}.contract_group_xref_ld    AS l   
INNER JOIN CTE AS t   
ON upper(trim(l.contract_group_xref_id)) = upper(trim(t.contract_group_xref_id))  
AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))   
AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) =  t.iCat;
