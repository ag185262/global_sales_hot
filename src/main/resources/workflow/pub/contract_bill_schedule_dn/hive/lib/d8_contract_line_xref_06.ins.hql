----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:24 
--Script Name: d8_contract_line_xref_06.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_xref_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t1;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_xref_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_line_xref_t1 (     contract_line_xref_id,  instance_id,    alt_contract_header_id,     as_of_date_time,    batch_nbr,  contract_header_id,     contract_line_id,   created_by_name,    creation_date_time,     exception_flag,     for_contract_line_id,   last_update_date_time,  last_update_login_name,     last_updated_by_name,   nbr_of_items,   object1_code,   object1_id1,    object1_id2,    priced_item_flag,   tran_code,  uom_code,   upg_orig_sys_ref_id,    upg_orig_sys_ref_name ) SELECT      contract_line_xref_id,  instance_id,    alt_contract_header_id,     as_of_date_time,    batch_nbr,  contract_header_id,     contract_line_id,   created_by_name,    creation_date_time,     exception_flag,     for_contract_line_id,   last_update_date_time,  last_update_login_name,     last_updated_by_name,   nbr_of_items,   object1_code,   object1_id1,    object1_id2,    priced_item_flag,   tran_code,  uom_code,   upg_orig_sys_ref_id,    upg_orig_sys_ref_name FROM ${AAPPLLIIDD1EENNVV}_contract_line_xref_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,      contract_line_xref_id,      instance_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),    contract_line_xref_id,      instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_line_xref_ml     WHERE tran_code = 'D'     GROUP BY 2,3 ) AND tran_code = 'D'  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t1           SELECT
            -- contract_line_xref_id,
            -- instance_id,
            -- alt_contract_header_id,
            -- as_of_date_time,
            -- batch_nbr,
            -- contract_header_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- exception_flag,
            -- for_contract_line_id,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- nbr_of_items,
            -- object1_code,
            -- object1_id1,
            -- object1_id2,
            -- priced_item_flag,
            -- tran_code,
            -- uom_code,
            -- upg_orig_sys_ref_id,
            -- upg_orig_sys_ref_name  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- contract_line_xref_id AS auto_c01,
                    -- instance_id AS auto_c02  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_ml     
                -- WHERE
                    -- upper(trim(tran_code)) = upper(trim('D'))  
                -- GROUP BY
                    -- contract_line_xref_id ,
                    -- instance_id 
            -- ) AS autoAlias_195 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_195.AUTO_C00 
                -- AND upper(trim(contract_line_xref_id)) = upper(trim(autoAlias_195.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_195.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) = upper(trim('D'));

--Corrected Query: Transformed Query build with self join for max values.by using rank() function we can achieve the same result 
   with qq1 as (
 SELECT
                contract_line_xref_id, 
                instance_id, 
                alt_contract_header_id, 
                as_of_date_time, 
                batch_nbr, 
                contract_header_id, 
                contract_line_id, 
                created_by_name, 
                creation_date_time, 
                exception_flag, 
                for_contract_line_id, 
                last_update_date_time, 
                last_update_login_name, 
                last_updated_by_name, 
                nbr_of_items, 
                object1_code, 
                object1_id1, 
                object1_id2, 
                priced_item_flag, 
                tran_code, 
                uom_code, 
                upg_orig_sys_ref_id, 
                upg_orig_sys_ref_name, 
                ROW_NUMBER() over (PARTITION BY contract_line_xref_id , instance_id 
                    ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) ,  batch_nbr) DESC) As RNK 
            FROM
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_ml 
            WHERE
                upper(trim(tran_code)) = upper(trim('D')) ) 
INSERT INTO  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t1 
SELECT
            EDW2.contract_line_xref_id,
            EDW2.instance_id,
            EDW2.alt_contract_header_id,
            EDW2.as_of_date_time,
            EDW2.batch_nbr,
            EDW2.contract_header_id,
            EDW2.contract_line_id,
            EDW2.created_by_name,
            EDW2.creation_date_time,
            EDW2.exception_flag,
            EDW2.for_contract_line_id,
            EDW2.last_update_date_time,
            EDW2.last_update_login_name,
            EDW2.last_updated_by_name,
            EDW2.nbr_of_items,
            EDW2.object1_code,
            EDW2.object1_id1,
            EDW2.object1_id2,
            EDW2.priced_item_flag,
            EDW2.tran_code,
            EDW2.uom_code,
            EDW2.upg_orig_sys_ref_id,
            EDW2.upg_orig_sys_ref_name
FROM     ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_ml
 EDW2 
  INNER JOIN (
    SELECT 
            contract_line_xref_id,
            instance_id,
            alt_contract_header_id,
            as_of_date_time,
            batch_nbr,
            contract_header_id,
            contract_line_id,
            created_by_name,
            creation_date_time,
            exception_flag,
            for_contract_line_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            nbr_of_items,
            object1_code,
            object1_id1,
            object1_id2,
            priced_item_flag,
            tran_code,
            uom_code,
            upg_orig_sys_ref_id,
            upg_orig_sys_ref_name, 
            RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND EDW2.contract_line_xref_id =IBVL.contract_line_xref_id 
   AND upper(trim(EDW2.tran_code)) = upper(trim('D'));
    

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_xref_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t2;

--(Nikhil: Replaced the Inequality condition with CTE block)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_xref_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_line_xref_t2 (     contract_line_xref_id,  instance_id,    alt_contract_header_id,     alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_header_id,     contract_line_id,   created_by_name,    creation_date_time,     exception_flag,     for_contract_line_id,   last_update_date_time,  last_update_login_name,     last_updated_by_name,   nbr_of_items,   object1_code,   object1_id1,    object1_id2,    priced_item_flag,   tran_code,  uom_code,   upg_orig_sys_ref_id,    upg_orig_sys_ref_name ) SELECT  l.contract_line_xref_id,    l.instance_id,  l.alt_contract_header_id,   t.as_of_date_time,  t.batch_nbr,    l.as_of_date_time,  l.batch_nbr,    l.contract_header_id,   l.contract_line_id,     l.created_by_name,  l.creation_date_time,   l.exception_flag,   l.for_contract_line_id,     l.last_update_date_time,    l.last_update_login_name,   l.last_updated_by_name,     l.nbr_of_items,     l.object1_code,     l.object1_id1,  l.object1_id2,  l.priced_item_flag,     t.tran_code,    l.uom_code,     l.upg_orig_sys_ref_id,  l.upg_orig_sys_ref_name FROM ${EEDDWWDDBB1}.contract_line_xref_ld l,       ${AAPPLLIIDD1EENNVV}_contract_line_xref_t1 t WHERE  l.contract_line_xref_id= t.contract_line_xref_id AND    l.instance_id = t.instance_id  AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <    CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t2           SELECT
            -- l.contract_line_xref_id,
            -- l.instance_id,
            -- l.alt_contract_header_id,
            -- t.as_of_date_time,
            -- t.batch_nbr,
            -- l.as_of_date_time,
            -- l.batch_nbr,
            -- l.contract_header_id,
            -- l.contract_line_id,
            -- l.created_by_name,
            -- l.creation_date_time,
            -- null,
            -- l.exception_flag,
            -- l.for_contract_line_id,
            -- l.last_update_date_time,
            -- l.last_update_login_name,
            -- l.last_updated_by_name,
            -- l.nbr_of_items,
            -- l.object1_code,
            -- l.object1_id1,
            -- l.object1_id2,
            -- l.priced_item_flag,
            -- t.tran_code,
            -- l.uom_code,
            -- l.upg_orig_sys_ref_id,
            -- l.upg_orig_sys_ref_name  
        -- FROM
            -- ${EEDDWWDDBB1}.contract_line_xref_ld    AS l   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t1    AS t   
        -- WHERE
            -- l.contract_line_xref_id = t.contract_line_xref_id  
            -- AND l.instance_id = t.instance_id   
            -- AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) 
            --<  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);
--Translated Query:(Corrected Manually)
CREATE TABLE ${EEDDWWDDBB1}.contract_line_xref_ld_TEMP LIKE ${EEDDWWDDBB1}.contract_line_xref_ld STORED AS ORC;
WITH CTE AS 
(
SELECT t.contract_line_xref_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
 CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat
from  ${EEDDWWDDBB1}.contract_line_xref_ld_TEMP    AS l
INNER JOIN 
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t1    AS t   
ON        trim(upper(l.contract_line_xref_id)) = trim(upper(t.contract_line_xref_id))  
            AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))
            
            WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
)
INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t2           SELECT
            l.contract_line_xref_id,
            l.instance_id,
            l.alt_contract_header_id,
            t.as_of_date_time,
            t.batch_nbr,
            l.as_of_date_time,
            l.batch_nbr,
            l.contract_header_id,
            l.contract_line_id,
            l.created_by_name,
            l.creation_date_time,
            null,
            l.exception_flag,
            l.for_contract_line_id,
            l.last_update_date_time,
            l.last_update_login_name,
            l.last_updated_by_name,
            l.nbr_of_items,
            l.object1_code,
            l.object1_id1,
            l.object1_id2,
            l.priced_item_flag,
            t.tran_code,
            l.uom_code,
            l.upg_orig_sys_ref_id,
            l.upg_orig_sys_ref_name  
        FROM
            ${EEDDWWDDBB1}.contract_line_xref_ld_TEMP    AS l   
            INNER JOIN CTE as t
            ON
            trim(upper(l.contract_line_xref_id)) = trim(upper(t.contract_line_xref_id))  
            AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))   
            AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat;
