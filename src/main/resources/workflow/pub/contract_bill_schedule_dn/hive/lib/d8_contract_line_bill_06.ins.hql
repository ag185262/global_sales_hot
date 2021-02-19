----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:15 
--Script Name: d8_contract_line_bill_06.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_bill_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t1;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_bill_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_line_bill_t1 (     contract_line_bill_id,  instance_id,    as_of_date_time,    batch_nbr,  bill_action_code,   bill_amt_ent,   bill_from_date_time,    bill_to_date_time,  bill_transaction_id,    contract_line_id,   created_by_name,    creation_date_time,     curr_code,  last_update_date_time,  last_update_login_name,     last_updated_by_name,   next_invoice_date_time,     sent_flag,  tran_code ) SELECT      contract_line_bill_id,  instance_id,    as_of_date_time,    batch_nbr,  bill_action_code,   bill_amt_ent,   bill_from_date_time,    bill_to_date_time,  bill_transaction_id,    contract_line_id,   created_by_name,    creation_date_time,     curr_code,  last_update_date_time,  last_update_login_name,     last_updated_by_name,   next_invoice_date_time,     sent_flag,  tran_code FROM ${AAPPLLIIDD1EENNVV}_contract_line_bill_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,      contract_line_bill_id,      instance_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),    contract_line_bill_id,      instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_line_bill_ml     WHERE tran_code = 'D'     GROUP BY 2,3 ) AND tran_code = 'D'  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t1           SELECT
            -- contract_line_bill_id,
            -- instance_id,
            -- as_of_date_time,
            -- batch_nbr,
            -- bill_action_code,
            -- bill_amt_ent,
            -- bill_from_date_time,
            -- bill_to_date_time,
            -- bill_transaction_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- curr_code,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- next_invoice_date_time,
            -- sent_flag,
            -- tran_code  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- contract_line_bill_id AS auto_c01,
                    -- instance_id AS auto_c02  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_ml     
                -- WHERE
                    -- upper(trim(tran_code)) = upper(trim('D'))  
                -- GROUP BY
                    -- contract_line_bill_id ,
                    -- instance_id 
            -- ) AS autoAlias_143 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_143.AUTO_C00 
                -- AND upper(trim(contract_line_bill_id)) = upper(trim(autoAlias_143.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_143.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) = upper(trim('D'));
                
--Corrected Query: Transformed Query build with self join for max values.by using rank() function we can achieve the same result                
       with qq1 as (
 SELECT
                    contract_line_bill_id,
                    instance_id,
                    as_of_date_time,
                    batch_nbr,
                    bill_action_code,
                    bill_amt_ent,
                    bill_from_date_time,
                    bill_to_date_time,
                    bill_transaction_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    curr_code,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    next_invoice_date_time,
                    sent_flag,
                    tran_code,
                    ROW_NUMBER() over (PARTITION BY contract_line_bill_id, instance_id 
                    ORDER BY
                    CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK 
                    FROM
                        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_ml 
                    WHERE
                    upper(trim(tran_code)) = upper(trim('D'))  ) 
INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t1 
SELECT
        EDW2.contract_line_bill_id,
        EDW2.instance_id,
        EDW2.as_of_date_time,
        EDW2.batch_nbr,
        EDW2.bill_action_code,
        EDW2.bill_amt_ent,
        EDW2.bill_from_date_time,
        EDW2.bill_to_date_time,
        EDW2.bill_transaction_id,
        EDW2.contract_line_id,
        EDW2.created_by_name,
        EDW2.creation_date_time,
        EDW2.curr_code,
        EDW2.last_update_date_time,
        EDW2.last_update_login_name,
        EDW2.last_updated_by_name,
        EDW2.next_invoice_date_time,
        EDW2.sent_flag,
        EDW2.tran_code
FROM     ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_ml 
 EDW2 
  INNER JOIN (
    SELECT 
       contract_line_bill_id,
        instance_id,
        as_of_date_time,
        batch_nbr,
        bill_action_code,
        bill_amt_ent,
        bill_from_date_time,
        bill_to_date_time,
        bill_transaction_id,
        contract_line_id,
        created_by_name,
        creation_date_time,
        curr_code,
        last_update_date_time,
        last_update_login_name,
        last_updated_by_name,
        next_invoice_date_time,
        sent_flag,
        tran_code, 
            RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND upper(trim(EDW2.contract_line_bill_id ))=upper(trim(IBVL.contract_line_bill_id ))
  AND upper(trim(EDW2.tran_code)) = upper(trim('D'));              

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_bill_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t2;

--(Nikhil: Replaced the Inequality condition with CTE block)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_bill_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_line_bill_t2 (     contract_line_bill_id,  instance_id,    alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  bill_action_code,   bill_amt_ent,   bill_from_date_time,    bill_to_date_time,  bill_transaction_id,    contract_line_id,   created_by_name,    creation_date_time,     curr_code,  last_update_date_time,  last_update_login_name,     last_updated_by_name,   next_invoice_date_time,     sent_flag,  tran_code ) SELECT  l.contract_line_bill_id,    l.instance_id,  t.as_of_date_time,  t.batch_nbr,    l.as_of_date_time,  l.batch_nbr,    l.bill_action_code,     l.bill_amt_ent,     l.bill_from_date_time,  l.bill_to_date_time,    l.bill_transaction_id,  l.contract_line_id,     l.created_by_name,  l.creation_date_time,   l.curr_code,    l.last_update_date_time,    l.last_update_login_name,   l.last_updated_by_name,     l.next_invoice_date_time,   l.sent_flag,    t.tran_code FROM ${EEDDWWDDBB1}.contract_line_bill_ld l,       ${AAPPLLIIDD1EENNVV}_contract_line_bill_t1 t WHERE  l.contract_line_bill_id= t.contract_line_bill_id AND    l.instance_id = t.instance_id  AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <    CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t2           SELECT
            -- l.contract_line_bill_id,
            -- l.instance_id,
            -- t.as_of_date_time,
            -- t.batch_nbr,
            -- l.as_of_date_time,
            -- l.batch_nbr,
            -- l.bill_action_code,
            -- l.bill_amt_ent,
            -- l.bill_from_date_time,
            -- l.bill_to_date_time,
            -- l.bill_transaction_id,
            -- l.contract_line_id,
            -- l.created_by_name,
            -- l.creation_date_time,
            -- null,
            -- l.curr_code,
            -- l.last_update_date_time,
            -- l.last_update_login_name,
            -- l.last_updated_by_name,
            -- l.next_invoice_date_time,
            -- l.sent_flag,
            -- t.tran_code  
        -- FROM
            -- ${EEDDWWDDBB1}.contract_line_bill_ld    AS l   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t1    AS t   
        -- WHERE
            -- l.contract_line_bill_id = t.contract_line_bill_id  
            -- AND l.instance_id = t.instance_id   
            -- AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) 


--Translated Query:(Corrected Manually)
WITH CTE AS 
(
SELECT t.contract_line_bill_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
 CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat
from  ${EEDDWWDDBB1}.contract_line_bill_ld    AS l
INNER JOIN 
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t1    AS t   
ON        upper(trim(l.contract_line_bill_id)) = upper(trim(t.contract_line_bill_id )) 
            AND upper(trim(l.instance_id)) = upper(trim(t.instance_id ))  
            WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
)

INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t2           SELECT
            l.contract_line_bill_id,
            l.instance_id,
            t.as_of_date_time,
            t.batch_nbr,
            l.as_of_date_time,
            l.batch_nbr,
            l.bill_action_code,
            l.bill_amt_ent,
            l.bill_from_date_time,
            l.bill_to_date_time,
            l.bill_transaction_id,
            l.contract_line_id,
            l.created_by_name,
            l.creation_date_time,
            null,
            l.curr_code,
            l.last_update_date_time,
            l.last_update_login_name,
            l.last_updated_by_name,
            l.next_invoice_date_time,
            l.sent_flag,
            t.tran_code  
        FROM
            ${EEDDWWDDBB1}.contract_line_bill_ld    AS l  
           INNER JOIN CTE AS t
           ON upper(trim(t.contract_line_bill_id))=upper(trim(l.contract_line_bill_id))
           and upper(trim(t.instance_id))=upper(trim(l.instance_id))
           where CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat;