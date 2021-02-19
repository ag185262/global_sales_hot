----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:27 
--Script Name: d8_contract_lvl_element_06.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_lvl_element_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 (     instance_id,    level_element_id,   amount_due_date_time,   as_of_date_time,    batch_nbr,  completed_date_time,    created_by_name,    creation_date_time,     gl_receivable_date_time,    interface_date_time,    last_update_date_time,  last_updated_by_name,   level_element_amt_ent,  period_start_date,  print_date_time,    rule_id,    rule_start_date_time,   sequence_nbr,   tran_code,  transaction_date_time,  alt_contract_header_id,     contract_line_id,   parent_contract_line_id,    period_end_date ) SELECT    instance_id,    level_element_id,   amount_due_date_time,   as_of_date_time,    batch_nbr,  completed_date_time,    created_by_name,    creation_date_time,     gl_receivable_date_time,    interface_date_time,    last_update_date_time,  last_updated_by_name,   level_element_amt_ent,  period_start_date,  print_date_time,    rule_id,    rule_start_date_time,   sequence_nbr,   tran_code,  transaction_date_time,  alt_contract_header_id,     contract_line_id,   parent_contract_line_id,    period_end_date FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,      level_element_id,   instance_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),    level_element_id,   instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_ml     WHERE tran_code = 'D'     GROUP BY 2,3 ) AND tran_code = 'D'  

--Translated Query: STATUS MANUAL

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1           SELECT
            -- instance_id,
            -- level_element_id,
            -- amount_due_date_time,
            -- as_of_date_time,
            -- batch_nbr,
            -- completed_date_time,
            -- created_by_name,
            -- creation_date_time,
            -- gl_receivable_date_time,
            -- interface_date_time,
            -- last_update_date_time,
            -- last_updated_by_name,
            -- level_element_amt_ent,
            -- period_start_date,
            -- print_date_time,
            -- rule_id,
            -- rule_start_date_time,
            -- sequence_nbr,
            -- tran_code,
            -- transaction_date_time,
            -- alt_contract_header_id,
            -- contract_line_id,
            -- parent_contract_line_id,
            -- period_end_date,
            -- null  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- level_element_id AS auto_c01,
                    -- instance_id AS auto_c02  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_ml     
                -- WHERE
                    -- upper(trim(tran_code)) = upper(trim('D'))  
                -- GROUP BY
                    -- level_element_id ,
                    -- instance_id 
            -- ) AS autoAlias_211 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_211.AUTO_C00 
                -- AND upper(trim(level_element_id)) = upper(trim(autoAlias_211.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_211.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) = upper(trim('D'));
                
--Corrected Query: Transformed Query build with self join for max values.by using rank() function we can achieve the same result
    with qq1 as (
  SELECT
                 instance_id,
                 level_element_id,
                 amount_due_date_time,
                 as_of_date_time,
                 batch_nbr,
                 completed_date_time,
                 created_by_name,
                 creation_date_time,
                 gl_receivable_date_time,
                 interface_date_time,
                 last_update_date_time,
                 last_updated_by_name,
                 level_element_amt_ent,
                 period_start_date,
                 print_date_time,
                 rule_id,
                 rule_start_date_time,
                 sequence_nbr,
                 tran_code,
                 transaction_date_time,
                 alt_contract_header_id,
                 contract_line_id,
                 parent_contract_line_id,
                 period_end_date,
                 null,
                 ROW_NUMBER() over (PARTITION BY instance_id , level_element_id 
                    ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK
              FROM
                 ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_ml 
              WHERE
                 upper(trim(tran_code)) = upper(trim('D')) ) 
INSERT INTO  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1  
SELECT
           EDW2.instance_id,
           EDW2.level_element_id,
           EDW2.amount_due_date_time,
           EDW2.as_of_date_time,
           EDW2.batch_nbr,
           EDW2.completed_date_time,
           EDW2.created_by_name,
           EDW2.creation_date_time,
           EDW2.gl_receivable_date_time,
           EDW2.interface_date_time,
           EDW2.last_update_date_time,
           EDW2.last_updated_by_name,
           EDW2.level_element_amt_ent,
           EDW2.period_start_date,
           EDW2.print_date_time,
           EDW2.rule_id,
           EDW2.rule_start_date_time,
           EDW2.sequence_nbr,
           EDW2.tran_code,
           EDW2.transaction_date_time,
           EDW2.alt_contract_header_id,
           EDW2.contract_line_id,
           EDW2.parent_contract_line_id,
           EDW2.period_end_date,
           null
FROM      ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_ml 
 EDW2 
  INNER JOIN (
    SELECT 
            instance_id,
           level_element_id,
           amount_due_date_time,
           as_of_date_time,
           batch_nbr,
           completed_date_time,
           created_by_name,
           creation_date_time,
           gl_receivable_date_time,
           interface_date_time,
           last_update_date_time,
           last_updated_by_name,
           level_element_amt_ent,
           period_start_date,
           print_date_time,
           rule_id,
           rule_start_date_time,
           sequence_nbr,
           tran_code,
           transaction_date_time,
           alt_contract_header_id,
           contract_line_id,
           parent_contract_line_id,
           period_end_date, 
           RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND upper(trim(EDW2.level_element_id ))=upper(trim(IBVL.level_element_id ))
   AND upper(trim(EDW2.tran_code)) = upper(trim('D'));              

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2;

--(Nikhil: Replaced the Inequality condition with CTE block)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 (     instance_id,    level_element_id,   alt_as_of_date_time,    alt_batch_nbr,  amount_due_date_time,   as_of_date_time,    batch_nbr,  completed_date_time,    created_by_name,    creation_date_time,     gl_receivable_date_time,    interface_date_time,    last_update_date_time,  last_updated_by_name,   level_element_amt_ent,  period_start_date,  print_date_time,    rule_id,    rule_start_date_time,   sequence_nbr,   tran_code,  transaction_date_time,  alt_contract_header_id,     contract_line_id,   parent_contract_line_id,    period_end_date ) SELECT    l.instance_id,  l.level_element_id,     t.as_of_date_time,  t.batch_nbr,    l.amount_due_date_time,     l.as_of_date_time,  l.batch_nbr,    l.completed_date_time,  l.created_by_name,  l.creation_date_time,   l.gl_receivable_date_time,  l.interface_date_time,  l.last_update_date_time,    l.last_updated_by_name,     l.level_element_amt_ent,    l.period_start_date,    l.print_date_time,  l.rule_id,  l.rule_start_date_time,     l.sequence_nbr,     t.tran_code,    l.transaction_date_time,    l.alt_contract_header_id,   l.contract_line_id,     l.parent_contract_line_id,  l.period_end_date FROM ${EEDDWWDDBB1}.contract_lvl_element_ld l,       ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 t WHERE  l.level_element_id = t.level_element_id AND    l.instance_id = t.instance_id  AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <   CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr <WM_COMMENT_START>Temp Fix Starts<WM_COMMENT_END> AND l.contract_line_id <>' ' AND l.rule_id <> ' '   

--Translated Query: STATUS MANUAL

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2           SELECT
            -- l.instance_id,
            -- l.level_element_id,
            -- l.amount_due_date_time,
            -- t.as_of_date_time,
            -- t.batch_nbr,
            -- l.as_of_date_time,
            -- l.batch_nbr,
            -- l.completed_date_time,
            -- l.created_by_name,
            -- l.creation_date_time,
            -- null,
            -- l.gl_receivable_date_time,
            -- l.interface_date_time,
            -- l.last_update_date_time,
            -- l.last_updated_by_name,
            -- l.level_element_amt_ent,
            -- l.period_start_date,
            -- l.print_date_time,
            -- l.rule_id,
            -- l.rule_start_date_time,
            -- l.sequence_nbr,
            -- t.tran_code,
            -- l.transaction_date_time,
            -- l.alt_contract_header_id,
            -- l.contract_line_id,
            -- l.parent_contract_line_id,
            -- l.period_end_date,
            -- null  
        -- FROM
            -- ${EEDDWWDDBB1}.contract_lvl_element_ld    AS l   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1    AS t   
        -- WHERE
            -- l.level_element_id = t.level_element_id  
            -- AND l.instance_id = t.instance_id   
            -- AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)   
            -- AND upper(trim(l.contract_line_id)) <> upper(trim(' '))   
            -- AND upper(trim(l.rule_id)) <> upper(trim(' '));

--Translated Query:(Corrected Manually)
WITH CTE AS 
(
SELECT t.level_element_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
 CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat,
 upper(trim(l.contract_line_id)) AS cl_Id,upper(trim(l.rule_id)) AS rl_id
from  ${EEDDWWDDBB1}.contract_lvl_element_ld    AS l
INNER JOIN 
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1    AS t   
ON        upper(trim(l.level_element_id)) = upper(trim(t.level_element_id )) 
            AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))
            
            WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
            AND upper(trim(l.contract_line_id)) <> upper(trim(' '))   
            AND upper(trim(l.rule_id)) <> upper(trim(' '))
)

INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2           SELECT
            l.instance_id,
            l.level_element_id,
            l.amount_due_date_time,
            t.as_of_date_time,
            t.batch_nbr,
            l.as_of_date_time,
            l.batch_nbr,
            l.completed_date_time,
            l.created_by_name,
            l.creation_date_time,
            null,
            l.gl_receivable_date_time,
            l.interface_date_time,
            l.last_update_date_time,
            l.last_updated_by_name,
            l.level_element_amt_ent,
            l.period_start_date,
            l.print_date_time,
            l.rule_id,
            l.rule_start_date_time,
            l.sequence_nbr,
            t.tran_code,
            l.transaction_date_time,
            l.alt_contract_header_id,
            l.contract_line_id,
            l.parent_contract_line_id,
            l.period_end_date,
            null  
        FROM
            ${EEDDWWDDBB1}.contract_lvl_element_ld    AS l   
            INNER JOIN CTE AS t
            ON
            upper(trim(l.level_element_id)) = upper(trim(t.level_element_id ))
            AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))   
            AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) =t.iCat   
            AND upper(trim(l.contract_line_id)) =t.cl_Id
            AND upper(trim(l.rule_id)) =t.rl_id;

--Original Query:
  --COLLECT STATS  ttmp.d8t_contract_lvl_element_t1 COLUMN level_element_id 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.d8t_contract_lvl_element_t1 COMPUTE STATISTICS  FOR COLUMNS level_element_id;

--Original Query:
  --COLLECT STATS  ttmp.d8t_contract_lvl_element_t1 COLUMN instance_id 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.d8t_contract_lvl_element_t1 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--(Nikhil: Replaced the Inequality condition with CTE block)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 (  instance_id,  level_element_id,  alt_as_of_date_time,  alt_batch_nbr,  amount_due_date_time,  as_of_date_time,  batch_nbr,  completed_date_time,  created_by_name,  creation_date_time,  gl_receivable_date_time,  interface_date_time,  last_update_date_time,  last_updated_by_name,  level_element_amt_ent,  period_start_date,  print_date_time,  rule_id,  rule_start_date_time,  sequence_nbr,  tran_code,  transaction_date_time,  alt_contract_header_id,  contract_line_id,  parent_contract_line_id,  period_end_date ) SELECT  l.instance_id,  l.level_element_id,  t.as_of_date_time,  t.batch_nbr,  l.amount_due_date_time,  l.as_of_date_time,  l.batch_nbr,  l.completed_date_time,  l.created_by_name,  l.creation_date_time,  l.gl_receivable_date_time,  l.interface_date_time,  l.last_update_date_time,  l.last_updated_by_name,  l.level_element_amt_ent,  l.period_start_date,  l.print_date_time,  l.rule_id,  l.rule_start_date_time,  l.sequence_nbr,  t.tran_code,  l.transaction_date_time,  l.alt_contract_header_id,  l.contract_line_id,  l.parent_contract_line_id,  l.period_end_date FROM ${EEDDWWDDBB1}.contract_lvl_element_ld l,      ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 t WHERE  l.level_element_id = t.level_element_id AND    l.instance_id = t.instance_id AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <         CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr AND l.contract_line_id ='' AND t.contract_line_id = ''  

--Translated Query: STATUS MANUAL

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2           SELECT
            -- l.instance_id,
            -- l.level_element_id,
            -- l.amount_due_date_time,
            -- t.as_of_date_time,
            -- t.batch_nbr,
            -- l.as_of_date_time,
            -- l.batch_nbr,
            -- l.completed_date_time,
            -- l.created_by_name,
            -- l.creation_date_time,
            -- null,
            -- l.gl_receivable_date_time,
            -- l.interface_date_time,
            -- l.last_update_date_time,
            -- l.last_updated_by_name,
            -- l.level_element_amt_ent,
            -- l.period_start_date,
            -- l.print_date_time,
            -- l.rule_id,
            -- l.rule_start_date_time,
            -- l.sequence_nbr,
            -- t.tran_code,
            -- l.transaction_date_time,
            -- l.alt_contract_header_id,
            -- l.contract_line_id,
            -- l.parent_contract_line_id,
            -- l.period_end_date,
            -- null  
        -- FROM
            -- ${EEDDWWDDBB1}.contract_lvl_element_ld    AS l   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1    AS t   
        -- WHERE
            -- l.level_element_id = t.level_element_id  
            -- AND l.instance_id = t.instance_id   
            -- AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)   
            -- AND upper(trim(l.contract_line_id)) = upper(trim(''))   
            -- AND t.contract_line_id = '';

--Translated Query:(Corrected Manually)
WITH CTE AS 
(
SELECT t.level_element_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
 CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat,
 upper(trim(l.contract_line_id)) AS cl_Id,upper(trim(l.rule_id)) AS rl_id
from  ${EEDDWWDDBB1}.contract_lvl_element_ld    AS l
INNER JOIN 
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1    AS t   
ON        upper(trim(l.level_element_id)) = upper(trim(t.level_element_id  ))
            AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))
            
            WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
            AND upper(trim(l.contract_line_id)) <> upper(trim(' '))   
            AND t.contract_line_id = ''
)

INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2           SELECT
            l.instance_id,
            l.level_element_id,
            l.amount_due_date_time,
            t.as_of_date_time,
            t.batch_nbr,
            l.as_of_date_time,
            l.batch_nbr,
            l.completed_date_time,
            l.created_by_name,
            l.creation_date_time,
            null,
            l.gl_receivable_date_time,
            l.interface_date_time,
            l.last_update_date_time,
            l.last_updated_by_name,
            l.level_element_amt_ent,
            l.period_start_date,
            l.print_date_time,
            l.rule_id,
            l.rule_start_date_time,
            l.sequence_nbr,
            t.tran_code,
            l.transaction_date_time,
            l.alt_contract_header_id,
            l.contract_line_id,
            l.parent_contract_line_id,
            l.period_end_date,
            null  
        FROM
            ${EEDDWWDDBB1}.contract_lvl_element_ld    AS l   
            INNER JOIN CTE as t 
            ON 
           upper(trim( l.level_element_id)) = upper(trim(t.level_element_id  ))
            AND upper(trim(l.instance_id)) =upper(trim( t.instance_id   ))
            where  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat
            AND upper(trim(l.contract_line_id)) = t.rl_id;