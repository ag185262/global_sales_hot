----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:40 
--Script Name: d8_contract_bill_invc_line_06.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1 (   bill_transaction_line_id,   instance_id,    as_of_date_time,    batch_nbr,  bill_instance_nbr,  bill_transaction_id,    contract_line_bill_id,  contract_subline_bill_id,   created_by_name,    creation_date_time,     invoice_line_amt_ent,   last_update_date_time,  last_update_login_name,     last_updated_by_name,   tran_code,  cycle_ref_seq_desc,                                                         invoice_date,                                                               invoice_nbr,                                                                invoice_type_code                                                       ) SELECT    bill_transaction_line_id,   instance_id,    as_of_date_time,    batch_nbr,  bill_instance_nbr,  bill_transaction_id,    contract_line_bill_id,  contract_subline_bill_id,   created_by_name,    creation_date_time,     invoice_line_amt_ent,   last_update_date_time,  last_update_login_name,     last_updated_by_name,   tran_code,  cycle_ref_seq_desc,                                                         invoice_date,                                                               invoice_nbr,                                                                invoice_type_code                                                  FROM ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,    bill_transaction_line_id,   instance_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),    bill_transaction_line_id,   instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_ml     WHERE tran_code = 'D'     GROUP BY 2,3 ) AND tran_code = 'D'  

--Translated Query: STATUS SUCCESS (Modified, Comments: Removed the additional check for Tran_Code = 'D' after the inner join block)

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1           SELECT
            -- bill_transaction_line_id,
            -- instance_id,
            -- as_of_date_time,
            -- batch_nbr,
            -- bill_instance_nbr,
            -- bill_transaction_id,
            -- contract_line_bill_id,
            -- contract_subline_bill_id,
            -- created_by_name,
            -- creation_date_time,
            -- invoice_line_amt_ent,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- tran_code,
            -- cycle_ref_seq_desc,
            -- invoice_date,
            -- invoice_nbr,
            -- invoice_type_code  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- bill_transaction_line_id AS auto_c01,
                    -- instance_id AS auto_c02  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_ml     
                -- WHERE
                    -- upper(trim(tran_code)) = upper(trim('D'))  
                -- GROUP BY
                    -- bill_transaction_line_id ,
                    -- instance_id 
            -- ) AS autoAlias_31 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_31.AUTO_C00 
                -- AND upper(trim(bill_transaction_line_id)) = upper(trim(autoAlias_31.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_31.AUTO_C02)) ); 
--           WHERE
  --             upper(trim(tran_code)) = upper(trim('D'));

--Translated Query: STATUS SUCCESS (Modified, Comments: Replaced inner join with ROW NUMBER function)

    with qq1 as (
SELECT
            bill_transaction_line_id,
            instance_id,
            as_of_date_time,
            batch_nbr,
            RANK() OVER 
            (PARTITION BY bill_transaction_line_id, instance_id 
            ORDER BY 
            (CONCAT (CAST (as_of_date_time AS CHAR(26)),batch_nbr)) DESC)  AS RNK,
            bill_instance_nbr,
            bill_transaction_id,
            contract_line_bill_id,
            contract_subline_bill_id,
            created_by_name,
            creation_date_time,
            invoice_line_amt_ent,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code,
            cycle_ref_seq_desc,
            invoice_date,
            invoice_nbr,
            invoice_type_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_ml 
            where upper(trim(tran_code)) = upper(trim('D')) ) 
INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1 
SELECT 
    EDW2.bill_transaction_line_id,
    EDW2.instance_id,
    EDW2.as_of_date_time,
    EDW2.batch_nbr,
    EDW2.bill_instance_nbr,
    EDW2.bill_transaction_id,
    EDW2.contract_line_bill_id,
    EDW2.contract_subline_bill_id,
    EDW2.created_by_name,
    EDW2.creation_date_time,
    EDW2.invoice_line_amt_ent,
    EDW2.last_update_date_time,
    EDW2.last_update_login_name,
    EDW2.last_updated_by_name,
    EDW2.tran_code,
    EDW2.cycle_ref_seq_desc,
    EDW2.invoice_date,
    EDW2.invoice_nbr,
    EDW2.invoice_type_code  
FROM 
  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_ml EDW2 
  INNER JOIN (
    SELECT 
      bill_transaction_line_id,
            instance_id,
            as_of_date_time,
            batch_nbr,
            bill_instance_nbr,
            bill_transaction_id,
            contract_line_bill_id,
            contract_subline_bill_id,
            created_by_name,
            creation_date_time,
            invoice_line_amt_ent,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code,
            cycle_ref_seq_desc,
            invoice_date,
            invoice_nbr,
            invoice_type_code, 
            RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL ON upper(trim(EDW2.instance_id)) = upper(trim(IBVL.instance_id ))
  WHERE upper(trim(EDW2.bill_transaction_line_id)) = upper(trim(IBVL.bill_transaction_line_id ))
  AND upper(trim(EDW2.tran_code)) = upper(trim('D'));

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t2;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t2 (   bill_transaction_line_id,   instance_id,    alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  bill_instance_nbr,  bill_transaction_id,    contract_line_bill_id,  contract_subline_bill_id,   created_by_name,    creation_date_time,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   tran_code,  cycle_ref_seq_desc,                                                         invoice_date,                                                               invoice_nbr,                                                                invoice_type_code                                                  ) SELECT     l.bill_transaction_line_id,     l.instance_id,  t.as_of_date_time,  t.batch_nbr,    l.as_of_date_time,  l.batch_nbr,    l.bill_instance_nbr,    l.bill_transaction_id,  l.contract_line_bill_id,    l.contract_subline_bill_id,     l.created_by_name,  l.creation_date_time,   l.last_update_date_time,    l.last_update_login_name,   l.last_updated_by_name,     t.tran_code,    l.cycle_ref_seq_desc,   l.invoice_date,     l.invoice_nbr,  l.invoice_type_code  FROM ${EEDDWWDDBB1}.contract_bill_invc_line_ld l,       ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1 t WHERE  l.bill_transaction_line_id = t.bill_transaction_line_id AND    l.instance_id = t.instance_id  AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <      CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS SUCCESS
WITH CTE AS 
(
SELECT t.bill_transaction_line_id,t.instance_id,CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) AS icat, t.batch_nbr, t.as_of_date_time, t.tran_code
FROM
${EEDDWWDDBB1}.contract_bill_invc_line_ld    AS l   
INNER JOIN ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1    AS t   
ON 
upper(trim(l.bill_transaction_line_id)) = upper(trim(t.bill_transaction_line_id )) 
AND upper(trim(l.instance_id)) = upper(trim(t.instance_id ))  
WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
)
INSERT 
INTO
TABLE
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t2           SELECT
l.bill_transaction_line_id,
l.instance_id,
t.as_of_date_time,
t.batch_nbr,
l.as_of_date_time,
l.batch_nbr,
l.bill_instance_nbr,
l.bill_transaction_id,
l.contract_line_bill_id,
l.contract_subline_bill_id,
l.created_by_name,
l.creation_date_time,
null,
null,
l.last_update_date_time,
l.last_update_login_name,
l.last_updated_by_name,
t.tran_code,
l.cycle_ref_seq_desc,
l.invoice_date,
l.invoice_nbr,
l.invoice_type_code  
FROM
${EEDDWWDDBB1}.contract_bill_invc_line_ld    AS l   INNER JOIN CTE AS t   
ON upper(trim(l.bill_transaction_line_id)) = upper(trim(t.bill_transaction_line_id )) 
AND upper(trim(l.instance_id)) = upper(trim(t.instance_id ))  
AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) =  t.icat;
