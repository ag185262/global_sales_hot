----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:38 
--Script Name: d8_contr_bill_stream_01.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1;
--(Nikhil: Replaced the inequality Condition with CTE block)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 (    contr_bill_stream_id   ,instance_id    ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,billing_amt_ent    ,billing_period_cnt     ,billing_period_uom_cnt     ,billing_period_uom_code    ,billing_seq_nbr    ,billing_stream_end_date    ,billing_stream_start_date  ,contract_header_id     ,contract_line_id   ,created_by_name    ,creation_date_time     ,interface_offset_day_nbr   ,invoice_offset_day_nbr     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,tran_code ) SELECT      contr_bill_stream_id   ,instance_id    ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,billing_amt_ent    ,billing_period_cnt     ,billing_period_uom_cnt     ,billing_period_uom_code    ,billing_seq_nbr    ,billing_stream_end_date    ,billing_stream_start_date  ,contract_header_id     ,contract_line_id   ,created_by_name    ,creation_date_time     ,interface_offset_day_nbr   ,invoice_offset_day_nbr     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,tran_code FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml WHERE (CAST(as_of_date_time AS CHAR(26))||batch_nbr   ,contr_bill_stream_id   ,instance_id)   IN  (SELECT  MAX(CAST(as_of_date_time AS CHAR(26))||batch_nbr)      ,contr_bill_stream_id       ,instance_id    FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml  WHERE tran_code <> 'D'  GROUP BY 2,3) AND   tran_code <> 'D'  

--Translated Query: STATUS MANUAL
--Replaced the max function by ROW_NUMBER
    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1           SELECT
            -- contr_bill_stream_id,
            -- instance_id,
            -- alt_contract_header_id,
            -- as_of_date_time,
            -- batch_nbr,
            -- billing_amt_ent,
            -- billing_period_cnt,
            -- billing_period_uom_cnt,
            -- billing_period_uom_code,
            -- billing_seq_nbr,
            -- billing_stream_end_date,
            -- billing_stream_start_date,
            -- contract_header_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- interface_offset_day_nbr,
            -- invoice_offset_day_nbr,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- tran_code  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- contr_bill_stream_id AS auto_c01,
                    -- instance_id AS auto_c02  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml     
                -- WHERE
                    -- upper(trim(tran_code)) <> upper(trim('D'))  
                -- GROUP BY
                    -- contr_bill_stream_id ,
                    -- instance_id 
            -- ) AS autoAlias_1 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_1.AUTO_C00 
                -- AND upper(trim(contr_bill_stream_id)) = upper(trim(autoAlias_1.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_1.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) <> upper(trim('D'));
                
--Translated Query:(Corrected Manually)
with qq1 as (
  SELECT
                contr_bill_stream_id,
                instance_id,
                alt_contract_header_id,
                as_of_date_time,
                batch_nbr,
                billing_amt_ent,
                billing_period_cnt,
                billing_period_uom_cnt,
                billing_period_uom_code,
                billing_seq_nbr,
                billing_stream_end_date,
                billing_stream_start_date,
                contract_header_id,
                contract_line_id,
                created_by_name,
                creation_date_time,
                interface_offset_day_nbr,
                invoice_offset_day_nbr,
                last_update_date_time,
                last_update_login_name,
                last_updated_by_name,
                tran_code ,
                ROW_NUMBER() OVER(PARTITION BY contr_bill_stream_id ,instance_id  ORDER BY (CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                        batch_nbr)) DESC) RNK
               FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml 
                WHERE upper(trim(tran_code)) <> upper(trim('D')) 
        ) 
INSERT INTO    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 
SELECT
        EDW2.contr_bill_stream_id,
            EDW2.instance_id,
            EDW2.alt_contract_header_id,
            EDW2.as_of_date_time,
            EDW2.batch_nbr,
            EDW2.billing_amt_ent,
            EDW2.billing_period_cnt,
            EDW2.billing_period_uom_cnt,
            EDW2.billing_period_uom_code,
            EDW2.billing_seq_nbr,
            EDW2.billing_stream_end_date,
            EDW2.billing_stream_start_date,
            EDW2.contract_header_id,
            EDW2.contract_line_id,
            EDW2.created_by_name,
            EDW2.creation_date_time,
            EDW2.interface_offset_day_nbr,
            EDW2.invoice_offset_day_nbr,
            EDW2.last_update_date_time,
            EDW2.last_update_login_name,
            EDW2.last_updated_by_name,
            EDW2.tran_code
FROM      ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml   
 EDW2 
  INNER JOIN (
    SELECT 
        contr_bill_stream_id,
            instance_id,
            alt_contract_header_id,
            as_of_date_time,
            batch_nbr,
            billing_amt_ent,
            billing_period_cnt,
            billing_period_uom_cnt,
            billing_period_uom_code,
            billing_seq_nbr,
            billing_stream_end_date,
            billing_stream_start_date,
            contract_header_id,
            contract_line_id,
            created_by_name,
            creation_date_time,
            interface_offset_day_nbr,
            invoice_offset_day_nbr,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code  , 
           RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND upper(trim(EDW2.contr_bill_stream_id)) =upper(trim(IBVL.contr_bill_stream_id)) 
   AND upper(trim(EDW2.tran_code)) <> upper(trim('D'));