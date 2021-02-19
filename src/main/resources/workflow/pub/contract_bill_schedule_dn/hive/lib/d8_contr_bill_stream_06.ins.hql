----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:28:51 
--Script Name: d8_contr_bill_stream_06.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query: 7e58aa2f355917bd712f410b9b1585c8
  --DATABASE ${TTMMPPDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${TTMMPPDDBB1} ;

--Original Query: 5b626c53415e7df84097438595d3d074
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1;

--Original Query: 7e2d79465f7bf35dd42a0678b08a9ff2
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 (    contr_bill_stream_id   ,instance_id    ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,billing_amt_ent    ,billing_period_cnt     ,billing_period_uom_cnt     ,billing_period_uom_code    ,billing_seq_nbr    ,billing_stream_end_date    ,billing_stream_start_date  ,contract_header_id     ,contract_line_id   ,created_by_name    ,creation_date_time     ,interface_offset_day_nbr   ,invoice_offset_day_nbr     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,tran_code ) SELECT      contr_bill_stream_id   ,instance_id    ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,billing_amt_ent    ,billing_period_cnt     ,billing_period_uom_cnt     ,billing_period_uom_code    ,billing_seq_nbr    ,billing_stream_end_date    ,billing_stream_start_date  ,contract_header_id     ,contract_line_id   ,created_by_name    ,creation_date_time     ,interface_offset_day_nbr   ,invoice_offset_day_nbr     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,tran_code FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml WHERE (CAST(as_of_date_time AS CHAR(26))||batch_nbr   ,contr_bill_stream_id   ,instance_id)   IN  (SELECT  MAX(CAST(as_of_date_time AS CHAR(26))||batch_nbr)      ,contr_bill_stream_id       ,instance_id    FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml  WHERE tran_code = 'D'   GROUP BY 2,3) AND   tran_code = 'D'  

--Translated Query: STATUS MANUAL

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
                    -- upper(trim(tran_code)) = upper(trim('D'))  
                -- GROUP BY
                    -- contr_bill_stream_id ,
                    -- instance_id 
            -- ) AS autoAlias_79 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_79.AUTO_C00 
                -- AND upper(trim(contr_bill_stream_id)) = upper(trim(autoAlias_79.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_79.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) = upper(trim('D'));
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
            tran_code,
            ROW_NUMBER() OVER (PARTITION BY contr_bill_stream_id, instance_id order by CONCAT (CAST (as_of_date_time AS CHAR(26)) ,batch_nbr) DESC) AS RNK
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml
        WHERE       upper(trim(tran_code)) = 'D'
        )
INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1          
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
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml EDW2
        INNER JOIN
           (
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
            tran_code
          FROM QQ1 WHERE RNK = 1
          )IBVL
on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND EDW2.contr_bill_stream_id =IBVL.contr_bill_stream_id 
   AND upper(trim(EDW2.tran_code)) ='D';      
 
--Original Query: c3a9db1d4b192b7431010d3424ab8267
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2;

--Original Query: 151523f0677d3abb4a8526153dd28ddc
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2 (     contr_bill_stream_id   ,instance_id    ,alt_as_of_date_time    ,alt_batch_nbr  ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,billing_amt_ent    ,billing_period_cnt     ,billing_period_uom_cnt     ,billing_period_uom_code    ,billing_seq_nbr    ,billing_stream_end_date    ,billing_stream_start_date  ,contract_header_id     ,contract_line_id   ,created_by_name    ,creation_date_time     ,interface_offset_day_nbr   ,invoice_offset_day_nbr     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,tran_code ) SELECT      l.contr_bill_stream_id     ,l.instance_id  ,t.as_of_date_time  ,t.batch_nbr    ,l.alt_contract_header_id   ,l.as_of_date_time  ,l.batch_nbr    ,l.billing_amt_ent  ,l.billing_period_cnt   ,l.billing_period_uom_cnt   ,l.billing_period_uom_code  ,l.billing_seq_nbr  ,l.billing_stream_end_date  ,l.billing_stream_start_date    ,l.contract_header_id   ,l.contract_line_id     ,l.created_by_name  ,l.creation_date_time   ,l.interface_offset_day_nbr     ,l.invoice_offset_day_nbr   ,l.last_update_date_time    ,l.last_update_login_name   ,l.last_updated_by_name     ,t.tran_code FROM   ${EEDDWWDDBB1}.contr_bill_stream_ld l,  ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 t WHERE   l.contr_bill_stream_id = t.contr_bill_stream_id AND l.instance_id = t.instance_id AND   CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <     CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS Manual

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2           SELECT
            -- l.contr_bill_stream_id,
            -- l.instance_id,
            -- t.as_of_date_time,
            -- t.batch_nbr,
            -- l.alt_contract_header_id,
            -- l.as_of_date_time,
            -- l.batch_nbr,
            -- l.billing_amt_ent,
            -- l.billing_period_cnt,
            -- l.billing_period_uom_cnt,
            -- l.billing_period_uom_code,
            -- l.billing_seq_nbr,
            -- l.billing_stream_end_date,
            -- l.billing_stream_start_date,
            -- l.contract_header_id,
            -- l.contract_line_id,
            -- l.created_by_name,
            -- l.creation_date_time,
            -- null,
            -- l.interface_offset_day_nbr,
            -- l.invoice_offset_day_nbr,
            -- l.last_update_date_time,
            -- l.last_update_login_name,
            -- l.last_updated_by_name,
            -- t.tran_code  
        -- FROM
            -- ${EEDDWWDDBB1}.contr_bill_stream_ld    AS l   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1    AS t   
        -- WHERE
            -- l.contr_bill_stream_id = t.contr_bill_stream_id  
            -- AND l.instance_id = t.instance_id   
            -- AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);

WITH QQ1 AS 
(
SELECT DISTINCT 
ld.contr_bill_stream_id,
ld.instance_id,
t1.as_of_date_time AS alt_as_of_date_time,
t1.batch_nbr AS alt_batch_nbr,
ld.alt_contract_header_id,
ld.as_of_date_time AS as_of_date_time,
ld.batch_nbr,
ld.billing_amt_ent,
ld.billing_period_cnt,
ld.billing_period_uom_cnt,
ld.billing_period_uom_code,
ld.billing_seq_nbr,
ld.billing_stream_end_date,
ld.billing_stream_start_date,
ld.contract_header_id,
ld.contract_line_id,
ld.created_by_name,
ld.creation_date_time,
null,
ld.interface_offset_day_nbr,
ld.invoice_offset_day_nbr,
ld.last_update_date_time,
ld.last_update_login_name,
ld.last_updated_by_name,
t1.tran_code  
FROM ${EEDDWWDDBB1}.contr_bill_stream_ld    ld
INNER JOIN
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 t1 
ON
upper(trim(ld.contr_bill_stream_id)) = upper(trim(t1.contr_bill_stream_id))  
AND upper(trim(ld.instance_id)) = upper(trim(t1.instance_id)) WHERE CONCAT (CAST (ld.as_of_date_time AS VARCHAR(26)) , ld.batch_nbr) <  CONCAT (CAST (t1.as_of_date_time AS VARCHAR(26)) , t1.batch_nbr))                       
INSERT INTO TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2
SELECT  * FROM QQ1;