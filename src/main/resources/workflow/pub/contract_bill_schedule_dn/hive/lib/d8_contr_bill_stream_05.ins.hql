----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:39 
--Script Name: d8_contr_bill_stream_05.ins.sql
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



--Original Query:
  --DELETE FROM contr_bill_stream_ld WHERE  (contr_bill_stream_id, instance_id) IN  (SELECT contr_bill_stream_id, instance_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1   WHERE (contr_bill_stream_id, instance_id)   NOT IN      (SELECT contr_bill_stream_id, instance_id   FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2   WHERE current_record_ind = 'D')    ) 

--Translated Query: STATUS MANUAL
--(Bala: Corrected the translated query,The table used in join was skipped, added that and also kept only limited columns in corrected query )
    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contr_bill_stream_ld SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.contr_bill_stream_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${EEDDWWDDBB1}.contr_bill_stream_ld      
            -- ) AS Q2 
                -- ON COALESCE( Q1.contr_bill_stream_id ,
            -- '1' ) = COALESCE( Q2.contr_bill_stream_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.instance_id ,
            -- '1' ) = COALESCE( Q2.instance_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.alt_batch_nbr ,
            -- 1) = COALESCE( Q2.alt_batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.alt_contract_header_id ,
            -- '1' ) = COALESCE( Q2.alt_contract_header_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.batch_nbr ,
            -- 1) = COALESCE( Q2.batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.billing_amt_ent ,
            -- 1) = COALESCE( Q2.billing_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.billing_period_cnt ,
            -- 1) = COALESCE( Q2.billing_period_cnt ,
            -- 1) 
            -- AND COALESCE( Q1.billing_period_uom_cnt ,
            -- 1) = COALESCE( Q2.billing_period_uom_cnt ,
            -- 1) 
            -- AND COALESCE( Q1.billing_period_uom_code ,
            -- '1' ) = COALESCE( Q2.billing_period_uom_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.billing_seq_nbr ,
            -- 1) = COALESCE( Q2.billing_seq_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.billing_stream_end_date ,
            -- CURRENT_DATE() ) = COALESCE( Q2.billing_stream_end_date ,
            -- CURRENT_DATE() ) 
            -- AND COALESCE( Q1.billing_stream_start_date ,
            -- CURRENT_DATE() ) = COALESCE( Q2.billing_stream_start_date ,
            -- CURRENT_DATE() ) 
            -- AND COALESCE( Q1.contract_header_id ,
            -- '1' ) = COALESCE( Q2.contract_header_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.contract_line_id ,
            -- '1' ) = COALESCE( Q2.contract_line_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.created_by_name ,
            -- '1' ) = COALESCE( Q2.created_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.interface_offset_day_nbr ,
            -- 1) = COALESCE( Q2.interface_offset_day_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.invoice_offset_day_nbr ,
            -- 1) = COALESCE( Q2.invoice_offset_day_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_update_login_name ,
            -- '1' ) = COALESCE( Q2.last_update_login_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_updated_by_name ,
            -- '1' ) = COALESCE( Q2.last_updated_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.tran_code ,
            -- '1' ) = COALESCE( Q2.tran_code ,
            -- '1' ) 
        -- WHERE
            -- Q2.contr_bill_stream_id IS NULL 
            -- AND Q2.instance_id IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.alt_contract_header_id IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.billing_amt_ent IS NULL 
            -- AND Q2.billing_period_cnt IS NULL 
            -- AND Q2.billing_period_uom_cnt IS NULL 
            -- AND Q2.billing_period_uom_code IS NULL 
            -- AND Q2.billing_seq_nbr IS NULL 
            -- AND Q2.billing_stream_end_date IS NULL 
            -- AND Q2.billing_stream_start_date IS NULL 
            -- AND Q2.contract_header_id IS NULL 
            -- AND Q2.contract_line_id IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.interface_offset_day_nbr IS NULL 
            -- AND Q2.invoice_offset_day_nbr IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_update_login_name IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.tran_code IS NULL;


    INSERT OVERWRITE
            TABLE ${EEDDWWDDBB1}.contr_bill_stream_ld
    select
    Q1.* 
    From
    ${EEDDWWDDBB1}.contr_bill_stream_ld Q1 
    Left Join
        (
            select
                A.contr_bill_stream_id,
                A.instance_id
            From
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1  As A 
                Left Join
                (
                    SELECT
                        contr_bill_stream_id,
                        instance_id 
                    FROM
                        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2
                    WHERE
                        current_record_ind = 'D' 
                )
                As B 
                On upper(trim(A.contr_bill_stream_id)) = upper(trim(b.contr_bill_stream_id ))
                And upper(trim(A.instance_id)) = upper(trim(B.instance_id))
            Where
                B.contr_bill_stream_id Is NULL 
                And B.instance_id Is Null            
        )
        As Q2 
        On upper(trim(Q1.contr_bill_stream_id)) = upper(trim(Q2.contr_bill_stream_id)) 
        And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id))        
    Where
    Q2.contr_bill_stream_id Is NULL 
    And Q2.instance_id Is Null ;        
   
--Original Query:
  --LOCK TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 FOR ACCESS  INSERT INTO contr_bill_stream_ld (   contr_bill_stream_id   ,instance_id    ,alt_as_of_date_time    ,alt_batch_nbr  ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,billing_amt_ent    ,billing_period_cnt     ,billing_period_uom_cnt     ,billing_period_uom_code    ,billing_seq_nbr    ,billing_stream_end_date    ,billing_stream_start_date  ,contract_header_id     ,contract_line_id   ,created_by_name    ,creation_date_time     ,interface_offset_day_nbr   ,invoice_offset_day_nbr     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,tran_code ) SELECT      contr_bill_stream_id   ,instance_id    ,as_of_date_time    ,batch_nbr  ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,billing_amt_ent    ,billing_period_cnt     ,billing_period_uom_cnt     ,billing_period_uom_code    ,billing_seq_nbr    ,billing_stream_end_date    ,billing_stream_start_date  ,contract_header_id     ,contract_line_id   ,created_by_name    ,creation_date_time     ,interface_offset_day_nbr   ,invoice_offset_day_nbr     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,tran_code FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 WHERE  (contr_bill_stream_id, instance_id)     NOT IN  (SELECT  contr_bill_stream_id       ,instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2   WHERE current_record_ind = 'D'     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contr_bill_stream_ld           SELECT
            contr_bill_stream_id,
            instance_id,
            as_of_date_time,
            batch_nbr,
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
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 
        LEFT OUTER JOIN
            (
                SELECT
                    DISTINCT contr_bill_stream_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2     
                WHERE
                    upper(trim(current_record_ind)) = upper(trim('D')) 
            ) AS autoAlias_13 
                ON (
                    upper(trim(contr_bill_stream_id)) = upper(trim(autoAlias_13.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_13.AUTO_C01)) 
                ) 
        WHERE
            autoAlias_13.AUTO_C00 IS  NULL  
            AND autoAlias_13.AUTO_C01 IS  NULL;
