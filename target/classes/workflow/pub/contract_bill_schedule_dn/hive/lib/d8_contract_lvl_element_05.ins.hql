----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:27 
--Script Name: d8_contract_lvl_element_05.ins.sql
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
  --DELETE FROM contract_lvl_element_ld  WHERE (level_element_id, instance_id) IN (SELECT level_element_id, instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1     WHERE (level_element_id, instance_id) NOT IN   (SELECT level_element_id, instance_id   FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2    WHERE current_record_ind = 'D'  )     )  

--Translated Query: STATUS MANUAL
--(Bala: Corrected the translated query,The table used in join was skipped, added that and also kept only limited columns in corrected query )
  -- /*
    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_lvl_element_ld SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.contract_lvl_element_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${EEDDWWDDBB1}.contract_lvl_element_ld      
            -- ) AS Q2 
                -- ON COALESCE( Q1.instance_id ,
            -- '1' ) = COALESCE( Q2.instance_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.level_element_id ,
            -- '1' ) = COALESCE( Q2.level_element_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.alt_batch_nbr ,
            -- 1) = COALESCE( Q2.alt_batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.amount_due_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.amount_due_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.batch_nbr ,
            -- 1) = COALESCE( Q2.batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.completed_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.completed_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.created_by_name ,
            -- '1' ) = COALESCE( Q2.created_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.gl_receivable_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.gl_receivable_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.interface_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.interface_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_updated_by_name ,
            -- '1' ) = COALESCE( Q2.last_updated_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.level_element_amt_ent ,
            -- 1) = COALESCE( Q2.level_element_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.period_start_date ,
            -- CURRENT_DATE() ) = COALESCE( Q2.period_start_date ,
            -- CURRENT_DATE() ) 
            -- AND COALESCE( Q1.print_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.print_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.rule_id ,
            -- '1' ) = COALESCE( Q2.rule_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.rule_start_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.rule_start_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.sequence_nbr ,
            -- '1' ) = COALESCE( Q2.sequence_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.tran_code ,
            -- '1' ) = COALESCE( Q2.tran_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.transaction_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.transaction_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.alt_contract_header_id ,
            -- '1' ) = COALESCE( Q2.alt_contract_header_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.contract_line_id ,
            -- '1' ) = COALESCE( Q2.contract_line_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.parent_contract_line_id ,
            -- '1' ) = COALESCE( Q2.parent_contract_line_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.period_end_date ,
            -- CURRENT_DATE() ) = COALESCE( Q2.period_end_date ,
            -- CURRENT_DATE() ) 
            -- AND COALESCE( Q1.level_element_amt_us ,
            -- 1) = COALESCE( Q2.level_element_amt_us ,
            -- 1) 
        -- WHERE
            -- Q2.instance_id IS NULL 
            -- AND Q2.level_element_id IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.amount_due_date_time IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.completed_date_time IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.gl_receivable_date_time IS NULL 
            -- AND Q2.interface_date_time IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.level_element_amt_ent IS NULL 
            -- AND Q2.period_start_date IS NULL 
            -- AND Q2.print_date_time IS NULL 
            -- AND Q2.rule_id IS NULL 
            -- AND Q2.rule_start_date_time IS NULL 
            -- AND Q2.sequence_nbr IS NULL 
            -- AND Q2.tran_code IS NULL 
            -- AND Q2.transaction_date_time IS NULL 
            -- AND Q2.alt_contract_header_id IS NULL 
            -- AND Q2.contract_line_id IS NULL 
            -- AND Q2.parent_contract_line_id IS NULL 
            -- AND Q2.period_end_date IS NULL 
            -- AND Q2.level_element_amt_us IS NULL;
  -- */
--Corrected Query  :
    INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_lvl_element_ld 
    select
    Q1.* 
    From
   ${EEDDWWDDBB1}.contract_lvl_element_ld Q1 
    Left Join
        (
            select
                A.level_element_id,
                A.instance_id 
            From
               ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 As A 
                Left Join
                (
                    SELECT
                        level_element_id,
                        instance_id 
                    FROM
                       ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 
                    WHERE
                        current_record_ind = 'D' 
                )
                As B 
                On upper(trim(A.level_element_id)) = upper(trim(B.level_element_id ))
                And upper(trim( A.instance_id)) = upper(trim(B.instance_id ))
            Where
                B.level_element_id Is NULL 
                And B.instance_id Is Null 
        )
        As Q2 
        On upper(trim(Q1.level_element_id)) = upper(trim(Q2.level_element_id ))
        And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id)) 
    Where
    Q2.level_element_id Is NULL 
    And Q2.instance_id Is Null ;
--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 FOR ACCESS  INSERT INTO contract_lvl_element_ld (   instance_id,    level_element_id,   alt_as_of_date_time,    alt_batch_nbr,  amount_due_date_time,   as_of_date_time,    batch_nbr,  completed_date_time,    created_by_name,    creation_date_time,     gl_receivable_date_time,    interface_date_time,    last_update_date_time,  last_updated_by_name,   level_element_amt_ent,  period_start_date,  print_date_time,    rule_id,    rule_start_date_time,   sequence_nbr,   tran_code,  transaction_date_time,  alt_contract_header_id,     contract_line_id,   parent_contract_line_id,    period_end_date ) SELECT    instance_id,    level_element_id,   as_of_date_time,    batch_nbr,  amount_due_date_time,   as_of_date_time,    batch_nbr,  completed_date_time,    created_by_name,    creation_date_time,     gl_receivable_date_time,    interface_date_time,    last_update_date_time,  last_updated_by_name,   level_element_amt_ent,  period_start_date,  print_date_time,    rule_id,    rule_start_date_time,   sequence_nbr,   tran_code,  transaction_date_time,  alt_contract_header_id,     contract_line_id,   parent_contract_line_id,    period_end_date FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 WHERE ( level_element_id, instance_id ) NOT IN (SELECT  level_element_id, instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2     WHERE current_record_ind = 'D'      ) Group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_lvl_element_ld           SELECT
            instance_id,
            level_element_id,
            as_of_date_time,
            batch_nbr,
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
            null  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t1 
        LEFT OUTER JOIN
            (
                SELECT
                    DISTINCT level_element_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2     
                WHERE
                    upper(trim(current_record_ind)) = upper(trim('D')) 
            ) AS autoAlias_209 
                ON (
                    upper(trim(level_element_id)) = upper(trim(autoAlias_209.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_209.AUTO_C01)) 
                ) 
        WHERE
            autoAlias_209.AUTO_C00 IS  NULL  
            AND autoAlias_209.AUTO_C01 IS  NULL  
        GROUP BY
            instance_id ,
            level_element_id ,
            as_of_date_time ,
            batch_nbr ,
            amount_due_date_time ,
            as_of_date_time ,
            batch_nbr ,
            completed_date_time ,
            created_by_name ,
            creation_date_time ,
            gl_receivable_date_time ,
            interface_date_time ,
            last_update_date_time ,
            last_updated_by_name ,
            level_element_amt_ent ,
            period_start_date ,
            print_date_time ,
            rule_id ,
            rule_start_date_time ,
            sequence_nbr ,
            tran_code ,
            transaction_date_time ,
            alt_contract_header_id ,
            contract_line_id ,
            parent_contract_line_id ,
            period_end_date;
