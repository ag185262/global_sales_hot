----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:34 
--Script Name: d8_contract_subline_bill_05.ins.sql
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
  --DELETE FROM contract_subline_bill_ld  WHERE (contract_subline_bill_id,instance_id) IN (SELECT contract_subline_bill_id,instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1     WHERE (contract_subline_bill_id,instance_id) NOT IN    (SELECT contract_subline_bill_id,instance_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2   WHERE current_record_ind = 'D'  )     )  

--Translated Query: STATUS MANUAL
--(Bala: Corrected the translated query,The table used in join was skipped, added that and also kept only limited columns in corrected query )
    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_subline_bill_ld SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.contract_subline_bill_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${EEDDWWDDBB1}.contract_subline_bill_ld      
            -- ) AS Q2 
                -- ON COALESCE( Q1.contract_subline_bill_id ,
            -- '1' ) = COALESCE( Q2.contract_subline_bill_id ,
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
            -- AND COALESCE( Q1.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.batch_nbr ,
            -- 1) = COALESCE( Q2.batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.bill_amt_ent ,
            -- 1) = COALESCE( Q2.bill_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.bill_avg_amt_ent ,
            -- 1) = COALESCE( Q2.bill_avg_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.bill_from_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.bill_from_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.bill_to_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.bill_to_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.contract_line_bill_id ,
            -- '1' ) = COALESCE( Q2.contract_line_bill_id ,
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
            -- Q2.contract_subline_bill_id IS NULL 
            -- AND Q2.instance_id IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.bill_amt_ent IS NULL 
            -- AND Q2.bill_avg_amt_ent IS NULL 
            -- AND Q2.bill_from_date_time IS NULL 
            -- AND Q2.bill_to_date_time IS NULL 
            -- AND Q2.contract_line_bill_id IS NULL 
            -- AND Q2.contract_line_id IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_update_login_name IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.tran_code IS NULL;
--Corrected Query:
        INSERT OVERWRITE
            TABLE ${EEDDWWDDBB1}.contract_subline_bill_ld
        select
        Q1.* 
        From
        ${EEDDWWDDBB1}.contract_subline_bill_ld Q1 
        Left Join
            (
                select
                    A.contract_subline_bill_id,
                    A.instance_id
                From
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1   As A 
                    Left Join
                    (
                        SELECT
                            contract_subline_bill_id,
                            instance_id 
                        FROM
                            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2  
                        WHERE
                            current_record_ind = 'D' 
                    )
                    As B 
                    On upper(trim(A.contract_subline_bill_id)) = upper(trim(b.contract_subline_bill_id)) 
                    And upper(trim(A.instance_id)) = upper(trim(B.instance_id))               
                Where
                    B.contract_subline_bill_id Is NULL 
                    And B.instance_id Is Null            
            )
            As Q2 
            On upper(trim(Q1.contract_subline_bill_id)) = upper(trim(Q2.contract_subline_bill_id ))
            And upper(trim(Q1.instance_id)) = upper(trim(Q2.instance_id))        
        Where
        Q2.contract_subline_bill_id Is NULL 
        And Q2.instance_id Is Null ;
--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1 FOR ACCESS  INSERT INTO contract_subline_bill_ld (     contract_subline_bill_id,   instance_id,    alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  bill_amt_ent,   bill_avg_amt_ent,   bill_from_date_time,    bill_to_date_time,  contract_line_bill_id,  contract_line_id,   created_by_name,    creation_date_time,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   tran_code ) SELECT      contract_subline_bill_id,   instance_id,    as_of_date_time,    batch_nbr,  as_of_date_time,    batch_nbr,  bill_amt_ent,   bill_avg_amt_ent,   bill_from_date_time,    bill_to_date_time,  contract_line_bill_id,  contract_line_id,   created_by_name,    creation_date_time,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   tran_code FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1 WHERE (contract_subline_bill_id,instance_id) NOT IN (SELECT  contract_subline_bill_id,instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2     WHERE current_record_ind = 'D'      )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_subline_bill_ld           SELECT
            contract_subline_bill_id,
            instance_id,
            as_of_date_time,
            batch_nbr,
            as_of_date_time,
            batch_nbr,
            bill_amt_ent,
            bill_avg_amt_ent,
            bill_from_date_time,
            bill_to_date_time,
            contract_line_bill_id,
            contract_line_id,
            created_by_name,
            creation_date_time,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t1 
        LEFT OUTER JOIN
            (
                SELECT
                    DISTINCT contract_subline_bill_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_subline_bill_t2     
                WHERE
                    upper(trim(current_record_ind)) = upper(trim('D')) 
            ) AS autoAlias_245 
                ON (
                    upper(trim(contract_subline_bill_id)) = upper(trim(autoAlias_245.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_245.AUTO_C01)) 
                ) 
        WHERE
            autoAlias_245.AUTO_C00 IS  NULL  
            AND autoAlias_245.AUTO_C01 IS  NULL;
