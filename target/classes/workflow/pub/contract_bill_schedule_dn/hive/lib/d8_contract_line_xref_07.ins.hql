----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:24 
--Script Name: d8_contract_line_xref_07.ins.sql
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

--(Bala: Corrected the translated query,The table used in join was skipped, added that and also kept only limited columns in corrected query )
--Original Query:
  --DELETE FROM contract_line_xref_ld  WHERE (contract_line_xref_id, instance_id) IN (SELECT contract_line_xref_id, instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t2     )  

--Translated Query: STATUS SUCCESS

    -- /*INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_line_xref_ld SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.contract_line_xref_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${EEDDWWDDBB1}.contract_line_xref_ld      
            -- ) AS Q2 
                -- ON COALESCE( Q1.contract_line_xref_id ,
            -- '1' ) = COALESCE( Q2.contract_line_xref_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.instance_id ,
            -- '1' ) = COALESCE( Q2.instance_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.alt_contract_header_id ,
            -- '1' ) = COALESCE( Q2.alt_contract_header_id ,
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
            -- AND COALESCE( Q1.exception_flag ,
            -- '1' ) = COALESCE( Q2.exception_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.for_contract_line_id ,
            -- '1' ) = COALESCE( Q2.for_contract_line_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_update_login_name ,
            -- '1' ) = COALESCE( Q2.last_update_login_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_updated_by_name ,
            -- '1' ) = COALESCE( Q2.last_updated_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.nbr_of_items ,
            -- 1) = COALESCE( Q2.nbr_of_items ,
            -- 1) 
            -- AND COALESCE( Q1.object1_code ,
            -- '1' ) = COALESCE( Q2.object1_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.object1_id1 ,
            -- '1' ) = COALESCE( Q2.object1_id1 ,
            -- '1' ) 
            -- AND COALESCE( Q1.object1_id2 ,
            -- '1' ) = COALESCE( Q2.object1_id2 ,
            -- '1' ) 
            -- AND COALESCE( Q1.priced_item_flag ,
            -- '1' ) = COALESCE( Q2.priced_item_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.tran_code ,
            -- '1' ) = COALESCE( Q2.tran_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.uom_code ,
            -- '1' ) = COALESCE( Q2.uom_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.upg_orig_sys_ref_id ,
            -- '1' ) = COALESCE( Q2.upg_orig_sys_ref_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.upg_orig_sys_ref_name ,
            -- '1' ) = COALESCE( Q2.upg_orig_sys_ref_name ,
            -- '1' ) 
        -- WHERE
            -- Q2.contract_line_xref_id IS NULL 
            -- AND Q2.instance_id IS NULL 
            -- AND Q2.alt_contract_header_id IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.contract_header_id IS NULL 
            -- AND Q2.contract_line_id IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.exception_flag IS NULL 
            -- AND Q2.for_contract_line_id IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_update_login_name IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.nbr_of_items IS NULL 
            -- AND Q2.object1_code IS NULL 
            -- AND Q2.object1_id1 IS NULL 
            -- AND Q2.object1_id2 IS NULL 
            -- AND Q2.priced_item_flag IS NULL 
            -- AND Q2.tran_code IS NULL 
            -- AND Q2.uom_code IS NULL 
            -- AND Q2.upg_orig_sys_ref_id IS NULL 
            -- AND Q2.upg_orig_sys_ref_name IS NULL;*/
 --Corrected Query:
    INSERT OVERWRITE
            TABLE ${EEDDWWDDBB1}.contract_line_xref_ld   
    select
    Q1.* 
    From
    ${EEDDWWDDBB1}.contract_line_xref_ld Q1 
    Left Join
        (
            SELECT contract_line_xref_id, instance_id  FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t2         
        )
        As Q2 
        On trim(upper(Q1.contract_line_xref_id)) = trim(upper(Q2.contract_line_xref_id)) 
        And trim(upper(Q1.instance_id)) = trim(upper(Q2.instance_id))
    Where
    Q2.contract_line_xref_id Is NULL 
    And Q2.instance_id Is Null ; 
    
--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t2 FOR ACCESS  INSERT INTO contract_line_xref_ld (   contract_line_xref_id,  instance_id,    alt_contract_header_id,     alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_header_id,     contract_line_id,   created_by_name,    creation_date_time,     exception_flag,     for_contract_line_id,   last_update_date_time,  last_update_login_name,     last_updated_by_name,   nbr_of_items,   object1_code,   object1_id1,    object1_id2,    priced_item_flag,   tran_code,  uom_code,   upg_orig_sys_ref_id,    upg_orig_sys_ref_name ) SELECT      contract_line_xref_id,  instance_id,    alt_contract_header_id,     alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_header_id,     contract_line_id,   created_by_name,    creation_date_time,     exception_flag,     for_contract_line_id,   last_update_date_time,  last_update_login_name,     last_updated_by_name,   nbr_of_items,   object1_code,   object1_id1,    object1_id2,    priced_item_flag,   tran_code,  uom_code,   upg_orig_sys_ref_id,    upg_orig_sys_ref_name FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t2  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_line_xref_ld           SELECT
            contract_line_xref_id,
            instance_id,
            alt_contract_header_id,
            alt_as_of_date_time,
            alt_batch_nbr,
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
            upg_orig_sys_ref_name  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t2;
