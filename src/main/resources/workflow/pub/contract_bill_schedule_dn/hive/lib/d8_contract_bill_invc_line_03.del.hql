----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:33:44 
--Script Name: d8_contract_bill_invc_line_03.del.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query: 8ac01af70ef22e7d586a4f5e5ecd1242
  --DATABASE ${EEDDWWDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${EEDDWWDDBB1} ;

--Original Query: 02c65b34104de74af72d7ed14ee31064
--DELETE FROM contract_bill_invc_line_ld A WHERE  (A.contract_line_bill_id,A.instance_id) IN  (SELECT B.contract_line_bill_id,B.instance_id   FROM contract_line_bill_ld B ,  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t2 C  WHERE B.contract_line_id = C.contract_line_id  AND B.instance_id= C.instance_id )  

--Translated Query: STATUS MANUAL
--Incorrect translation, missing one more more tables 

    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_bill_invc_line_ld SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.contract_bill_invc_line_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- A.*  
                -- FROM
                    -- ${EEDDWWDDBB1}.contract_bill_invc_line_ld    AS A    
            -- ) AS Q2 
                -- ON COALESCE( Q1.bill_transaction_line_id ,
            -- '1' ) = COALESCE( Q2.bill_transaction_line_id ,
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
            -- AND COALESCE( Q1.bill_instance_nbr ,
            -- '1' ) = COALESCE( Q2.bill_instance_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.bill_transaction_id ,
            -- '1' ) = COALESCE( Q2.bill_transaction_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.contract_line_bill_id ,
            -- '1' ) = COALESCE( Q2.contract_line_bill_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.contract_subline_bill_id ,
            -- '1' ) = COALESCE( Q2.contract_subline_bill_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.created_by_name ,
            -- '1' ) = COALESCE( Q2.created_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.invoice_line_amt_ent ,
            -- '1' ) = COALESCE( Q2.invoice_line_amt_ent ,
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
            -- AND COALESCE( Q1.tran_code ,
            -- '1' ) = COALESCE( Q2.tran_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.cycle_ref_seq_desc ,
            -- '1' ) = COALESCE( Q2.cycle_ref_seq_desc ,
            -- '1' ) 
            -- AND COALESCE( Q1.invoice_date ,
            -- CURRENT_DATE() ) = COALESCE( Q2.invoice_date ,
            -- CURRENT_DATE() ) 
            -- AND COALESCE( Q1.invoice_nbr ,
            -- '1' ) = COALESCE( Q2.invoice_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.invoice_type_code ,
            -- '1' ) = COALESCE( Q2.invoice_type_code ,
            -- '1' ) 
        -- WHERE
            -- Q2.bill_transaction_line_id IS NULL 
            -- AND Q2.instance_id IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.bill_instance_nbr IS NULL 
            -- AND Q2.bill_transaction_id IS NULL 
            -- AND Q2.contract_line_bill_id IS NULL 
            -- AND Q2.contract_subline_bill_id IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.invoice_line_amt_ent IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_update_login_name IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.tran_code IS NULL 
            -- AND Q2.cycle_ref_seq_desc IS NULL 
            -- AND Q2.invoice_date IS NULL 
            -- AND Q2.invoice_nbr IS NULL 
            -- AND Q2.invoice_type_code IS NULL;

INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_bill_invc_line_ld
SELECT ld.* FROM ${EEDDWWDDBB1}.contract_bill_invc_line_ld ld
LEFT OUTER JOIN
(SELECT 
DISTINCT B.contract_line_bill_id,B.instance_id   
FROM ${EEDDWWDDBB1}.contract_line_bill_ld B 
INNER JOIN
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t2 C  
ON upper(trim(B.contract_line_bill_id)) = upper(trim(C.contract_line_id))  
AND upper(trim(B.instance_id)) = upper(trim(C.instance_id)) 
)Q2
ON upper(trim(ld.contract_line_bill_id)) = upper(trim(Q2.contract_line_bill_id))  
AND upper(trim(ld.instance_id)) = upper(trim(Q2.instance_id)) 
WHERE
Q2.contract_line_bill_id IS NULL AND Q2.instance_id IS NULL;
