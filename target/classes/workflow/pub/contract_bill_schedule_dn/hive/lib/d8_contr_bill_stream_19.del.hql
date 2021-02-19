----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:28:52 
--Script Name: d8_contr_bill_stream_19.del.sql
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

--Original Query: df1746fa68466d04c287a18731577d9e
  --DELETE A FROM contr_bill_stream_ld A, ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1 B WHERE A.instance_id = B.instance_id AND A.alt_contract_header_id = B.contract_header_id  

--Translated Query: STATUS MANUAL
--(Nikhil) Corrected Manually
    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contr_bill_stream_ld SELECT
            -- A.* 
        -- FROM
            -- ${EEDDWWDDBB1}.contr_bill_stream_ld    AS A   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1    AS B   
        -- WHERE
            -- NOT(A.instance_id = B.instance_id  
            -- AND A.alt_contract_header_id = B.contract_header_id  )  
            -- OR A.instance_id IS NULL  
            -- OR B.instance_id IS NULL  
            -- OR A.alt_contract_header_id IS NULL  
            -- OR B.contract_header_id IS NULL;

 INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.contr_bill_stream_ld SELECT
            A.* 
        FROM
            ${EEDDWWDDBB1}.contr_bill_stream_ld    AS A
LEFT OUTER JOIN
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1    AS B  
ON (upper(trim(A.instance_id)) = upper(trim(B.instance_id)) 
            AND upper(trim(A.alt_contract_header_id)) = upper(trim(B.contract_header_id)))
where B.instance_id is null and B.contract_header_id is null;