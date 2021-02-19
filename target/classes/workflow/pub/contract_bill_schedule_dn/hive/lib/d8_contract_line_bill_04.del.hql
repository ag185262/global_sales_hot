----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:39:53 
--Script Name: d8_contract_line_bill_04.del.sql
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

--Original Query: 503a15c5f707231aa2c5fc570b69bc51
  --DELETE A FROM contract_line_bill_ld A, ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t2 B WHERE A.instance_id = B.instance_id AND A.contract_line_id = B.contract_line_id  

--Translated Query: STATUS MANUAL

    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_line_bill_ld SELECT
            -- A.* 
        -- FROM
            -- ${EEDDWWDDBB1}.contract_line_bill_ld    AS A   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t2    AS B   
        -- WHERE
            -- NOT(A.instance_id = B.instance_id  
            -- AND A.contract_line_id = B.contract_line_id  )  
            -- OR A.instance_id IS NULL  
            -- OR B.instance_id IS NULL  
            -- OR A.contract_line_id IS NULL  
            -- OR B.contract_line_id IS NULL;

INSERT OVERWRITE
TABLE ${EEDDWWDDBB1}.contract_line_bill_ld 
SELECT A.* FROM
${EEDDWWDDBB1}.contract_line_bill_ld A   
LEFT OUTER JOIN
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t2    AS B   
ON
UPPER(TRIM(A.instance_id)) = UPPER(TRIM(B.instance_id))  
AND UPPER(TRIM(A.contract_line_id)) = UPPER(TRIM(B.contract_line_id))  
WHERE B.instance_id IS NULL  
AND B.contract_line_id IS NULL;