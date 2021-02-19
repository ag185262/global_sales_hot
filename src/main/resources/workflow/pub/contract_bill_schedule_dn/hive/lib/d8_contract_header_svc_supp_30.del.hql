----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:38:59 
--Script Name: d8_contract_header_svc_supp_30.del.sql
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

--Original Query: 28d351a611f086604127356d0c647479
  --DELETE A FROM contract_header_svc_supp_ld A, ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1 B WHERE A.instance_id = B.instance_id AND A.contract_header_id = B.contract_header_id  

--Translated Query: STATUS MANUAL
--(Nikhil) Corrected the query
    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_header_svc_supp_ld SELECT
            -- A.* 
        -- FROM
            -- ${EEDDWWDDBB1}.contract_header_svc_supp_ld    AS A   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1    AS B   
        -- WHERE
            -- NOT(A.instance_id = B.instance_id  
            -- AND A.contract_header_id = B.contract_header_id  )  
            -- OR A.instance_id IS NULL  
            -- OR B.instance_id IS NULL  
            -- OR A.contract_header_id IS NULL  
            -- OR B.contract_header_id IS NULL;


INSERT OVERWRITE TABLE 
${EEDDWWDDBB1}.contract_header_svc_supp_ld SELECT
            A.* 
        FROM
            ${EEDDWWDDBB1}.contract_header_svc_supp_ld    AS A
LEFT OUTER JOIN
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1    AS B  
ON (upper(trim( A.instance_id)) = upper(trim( B.instance_id))  
  AND upper(trim( A.contract_header_id)) = upper(trim( B.contract_header_id)) )
WHERE B.instance_id IS NULL  AND B.contract_header_id is null;