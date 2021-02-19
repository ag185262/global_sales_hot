----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:41:47 
--Script Name: d8_contract_line_svc_supp_24.del.sql
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

--Original Query: 9d060527763642646067209071f7dd5f
  --DELETE A FROM contract_line_svc_supp_ld A, ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1 B WHERE A.instance_id = B.instance_id AND A.alt_contract_header_id = B.contract_header_id  

--Translated Query: STATUS MANUAL
--(Nikhil) Corrected Manually
    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_line_svc_supp_ld SELECT
            -- A.* 
        -- FROM
            -- ${EEDDWWDDBB1}.contract_line_svc_supp_ld    AS A   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1    AS B   
        -- WHERE
            -- NOT(A.instance_id = B.instance_id  
            -- AND A.alt_contract_header_id = B.contract_header_id  )  
            -- OR A.instance_id IS NULL  
            -- OR B.instance_id IS NULL  
            -- OR A.alt_contract_header_id IS NULL  
            -- OR B.contract_header_id IS NULL;
INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.contract_line_svc_supp_ld SELECT
            A.* 
        FROM
            ${EEDDWWDDBB1}.contract_line_svc_supp_ld    AS A   
LEFT OUTER JOIN
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1    AS B   
ON (trim(upper(A.instance_id)) = trim(upper(B.instance_id))  
            AND trim(upper(A.alt_contract_header_id)) = trim(upper(B.contract_header_id  )) )
WHERE B.instance_id IS NULL and B.contract_header_id  IS NULL;
