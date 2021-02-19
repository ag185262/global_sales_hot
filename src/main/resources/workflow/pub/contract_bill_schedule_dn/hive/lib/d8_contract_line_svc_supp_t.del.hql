----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:42:21 
--Script Name: d8_contract_line_svc_supp_t.del.sql
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

--Original Query: 1db9750272c49f3ac623486714bb28db
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml;

--Original Query: 6f010117429a5aa8a58cdc4e3bc43667
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1;

--Original Query: 054398e4e642b9b7f1e2817b194487ed
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2;

--Original Query: af6e0d5ea97b3ca916d35b25749e71e1
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t3 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t3;
