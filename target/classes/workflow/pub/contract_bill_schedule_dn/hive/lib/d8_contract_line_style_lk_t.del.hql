----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:41:24 
--Script Name: d8_contract_line_style_lk_t.del.sql
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

--Original Query: 32c6d77d91286ed04f72388e84f8bc9e
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_ml 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_ml;

--Original Query: 20c8f561475b6a173e294f14d38e6fae
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t1 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t1;

--Original Query: 62835b1696ddb8f90562aebd7cdeeada
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2;

--Original Query: 013bf1292bdf85d15eec5ebec331b85a
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t3 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t3;
