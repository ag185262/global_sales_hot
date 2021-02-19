----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:40:05 
--Script Name: d8_contract_line_bill_t.del.sql
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

--Original Query: 9f215a726646bf5b76bc9d5d852d8a31
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_bill_ml 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_ml;

--Original Query: 1ea0df937c64ed1616e77848f3c92a4f
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_bill_t1 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t1;

--Original Query: 73f503046058a9c4108f15d0f3359598
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_bill_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t2;

--Original Query: 61236be46e77484f59a23273d62696e8
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_bill_t3 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t3;
