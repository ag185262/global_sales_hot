----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:42:56 
--Script Name: d8_contract_line_xref_t.del.sql
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

--Original Query: 4fc4bfa3004dceae198f86c3ed2bba02
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_xref_ml 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_ml;

--Original Query: a8008f5810f4ceccda0e52f20bf67820
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_xref_t1 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t1;

--Original Query: c841b145d2a8385dbb2df0e74c358822
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_xref_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t2;

--Original Query: 779de81ceeea0f813abcceb41fe71f06
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_xref_t3 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t3;
