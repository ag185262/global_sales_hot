----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:39:45 
--Script Name: d8_contract_line_bill2.ren.sql
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

--Original Query: 67927ec1155975904f1b1c9369d6ce0a
  --RENAME TABLE contract_line_bill_ld TO contract_line_bill 

--Translated Query: STATUS MANUAL

    --ALTER TABLE ${EEDDWWDDBB1}.contract_line_bill_ld RENAME TO ${EEDDWWDDBB1}.contract_line_bill;
--(Nikhil) Replaced the Single Rename Statement with 3 Rename Statements

--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_bill RENAME TO ${EEDDWWDDBB1}.contract_line_bill_hd;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_bill_ld RENAME TO ${EEDDWWDDBB1}.contract_line_bill;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_bill_hd RENAME TO ${EEDDWWDDBB1}.contract_line_bill_ld;
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_line_bill SELECT * FROM ${EEDDWWDDBB1}.contract_line_bill_ld;
