----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:52:15 
--Script Name: d8_contract_subline_bill2.ren.sql
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

--Original Query: ae52f33e23a0cf8932efd6dcd124d014
  --RENAME TABLE contract_subline_bill_ld TO contract_subline_bill 

--Translated Query: STATUS MANUAL

--    ALTER TABLE ${EEDDWWDDBB1}.contract_subline_bill_ld RENAME TO ${EEDDWWDDBB1}.contract_subline_bill;
--(Nikhil) Replaced the Single Rename Statement with 3 Rename Statements

--    ALTER TABLE ${EEDDWWDDBB1}.contract_subline_bill RENAME TO ${EEDDWWDDBB1}.contract_subline_bill_hd;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_subline_bill_ld RENAME TO ${EEDDWWDDBB1}.contract_subline_bill;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_subline_bill_hd RENAME TO ${EEDDWWDDBB1}.contract_subline_bill_ld;
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_subline_bill  SELECT * FROM ${EEDDWWDDBB1}.contract_subline_bill_ld;
