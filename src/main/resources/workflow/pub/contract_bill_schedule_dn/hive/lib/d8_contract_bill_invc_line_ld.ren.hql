----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:33:41 
--Script Name: d8_contract_bill_invc_line2.ren.sql
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

--Original Query: b4e49ba8674ec14a98b197963348aa09
  --RENAME TABLE contract_bill_invc_line_ld TO contract_bill_invc_line 

--Translated Query: STATUS MANUAL

--    ALTER TABLE ${EEDDWWDDBB1}.contract_bill_invc_line_ld RENAME TO ${EEDDWWDDBB1}.contract_bill_invc_line;
--(Nikhil) Replaced the Single Rename Statement with 3 Rename Statements

--    ALTER TABLE ${EEDDWWDDBB1}.contract_bill_invc_line RENAME TO ${EEDDWWDDBB1}.contract_bill_invc_line_hd;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_bill_invc_line_ld RENAME TO ${EEDDWWDDBB1}.contract_bill_invc_line;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_bill_invc_line_hd RENAME TO ${EEDDWWDDBB1}.contract_bill_invc_line_ld;
INSERT OVERWRITE TABLE  ${EEDDWWDDBB1}.contract_bill_invc_line SELECT * FROM  ${EEDDWWDDBB1}.contract_bill_invc_line_ld;
