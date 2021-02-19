----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:42:33 
--Script Name: d8_contract_line_xref2.ren.sql
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

--Original Query: de5807b4f224e8b9df730c39938b77e0
  --RENAME TABLE contract_line_xref_ld TO contract_line_xref 

--Translated Query: STATUS MANUAL

--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_xref_ld RENAME TO ${EEDDWWDDBB1}.contract_line_xref;
--(Nikhil) Replaced the Single Rename Statement with 3 Rename Statements

--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_xref RENAME TO ${EEDDWWDDBB1}.contract_line_xref_hd;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_xref_ld RENAME TO ${EEDDWWDDBB1}.contract_line_xref;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_xref_hd RENAME TO ${EEDDWWDDBB1}.contract_line_xref_ld;
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_line_xref SELECT * FROM ${EEDDWWDDBB1}.contract_line_xref_ld;
