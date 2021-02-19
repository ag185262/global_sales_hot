----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:37:35 
--Script Name: d8_contract_group_xref2.ren.sql
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

--Original Query: c65247c0102d22486ff8e267f32b6fbf
  --RENAME TABLE contract_group_xref_ld TO contract_group_xref 

--Translated Query: STATUS  MANUAL

--    ALTER TABLE ${EEDDWWDDBB1}.contract_group_xref_ld RENAME TO ${EEDDWWDDBB1}.contract_group_xref;

--(Nikhil) Replaced the Single Rename Statement with 3 Rename Statements

--    ALTER TABLE ${EEDDWWDDBB1}.contract_group_xref RENAME TO ${EEDDWWDDBB1}.contract_group_xref_hd;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_group_xref_ld RENAME TO ${EEDDWWDDBB1}.contract_group_xref;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_group_xref_hd RENAME TO ${EEDDWWDDBB1}.contract_group_xref_ld;
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_group_xref  SELECT * FROM ${EEDDWWDDBB1}.contract_group_xref_ld ;
