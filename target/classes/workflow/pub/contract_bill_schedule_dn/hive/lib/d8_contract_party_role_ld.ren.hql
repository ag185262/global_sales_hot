----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:45:02 
--Script Name: d8_contract_party_role2.ren.sql
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

--Original Query: 8adc64ca3fddac52e7d8adcf56b6e80d
  --RENAME TABLE contract_party_role_ld TO contract_party_role 

--Translated Query: STATUS MANUAL

--    ALTER TABLE ${EEDDWWDDBB1}.contract_party_role_ld RENAME TO ${EEDDWWDDBB1}.contract_party_role;
--(Nikhil) Replaced the Single Rename Statement with 3 Rename Statements

--    ALTER TABLE ${EEDDWWDDBB1}.contract_party_role RENAME TO ${EEDDWWDDBB1}.contract_party_role_hd;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_party_role_ld RENAME TO ${EEDDWWDDBB1}.contract_party_role;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_party_role_hd RENAME TO ${EEDDWWDDBB1}.contract_party_role_ld;
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_party_role SELECT * FROM ${EEDDWWDDBB1}.contract_party_role_ld;
