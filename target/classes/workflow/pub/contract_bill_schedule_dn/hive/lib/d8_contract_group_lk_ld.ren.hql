----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:37:10 
--Script Name: d8_contract_group_lk2.ren.sql
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

--Original Query: d6cd85327ab58361a2c0e504a3058dd3
  --RENAME TABLE contract_group_lk_ld TO contract_group_lk 

--Translated Query: STATUS MANUAL

--    ALTER TABLE ${EEDDWWDDBB1}.contract_group_lk_ld RENAME TO ${EEDDWWDDBB1}.contract_group_lk;
--(Nikhil) Replaced the Single Rename Statement with 3 Rename Statements

    ALTER TABLE ${EEDDWWDDBB1}.contract_group_lk RENAME TO ${EEDDWWDDBB1}.contract_group_lk_hd;
    ALTER TABLE ${EEDDWWDDBB1}.contract_group_lk_ld RENAME TO ${EEDDWWDDBB1}.contract_group_lk;
    ALTER TABLE ${EEDDWWDDBB1}.contract_group_lk_hd RENAME TO ${EEDDWWDDBB1}.contract_group_lk_ld;
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_group_lk SELECT * FROM ${EEDDWWDDBB1}.contract_group_lk_ld;
