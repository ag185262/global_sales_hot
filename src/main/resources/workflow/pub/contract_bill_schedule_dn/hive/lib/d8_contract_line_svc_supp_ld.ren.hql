----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:41:31 
--Script Name: d8_contract_line_svc_supp.ren.sql
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

--Original Query: 06ae0767582d67416428345370eaed38
  --RENAME TABLE contract_line_svc_supp_ld TO contract_line_svc_supp   

--Translated Query: STATUS FAILED

--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_svc_supp_ld RENAME TO ${EEDDWWDDBB1}.contract_line_svc_supp;
--(Nikhil) Replaced the Single Rename Statement with 3 Rename Statements

--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_svc_supp RENAME TO ${EEDDWWDDBB1}.contract_line_svc_supp_hd;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_svc_supp_ld RENAME TO ${EEDDWWDDBB1}.contract_line_svc_supp;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_svc_supp_hd RENAME TO ${EEDDWWDDBB1}.contract_line_svc_supp_ld;
INSERT OVERWRITE TABLE  ${EEDDWWDDBB1}.contract_line_svc_supp SELECT * FROM  ${EEDDWWDDBB1}.contract_line_svc_supp_ld;
