----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:58 
--Script Name: d8_contract_bill_sched_dn_ld.ren.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${EEDDWWDDBB}  

--Translated Query: STATUS SUCCESS

    use ${EEDDWWDDBB} ;

--(Nikhil: Corrected Manually, Added three Rename statements instead of one)
--Original Query:
  --RENAME TABLE contract_bill_sched_dn_hd TO contract_bill_sched_dn_ld 

--Translated Query: STATUS FAILED
   
    ALTER TABLE   ${EEDDWWDDBB}.contract_bill_sched_dn RENAME TO ${EEDDWWDDBB}.contract_bill_sched_dn_hd;
    ALTER TABLE   ${EEDDWWDDBB}.contract_bill_sched_dn_ld RENAME TO ${EEDDWWDDBB}.contract_bill_sched_dn;
    ALTER TABLE  ${EEDDWWDDBB}.contract_bill_sched_dn_hd RENAME TO ${EEDDWWDDBB}.contract_bill_sched_dn_ld;

