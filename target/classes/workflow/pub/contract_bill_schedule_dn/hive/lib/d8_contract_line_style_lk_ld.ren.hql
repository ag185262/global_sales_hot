----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:41:06 
--Script Name: d8_contract_line_style_lk2.ren.sql
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

--Original Query: 60e813c50444e03165661a989023c544
  --RENAME TABLE contract_line_style_lk_ld TO contract_line_style_lk 

--Translated Query: STATUS MANUAL

   -- ALTER TABLE ${EEDDWWDDBB1}.contract_line_style_lk_ld RENAME TO ${EEDDWWDDBB1}.contract_line_style_lk;
--(Nikhil) Replaced the Single Rename Statement with 3 Rename Statements

--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_style_lk RENAME TO ${EEDDWWDDBB1}.contract_line_style_lk_hd;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_style_lk_ld RENAME TO ${EEDDWWDDBB1}.contract_line_style_lk;
--    ALTER TABLE ${EEDDWWDDBB1}.contract_line_style_lk_hd RENAME TO ${EEDDWWDDBB1}.contract_line_style_lk_ld;
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_line_style_lk  SELECT * FROM ${EEDDWWDDBB1}.contract_line_style_lk_ld;
