----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:28:54 
--Script Name: d8_contr_bill_stream_ld.ren.sql
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

--Original Query: 4e7a4e584a905df3d3ca9afb6174b767
  --RENAME TABLE contr_bill_stream_hd TO contr_bill_stream_ld   

--Translated Query: STATUS MANUAL

--    ALTER TABLE ${EEDDWWDDBB1}.contr_bill_stream_hd RENAME TO ${EEDDWWDDBB1}.contr_bill_stream_ld;
--(Nikhil) Replaced the Single Rename Statement with 3 Rename Statements

--    ALTER TABLE ${EEDDWWDDBB1}.contr_bill_stream RENAME TO ${EEDDWWDDBB1}.contr_bill_stream_hd;
--    ALTER TABLE ${EEDDWWDDBB1}.contr_bill_stream_ld RENAME TO ${EEDDWWDDBB1}.contr_bill_stream;
--    ALTER TABLE ${EEDDWWDDBB1}.contr_bill_stream_hd RENAME TO ${EEDDWWDDBB1}.contr_bill_stream_ld;
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contr_bill_stream SELECT * FROM ${EEDDWWDDBB1}.contr_bill_stream_ld;
