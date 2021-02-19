----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:28:12 
--Script Name: d8_cfs_invtry_item_xref.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query: 7e58aa2f355917bd712f410b9b1585c8
  --DATABASE ${TTMMPPDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${TTMMPPDDBB1} ;

--Original Query: 45fac6bb9f9f887921c3197f2fa060a8
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_cfs_invtry_item_xref ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_invtry_item_xref;

--Original Query: 1b14016871afd1e0fa5667e13a09448c
  --LOCK TABLE ${EEDDWWDDBB2}.invtry_item FOR ACCESS  INSERT into ${AAPPLLIIDD1EENNVV}_cfs_invtry_item_xref (  instance_id ,inventory_item_id ,inventory_org_id ,offering_id ,offering_id_hyphenated ) SELECT  instance_id ,inventory_item_id ,inventory_org_id ,offering_id ,offering_id_hyphenated FROM ${EEDDWWDDBB2}.invtry_item  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_invtry_item_xref           SELECT
            instance_id,
            inventory_item_id,
            inventory_org_id,
            offering_id,
            offering_id_hyphenated  
        FROM
            ${EEDDWWDDBB2}.invtry_item;
