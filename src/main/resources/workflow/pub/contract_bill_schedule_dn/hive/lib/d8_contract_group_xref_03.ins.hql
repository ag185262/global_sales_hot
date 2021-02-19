----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:02 
--Script Name: d8_contract_group_xref_03.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${TTMMPPDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${TTMMPPDDBB1} ;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_xref_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_group_xref_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_group_xref_t3 ( 	contract_group_xref_id, 	instance_id, 	created_by_name, 	creation_date_time, 	included_contract_group_id, 	included_contract_header_id, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	parent_contract_group_id, 	subclass_code, 	tran_code ) SELECT 	contract_group_xref_id, 	instance_id, 	created_by_name, 	creation_date_time, 	included_contract_group_id, 	included_contract_header_id, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	parent_contract_group_id, 	subclass_code, 	tran_code FROM ${AAPPLLIIDD1EENNVV}_contract_group_xref_t1 WHERE (contract_group_xref_id, instance_id) IN (SELECT      contract_group_xref_id,     instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_group_xref_t2     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_t3           SELECT
            contract_group_xref_id,
            instance_id,
            created_by_name,
            creation_date_time,
            included_contract_group_id,
            included_contract_header_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            parent_contract_group_id,
            subclass_code,
            tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_t1 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_group_xref_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_group_xref_t2 
            ) AS autoAlias_65 
                ON (
                    upper(trim(contract_group_xref_id)) = upper(trim(autoAlias_65.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_65.AUTO_C01)) 
                );
