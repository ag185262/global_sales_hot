----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:22 
--Script Name: d8_contract_line_xref_03.ins.sql
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
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_xref_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_xref_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_line_xref_t3 ( 	contract_line_xref_id, 	instance_id, 	alt_contract_header_id, 	contract_header_id, 	contract_line_id, 	created_by_name, 	creation_date_time, 	exception_flag, 	for_contract_line_id, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	nbr_of_items, 	object1_code, 	object1_id1, 	object1_id2, 	priced_item_flag, 	tran_code, 	uom_code, 	upg_orig_sys_ref_id, 	upg_orig_sys_ref_name ) SELECT 	contract_line_xref_id, 	instance_id, 	alt_contract_header_id, 	contract_header_id, 	contract_line_id, 	created_by_name, 	creation_date_time, 	exception_flag, 	for_contract_line_id, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	nbr_of_items, 	object1_code, 	object1_id1, 	object1_id2, 	priced_item_flag, 	tran_code, 	uom_code, 	upg_orig_sys_ref_id, 	upg_orig_sys_ref_name FROM ${AAPPLLIIDD1EENNVV}_contract_line_xref_t1 WHERE (contract_line_xref_id, instance_id) IN (SELECT      contract_line_xref_id,     instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_line_xref_t2     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t3           SELECT
            contract_line_xref_id,
            instance_id,
            alt_contract_header_id,
            contract_header_id,
            contract_line_id,
            created_by_name,
            creation_date_time,
            exception_flag,
            for_contract_line_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            nbr_of_items,
            object1_code,
            object1_id1,
            object1_id2,
            priced_item_flag,
            tran_code,
            uom_code,
            upg_orig_sys_ref_id,
            upg_orig_sys_ref_name  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t1 
        INNER JOIN
            (
                SELECT
                    DISTINCT contract_line_xref_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_xref_t2 
            ) AS autoAlias_187 
                ON (
                    upper(trim(contract_line_xref_id)) = upper(trim(autoAlias_187.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_187.AUTO_C01)) 
                );
