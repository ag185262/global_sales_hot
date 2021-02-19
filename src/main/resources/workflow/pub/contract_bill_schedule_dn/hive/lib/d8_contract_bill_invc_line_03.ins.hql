----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:40 
--Script Name: d8_contract_bill_invc_line_03.ins.sql
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
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t3 ( 	bill_transaction_line_id, 	instance_id, 	bill_instance_nbr, 	bill_transaction_id, 	contract_line_bill_id, 	contract_subline_bill_id, 	created_by_name, 	creation_date_time, 	invoice_line_amt_ent, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	tran_code, 	cycle_ref_seq_desc, 	invoice_date,                                                           	invoice_nbr,                                                            	invoice_type_code                                                      ) SELECT 	bill_transaction_line_id, 	instance_id, 	bill_instance_nbr, 	bill_transaction_id, 	contract_line_bill_id, 	contract_subline_bill_id, 	created_by_name, 	creation_date_time, 	invoice_line_amt_ent, 	last_update_date_time, 	last_update_login_name, 	last_updated_by_name, 	tran_code, 	cycle_ref_seq_desc,                                                     	invoice_date,                                                           	invoice_nbr,                                                            	invoice_type_code                                                       FROM ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1 WHERE (bill_transaction_line_id, instance_id) IN (SELECT      bill_transaction_line_id,     instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t2     )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t3           SELECT
            bill_transaction_line_id,
            instance_id,
            bill_instance_nbr,
            bill_transaction_id,
            contract_line_bill_id,
            contract_subline_bill_id,
            created_by_name,
            creation_date_time,
            invoice_line_amt_ent,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code,
            cycle_ref_seq_desc,
            invoice_date,
            invoice_nbr,
            invoice_type_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t1 
        INNER JOIN
            (
                SELECT
                    DISTINCT bill_transaction_line_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_bill_invc_line_t2 
            ) AS autoAlias_23 
                ON (
                    upper(trim(bill_transaction_line_id)) = upper(trim(autoAlias_23.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_23.AUTO_C01)) 
                );
