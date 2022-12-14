----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:14 
--Script Name: d8_contract_line_bill_04.upd.sql
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

--(Nikhil: Corrected the case statement used in translated query)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_bill_t3 FOR ACCESS  UPDATE ${AAPPLLIIDD1EENNVV}_contract_line_bill_t2 SET current_record_ind = 'D' WHERE (contract_line_bill_id, instance_id) IN     (SELECT contract_line_bill_id, instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_line_bill_t3     GROUP BY contract_line_bill_id, instance_id     HAVING COUNT(*) = 1     )  

--Translated Query: STATUS SUCCESS

    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t2 SELECT
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.contract_line_bill_id AS contract_line_bill_id ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.alt_as_of_date_time AS alt_as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.alt_batch_nbr AS alt_batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.as_of_date_time AS as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.batch_nbr AS batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.bill_action_code AS bill_action_code ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.bill_amt_ent AS bill_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.bill_from_date_time AS bill_from_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.bill_to_date_time AS bill_to_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.bill_transaction_id AS bill_transaction_id ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.contract_line_id AS contract_line_id ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.created_by_name AS created_by_name ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.creation_date_time AS creation_date_time ,
            CASE 
                --WHEN autoAlias_137.auto_c00 IS not null THEN 'D' 
                WHEN autoAlias_137.auto_c00 IS not null and autoAlias_137.auto_c01 is not null  THEN 'D'
                ELSE AAPPLLIIDD1EENNVV_contract_line_bill_t2.current_record_ind 
            END AS current_record_ind ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.curr_code AS curr_code ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.last_update_date_time AS last_update_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.last_update_login_name AS last_update_login_name ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.last_updated_by_name AS last_updated_by_name ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.next_invoice_date_time AS next_invoice_date_time ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.sent_flag AS sent_flag ,
            AAPPLLIIDD1EENNVV_contract_line_bill_t2.tran_code AS tran_code 
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t2 AS AAPPLLIIDD1EENNVV_contract_line_bill_t2 
        Left Outer Join
            (
                SELECT
                    contract_line_bill_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_bill_t3      
                GROUP BY
                    contract_line_bill_id,
                    instance_id 
                HAVING
                    COUNT(*) = 1
            )autoAlias_137 
                ON (
                    upper(trim( autoAlias_137.auto_c00)) = upper(trim( AAPPLLIIDD1EENNVV_contract_line_bill_t2.contract_line_bill_id ))
                    AND upper(trim( autoAlias_137.auto_c01)) = upper(trim( AAPPLLIIDD1EENNVV_contract_line_bill_t2.instance_id))
                );
