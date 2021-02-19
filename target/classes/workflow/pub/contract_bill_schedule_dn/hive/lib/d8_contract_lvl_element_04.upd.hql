----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:26 
--Script Name: d8_contract_lvl_element_04.upd.sql
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
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t3 FOR ACCESS  UPDATE ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 SET current_record_ind = 'D' WHERE (level_element_id, instance_id) IN     (SELECT level_element_id, instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_lvl_element_t3     GROUP BY level_element_id, instance_id     HAVING COUNT(*) = 1     )  

--Translated Query: STATUS MANUAL
--(Nikhil: Corrected the case statement used in translated query)
    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 SELECT
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.level_element_id AS level_element_id ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.amount_due_date_time AS amount_due_date_time ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.alt_as_of_date_time AS alt_as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.alt_batch_nbr AS alt_batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.as_of_date_time AS as_of_date_time ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.batch_nbr AS batch_nbr ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.completed_date_time AS completed_date_time ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.created_by_name AS created_by_name ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.creation_date_time AS creation_date_time ,
            CASE 
                --WHEN autoAlias_205.auto_c00 IS not null THEN 'D' 
                WHEN autoAlias_205.auto_c00 IS not null and autoAlias_205.auto_c01 IS not null THEN 'D' 
                ELSE AAPPLLIIDD1EENNVV_contract_lvl_element_t2.current_record_ind 
            END AS current_record_ind ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.gl_receivable_date_time AS gl_receivable_date_time ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.interface_date_time AS interface_date_time ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.last_update_date_time AS last_update_date_time ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.last_updated_by_name AS last_updated_by_name ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.level_element_amt_ent AS level_element_amt_ent ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.period_start_date AS period_start_date ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.print_date_time AS print_date_time ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.rule_id AS rule_id ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.rule_start_date_time AS rule_start_date_time ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.sequence_nbr AS sequence_nbr ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.tran_code AS tran_code ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.transaction_date_time AS transaction_date_time ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.alt_contract_header_id AS alt_contract_header_id ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.contract_line_id AS contract_line_id ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.parent_contract_line_id AS parent_contract_line_id ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.period_end_date AS period_end_date ,
            AAPPLLIIDD1EENNVV_contract_lvl_element_t2.level_element_amt_us AS level_element_amt_us 
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t2 AS AAPPLLIIDD1EENNVV_contract_lvl_element_t2 
        Left Outer Join
            (
                SELECT
                    level_element_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_lvl_element_t3      
                GROUP BY
                    level_element_id,
                    instance_id 
                HAVING
                    COUNT(*) = 1
            )autoAlias_205 
                ON (
                    upper(trim(autoAlias_205.auto_c00)) =  upper(trim(AAPPLLIIDD1EENNVV_contract_lvl_element_t2.level_element_id)) 
                    AND  upper(trim(autoAlias_205.auto_c01)) =  upper(trim(AAPPLLIIDD1EENNVV_contract_lvl_element_t2.instance_id))
                );
