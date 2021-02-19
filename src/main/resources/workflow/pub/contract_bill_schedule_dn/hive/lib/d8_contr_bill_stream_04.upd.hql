----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:38 
--Script Name: d8_contr_bill_stream_04.upd.sql
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
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contract_line_t3 FOR ACCESS  UPDATE ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2 SET   current_record_ind = 'D' WHERE  (    contr_bill_stream_id   ,instance_id)   IN  (SELECT contr_bill_stream_id        ,instance_id    FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t3  GROUP BY contr_bill_stream_id       ,instance_id    HAVING COUNT(*) = 1     )  

--Translated Query: STATUS MANUAL
--Added one more column for null condition in case statement
    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2 SELECT
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.contr_bill_stream_id AS contr_bill_stream_id ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.alt_as_of_date_time AS alt_as_of_date_time ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.alt_batch_nbr AS alt_batch_nbr ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.alt_contract_header_id AS alt_contract_header_id ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.as_of_date_time AS as_of_date_time ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.batch_nbr AS batch_nbr ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_amt_ent AS billing_amt_ent ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_period_cnt AS billing_period_cnt ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_period_uom_cnt AS billing_period_uom_cnt ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_period_uom_code AS billing_period_uom_code ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_seq_nbr AS billing_seq_nbr ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_stream_end_date AS billing_stream_end_date ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.billing_stream_start_date AS billing_stream_start_date ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.contract_header_id AS contract_header_id ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.contract_line_id AS contract_line_id ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.created_by_name AS created_by_name ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.creation_date_time AS creation_date_time ,
            CASE 
                --WHEN autoAlias_9.auto_c00 IS not null THEN 'D' 
                WHEN autoAlias_9.auto_c00 IS not null and autoAlias_9.auto_c01 is not null THEN 'D' 
                ELSE AAPPLLIIDD1EENNVV_contr_bill_stream_t2.current_record_ind 
            END AS current_record_ind ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.interface_offset_day_nbr AS interface_offset_day_nbr ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.invoice_offset_day_nbr AS invoice_offset_day_nbr ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.last_update_date_time AS last_update_date_time ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.last_update_login_name AS last_update_login_name ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.last_updated_by_name AS last_updated_by_name ,
            AAPPLLIIDD1EENNVV_contr_bill_stream_t2.tran_code AS tran_code 
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2 AS AAPPLLIIDD1EENNVV_contr_bill_stream_t2 
        Left Outer Join
            (
                SELECT
                    contr_bill_stream_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t3      
                GROUP BY
                    contr_bill_stream_id,
                    instance_id 
                HAVING
                    COUNT(*) = 1
            )autoAlias_9 
                ON (
                    upper(trim(autoAlias_9.auto_c00)) =  upper(trim(AAPPLLIIDD1EENNVV_contr_bill_stream_t2.contr_bill_stream_id ))
                    AND  upper(trim(autoAlias_9.auto_c01)) =  upper(trim(AAPPLLIIDD1EENNVV_contr_bill_stream_t2.instance_id))
                );
