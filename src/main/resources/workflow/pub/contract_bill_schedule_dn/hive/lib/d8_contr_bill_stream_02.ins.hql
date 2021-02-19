----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:38 
--Script Name: d8_contr_bill_stream_02.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB1}.contr_bill_stream_ld FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2 ( 	 contr_bill_stream_id 	,instance_id 	,alt_as_of_date_time 	,alt_batch_nbr 	,alt_contract_header_id 	,as_of_date_time 	,batch_nbr 	,billing_amt_ent 	,billing_period_cnt 	,billing_period_uom_cnt 	,billing_period_uom_code 	,billing_seq_nbr 	,billing_stream_end_date 	,billing_stream_start_date 	,contract_header_id 	,contract_line_id 	,created_by_name 	,creation_date_time 	,current_record_ind 	,interface_offset_day_nbr 	,invoice_offset_day_nbr 	,last_update_date_time 	,last_update_login_name 	,last_updated_by_name 	,tran_code ) SELECT  	 contr_bill_stream_id 	,instance_id 	,alt_as_of_date_time 	,alt_batch_nbr 	,alt_contract_header_id 	,as_of_date_time 	,batch_nbr 	,billing_amt_ent 	,billing_period_cnt 	,billing_period_uom_cnt 	,billing_period_uom_code 	,billing_seq_nbr 	,billing_stream_end_date 	,billing_stream_start_date 	,contract_header_id 	,contract_line_id 	,created_by_name 	,creation_date_time 	,'Y' 	,interface_offset_day_nbr 	,invoice_offset_day_nbr 	,last_update_date_time 	,last_update_login_name 	,last_updated_by_name 	,tran_code FROM	${EEDDWWDDBB1}.contr_bill_stream_ld WHERE	(contr_bill_stream_id 	,instance_id) 	IN 	(SELECT	 contr_bill_stream_id 		,instance_id 	 FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 	)  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2           SELECT
            contr_bill_stream_id,
            instance_id,
            alt_as_of_date_time,
            alt_batch_nbr,
            alt_contract_header_id,
            as_of_date_time,
            batch_nbr,
            billing_amt_ent,
            billing_period_cnt,
            billing_period_uom_cnt,
            billing_period_uom_code,
            billing_seq_nbr,
            billing_stream_end_date,
            billing_stream_start_date,
            contract_header_id,
            contract_line_id,
            created_by_name,
            creation_date_time,
            'Y',
            interface_offset_day_nbr,
            invoice_offset_day_nbr,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code  
        FROM
            ${EEDDWWDDBB1}.contr_bill_stream_ld 
        INNER JOIN
            (
                SELECT
                    DISTINCT contr_bill_stream_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 
            ) AS autoAlias_3 
                ON (
                    upper(trim(contr_bill_stream_id)) = upper(trim(autoAlias_3.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_3.AUTO_C01)) 
                );

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t3 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t3;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t3 ( 	 contr_bill_stream_id 	,instance_id 	,alt_contract_header_id 	,billing_amt_ent 	,billing_period_cnt 	,billing_period_uom_cnt 	,billing_period_uom_code 	,billing_seq_nbr 	,billing_stream_end_date 	,billing_stream_start_date 	,contract_header_id 	,contract_line_id 	,created_by_name 	,creation_date_time 	,interface_offset_day_nbr 	,invoice_offset_day_nbr 	,last_update_date_time 	,last_update_login_name 	,last_updated_by_name 	,tran_code ) SELECT  	 contr_bill_stream_id 	,instance_id 	,alt_contract_header_id 	,billing_amt_ent 	,billing_period_cnt 	,billing_period_uom_cnt 	,billing_period_uom_code 	,billing_seq_nbr 	,billing_stream_end_date 	,billing_stream_start_date 	,contract_header_id 	,contract_line_id 	,created_by_name 	,creation_date_time 	,interface_offset_day_nbr 	,invoice_offset_day_nbr 	,last_update_date_time 	,last_update_login_name 	,last_updated_by_name 	,tran_code FROM	${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2 WHERE	(contr_bill_stream_id 	,instance_id) 	IN 	(SELECT	 contr_bill_stream_id 		,instance_id 	 FROM ${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 	)  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t3           SELECT
            contr_bill_stream_id,
            instance_id,
            alt_contract_header_id,
            billing_amt_ent,
            billing_period_cnt,
            billing_period_uom_cnt,
            billing_period_uom_code,
            billing_seq_nbr,
            billing_stream_end_date,
            billing_stream_start_date,
            contract_header_id,
            contract_line_id,
            created_by_name,
            creation_date_time,
            interface_offset_day_nbr,
            invoice_offset_day_nbr,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t2 
        INNER JOIN
            (
                SELECT
                    DISTINCT contr_bill_stream_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_t1 
            ) AS autoAlias_5 
                ON (
                    upper(trim(contr_bill_stream_id)) = upper(trim(autoAlias_5.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_5.AUTO_C01)) 
                );
