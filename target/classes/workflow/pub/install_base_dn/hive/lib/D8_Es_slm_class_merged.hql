--Original Query:
  ----DATABASE ${EEDDWWDDBB1}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB2};

--Original Query:
  ----DELETE FROM EEDDWWDDBB1.install_base_item_ld;

--Translated Query: SUCCESS

    TRUNCATE TABLE  ${EEDDWWDDBB2}.es_slm_class_ld; 

--Original Query:
  ----INSERT INTO EEDDWWDDBB1.install_base_item_ld
--SELECT *
---FROM EEDDWWDDBB1.install_base_item
--;

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB2}.es_slm_class_ld          SELECT
            A.*  
        FROM
            ${EEDDWWDDBB2}.es_slm_class A;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_es_slm_class_t1.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DELETE FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_es_slm_class_t1 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_es_slm_class_t1;

--Original Query:
  ----INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_es_slm_class_t1( instance_id ,prod_class_code ,alt_as_of_date_time,as_of_date_time,tran_code,update_date_time)SELECT EDW1.instance_id ,EDW1.text_01_value  <WM_COMMENT_START>prod_class_code<WM_COMMENT_END>,CAST(CAST(EDW1.alt_as_of_date_time AS VARCHAR(19)) AS TIMESTAMP(0)),CAST(CAST(EDW1.as_of_date_time AS VARCHAR(19)) AS TIMESTAMP(0)),EDW1.tran_code ,CURRENT_TIMESTAMP(0) <WM_COMMENT_START>update_date_time<WM_COMMENT_END>FROM ${EEDDWWDDBB2}.edw_custom_lookup EDW1,${EEDDWWDDBB2}.calendar_dates EDW2WHERE EDW1.phantom_table_name='es_slm_class_lup'AND EDW1.data_application_id='AAPPLLIIDDUUCC'AND EDW1.tran_code = 'U'AND ( DATE -28) BETWEEN EDW2.begin_period_date AND EDW2.end_period_dateAND EDW2.type_period= 'C'GROUP BY 1,2,3,4,5,6

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_es_slm_class_t1           SELECT
            EDW1.instance_id,
            EDW1.text_01_value,
            CAST (CAST (EDW1.alt_as_of_date_time AS VARCHAR(19)) AS timestamp),
            CAST (CAST (EDW1.as_of_date_time AS VARCHAR(19)) AS timestamp),
            EDW1.tran_code,
            CURRENT_TIMESTAMP() AS auto_c05  
        FROM
            ${EEDDWWDDBB2}.edw_custom_lookup    AS EDW1   CROSS 
        JOIN
            ${EEDDWWDDBB2}.calendar_dates    AS EDW2   
        WHERE
            upper(trim(EDW1.phantom_table_name)) = upper(trim('es_slm_class_lup'))  
            AND upper(trim(EDW1.data_application_id)) = upper(trim('D8'))   
            AND upper(trim(EDW1.tran_code)) = upper(trim('U'))   
            AND  (
                CAST (DATE_SUB( CURRENT_DATE() , 28 )  AS DATE) BETWEEN EDW2.begin_period_date AND EDW2.end_period_date 
            )   
            AND upper(trim(EDW2.type_period)) = upper(trim('C'))   
        GROUP BY
            EDW1.instance_id ,
            EDW1.text_01_value ,
            CAST (CAST (EDW1.alt_as_of_date_time AS VARCHAR(19)) AS timestamp) ,
            CAST (CAST (EDW1.as_of_date_time AS VARCHAR(19)) AS timestamp) ,
            EDW1.tran_code ,
            CURRENT_TIMESTAMP();
--Original Query:
  ----DATABASE ${EEDDWWDDBB1}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB2};

--Original Query:
  ----DELETE FROM EEDDWWDDBB1.install_base_item_ld;

--Translated Query: SUCCESS

    TRUNCATE TABLE  ${EEDDWWDDBB2}.es_slm_class_ld; 

--Original Query:
  ----INSERT INTO EEDDWWDDBB1.install_base_item_ld
--SELECT *
---FROM EEDDWWDDBB1.install_base_item
--;

--Translated Query: SUCCESS
INSERT INTO ${EEDDWWDDBB2}.es_slm_class_ld
SELECT
 EDW1.instance_id
,EDW1.prod_class_code
,EDW1.alt_as_of_date_time
,EDW1.as_of_date_time
,EDW1.tran_code
,EDW1.update_date_time
FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_es_slm_class_t1 EDW1
;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_es_slm_class.ren.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${EEDDWWDDBB}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB2};

--Original Query:
  ----RENAME TABLE es_slm_class_ld TO es_slm_class

--Translated Query: MANUAL

  ALTER TABLE es_slm_class RENAME TO es_slm_class_hd;
--Translated Query: SUCCESS
ALTER TABLE es_slm_class_ld RENAME TO es_slm_class;
--Translated Query: SUCCESS
ALTER TABLE es_slm_class_hd RENAME TO es_slm_class_ld;
