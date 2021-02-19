----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:44 
--Script Name: d8_contract_bill_sched_dn_23.ins.sql
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
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 column (service_line_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 COMPUTE STATISTICS  FOR COLUMNS service_line_id;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 column (instance_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 column (service_line_style_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 COMPUTE STATISTICS  FOR COLUMNS service_line_style_id;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB}.contract_line_style_lk_ld FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 FOR ACCESS   CREATE VOLATILE table EDW1 AS (SELECT  V1.contract_header_id ,V1.instance_id ,V1.service_line_id ,V1.service_line_nbr ,V1.service_line_style_id ,V2.line_style_name ,V1.serviced_qty  FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 V1 LEFT OUTER JOIN      ${EEDDWWDDBB}.contract_line_style_lk_ld V2 ON   V1.instance_id = V2.instance_id AND  V1.service_line_style_id = V2.contract_line_style_id AND  V2.language_code = 'US' AND  V2.tran_code <> 'D') WITH DATA PRIMARY INDEX (service_line_id, instance_id) ON COMMIT preserve rows 

--Translated Query: STATUS SUCCESS
	DROP TABLE IF EXISTS  ${TTMMPPDDBB1}.EDW1;
    CREATE  TABLE IF NOT EXISTS  ${TTMMPPDDBB1}.EDW1 STORED AS ORC        AS     SELECT
        V1.contract_header_id,
        V1.instance_id,
        V1.service_line_id,
        V1.service_line_nbr,
        V1.service_line_style_id,
        V2.line_style_name,
        V1.serviced_qty  
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22    AS V1   
    LEFT OUTER  JOIN
        ${EEDDWWDDBB}.contract_line_style_lk_ld    AS V2    
            ON V1.instance_id = V2.instance_id  
            AND V1.service_line_style_id = V2.contract_line_style_id   
            AND upper(trim(V2.language_code)) = upper(trim('US'))   
            AND upper(trim(V2.tran_code)) <> upper(trim('D'));

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t22 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23 (  contract_header_id ,instance_id ,service_line_id ,service_line_nbr ,service_line_style_id ,service_line_style_name ,tax_exempt_status_code ,tax_exemption_id ,vat_tax_id ,serviced_qty ) SELECT   V3.contract_header_id ,V3.instance_id ,V3.service_line_id ,V3.service_line_nbr ,V3.service_line_style_id <WM_COMMENT_START> service_line_style_name <WM_COMMENT_END> ,V3.line_style_name ,EDW3.tax_exempt_status_code ,EDW3.tax_exemption_id ,EDW3.vat_tax_id ,V3.serviced_qty  FROM   EDW1 V3 LEFT OUTER JOIN        (SELECT  tax_exempt_status_code                ,tax_exemption_id                ,vat_tax_id                ,instance_id                ,contract_line_id         FROM ${EEDDWWDDBB}.contract_line_svc_supp_ld         WHERE contract_line_id <> ' '         AND tran_code <> 'D'         GROUP BY 1,2,3,4,5) EDW3  ON      V3.service_line_id = EDW3.contract_line_id AND     V3.instance_id = EDW3.instance_id  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23           SELECT
            V3.contract_header_id,
            V3.instance_id,
            V3.service_line_id,
            V3.service_line_nbr,
            V3.service_line_style_id,
            V3.line_style_name,
            EDW3.tax_exempt_status_code,
            EDW3.tax_exemption_id,
            EDW3.vat_tax_id,
            V3.serviced_qty  
        FROM
            ${TTMMPPDDBB1}.EDW1    AS V3   
        LEFT OUTER  JOIN
            (
                SELECT
                    tax_exempt_status_code,
                    tax_exemption_id,
                    vat_tax_id,
                    instance_id,
                    contract_line_id  
                FROM
                    ${EEDDWWDDBB}.contract_line_svc_supp_ld     
                WHERE
                    upper(trim(contract_line_id)) <> upper(trim(' '))  
                    AND upper(trim(tran_code)) <> upper(trim('D'))   
                GROUP BY
                    tax_exempt_status_code ,
                    tax_exemption_id ,
                    vat_tax_id ,
                    instance_id ,
                    contract_line_id      
            )    AS EDW3    
                ON V3.service_line_id = EDW3.contract_line_id  
                AND V3.instance_id = EDW3.instance_id;
