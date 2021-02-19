----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:47 
--Script Name: d8_contract_bill_sched_dn_24.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t24 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t24;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23 column (instance_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23 column (vat_tax_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23 COMPUTE STATISTICS  FOR COLUMNS vat_tax_id;

--Original Query:
  --COLLECT STATS ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23 column (tax_exemption_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23 COMPUTE STATISTICS  FOR COLUMNS tax_exemption_id;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23 FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.tax_exemption FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.ar_vat_tax FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.contract_line_xref_ld FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_cfs_invtry_item_xref FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t24 (  contract_header_id ,instance_id ,service_line_id ,service_line_nbr ,service_line_style_id ,service_line_style_name ,tax_exempt_status_code ,tax_exemption_id ,vat_tax_id ,customer_exemption_nbr ,exemption_reason_code ,exemption_tax_code ,vat_tax_code ,inventory_item_id ,inventory_org_id ,line_product_id ,line_product_id_formatted ,serviced_qty ) SELECT   EDW1.contract_header_id ,EDW1.instance_id ,EDW1.service_line_id ,EDW1.service_line_nbr ,EDW1.service_line_style_id ,EDW1.service_line_style_name ,EDW1.tax_exempt_status_code ,EDW1.tax_exemption_id ,EDW1.vat_tax_id <WM_COMMENT_START> customer_exemption_nbr <WM_COMMENT_END> ,COALESCE(EDW2.customer_exemption_nbr, ' ') <WM_COMMENT_START> exemption_reason_code <WM_COMMENT_END> ,COALESCE(EDW2.reason_code, ' ') <WM_COMMENT_START> exemption_tax_code <WM_COMMENT_END> ,COALESCE(EDW2.tax_code, ' ') <WM_COMMENT_START> vat_tax_code <WM_COMMENT_END> ,COALESCE(EDW3.vat_tax_code, ' ')  <WM_COMMENT_START>inventory_item_id <WM_COMMENT_END>  ,COALESCE(EDW4.obj_id1, 0)  <WM_COMMENT_START>inventory_org_id <WM_COMMENT_END> ,COALESCE(EDW4.obj_id2, 0) <WM_COMMENT_START>line_product_id <WM_COMMENT_END> ,COALESCE(EDW5.offering_id, ' ') <WM_COMMENT_START>line_product_id_formatted <WM_COMMENT_END> ,COALESCE(EDW5.offering_id_hyphenated, ' ') ,EDW1.serviced_qty  FROM    ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23 EDW1 LEFT OUTER JOIN         ${EEDDWWDDBB2}.tax_exemption EDW2 ON      EDW1.instance_id = EDW2.instance_id AND     EDW1.tax_exemption_id = EDW2.tax_exemption_id  LEFT OUTER JOIN         ${EEDDWWDDBB2}.ar_vat_tax EDW3 ON      EDW1.instance_id = EDW3.instance_id AND     EDW1.vat_tax_id = EDW3.vat_tax_id  LEFT OUTER JOIN        (SELECT contract_line_id               ,instance_id               ,CAST(object1_id1 AS DECIMAL(18,0)) obj_id1               ,CAST(object1_id2 AS DECIMAL(18,0)) obj_id2         FROM  ${EEDDWWDDBB2}.contract_line_xref_ld         WHERE object1_code = 'OKX_SERVICE'         AND   tran_code <> 'D') EDW4         ON    EDW1.service_line_id = EDW4.contract_line_id         AND   EDW1.instance_id = coalesce(EDW4.instance_id,'')  LEFT OUTER JOIN        (SELECT inventory_item_id               ,inventory_org_id               ,instance_id               ,offering_id               ,offering_id_hyphenated         FROM  ${AAPPLLIIDD1EENNVV}_cfs_invtry_item_xref )EDW5         ON    coalesce(EDW4.instance_id,'') = EDW5.instance_id         AND   EDW4.obj_id1 = EDW5.inventory_item_id         AND   EDW4.obj_id2 = EDW5.inventory_org_id         GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t24           SELECT
            EDW1.contract_header_id,
            EDW1.instance_id,
            EDW1.service_line_id,
            EDW1.service_line_nbr,
            EDW1.service_line_style_id,
            EDW1.service_line_style_name,
            EDW1.tax_exempt_status_code,
            EDW1.tax_exemption_id,
            EDW1.vat_tax_id,
            COALESCE( EDW2.customer_exemption_nbr ,
            ' ' ) AS auto_c09,
            COALESCE( EDW2.reason_code ,
            ' ' ) AS auto_c010,
            COALESCE( EDW2.tax_code ,
            ' ' ) AS auto_c011,
            COALESCE( EDW3.vat_tax_code ,
            ' ' ) AS auto_c012,
            COALESCE( EDW4.obj_id1 ,
            0 ) AS auto_c013,
            COALESCE( EDW4.obj_id2 ,
            0 ) AS auto_c014,
            COALESCE( EDW5.offering_id ,
            ' ' ) AS auto_c015,
            COALESCE( EDW5.offering_id_hyphenated ,
            ' ' ) AS auto_c016,
            EDW1.serviced_qty  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t23    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.tax_exemption    AS EDW2    
                ON EDW1.instance_id = EDW2.instance_id  
                AND EDW1.tax_exemption_id = EDW2.tax_exemption_id    
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.ar_vat_tax    AS EDW3    
                ON EDW1.instance_id = EDW3.instance_id  
                AND EDW1.vat_tax_id = EDW3.vat_tax_id    
        LEFT OUTER  JOIN
            (
                SELECT
                    contract_line_id,
                    instance_id,
                    -- FORMAT_DECIMAL( object1_id1 , #santosh
                    -- 'decimal(18,0)' ) AS obj_id1, #santosh
                    -- FORMAT_DECIMAL( object1_id2 , #santosh
                    -- 'decimal(18,0)' ) AS obj_id2 #santosh
					SUBSTR( ROUND(INT(object1_id1)),1,18) AS obj_id1,
					SUBSTR( ROUND(INT(object1_id2)),1,18) AS obj_id2
                FROM
                    ${EEDDWWDDBB2}.contract_line_xref_ld     
                WHERE
                    upper(trim(object1_code)) = upper(trim('OKX_SERVICE'))  
                    AND upper(trim(tran_code)) <> upper(trim('D'))         
            )    AS EDW4    
                ON EDW1.service_line_id = EDW4.contract_line_id  
                AND EDW1.instance_id = COALESCE( EDW4.instance_id ,
            '' )     
        LEFT OUTER  JOIN
            (
                SELECT
                    inventory_item_id,
                    inventory_org_id,
                    instance_id,
                    offering_id,
                    offering_id_hyphenated  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_invtry_item_xref            
            )    AS EDW5    
                ON COALESCE( EDW4.instance_id ,
            '' )  = EDW5.instance_id  
            AND EDW4.obj_id1 = EDW5.inventory_item_id   
            AND EDW4.obj_id2 = EDW5.inventory_org_id     
        GROUP BY
            EDW1.contract_header_id ,
            EDW1.instance_id ,
            EDW1.service_line_id ,
            EDW1.service_line_nbr ,
            EDW1.service_line_style_id ,
            EDW1.service_line_style_name ,
            EDW1.tax_exempt_status_code ,
            EDW1.tax_exemption_id ,
            EDW1.vat_tax_id ,
            COALESCE( EDW2.customer_exemption_nbr ,
            ' ' ) ,
            COALESCE( EDW2.reason_code ,
            ' ' ) ,
            COALESCE( EDW2.tax_code ,
            ' ' ) ,
            COALESCE( EDW3.vat_tax_code ,
            ' ' ) ,
            COALESCE( EDW4.obj_id1 ,
            0 ) ,
            COALESCE( EDW4.obj_id2 ,
            0 ) ,
            COALESCE( EDW5.offering_id ,
            ' ' ) ,
            COALESCE( EDW5.offering_id_hyphenated ,
            ' ' ) ,
            EDW1.serviced_qty;

--Original Query:
  --COLLECT STATS COLUMN (contract_header_id),COLUMN (instance_id) ON ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t24  

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t24 COMPUTE STATISTICS  FOR COLUMNS contract_header_id  , instance_id;
