 ----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:53 
--Script Name: d8_contract_bill_sched_dn_44_trg2.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${TTMMPPDDBB}  

--Translated Query: STATUS SUCCESS

    use ${TTMMPPDDBB} ;

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2;

--Original Query:
  --LOCK TABLE ${EEDDWWDDBB2}.invoice_line FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.customer_invoice FOR ACCESS  INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2  (   instance_id ,bill_instance_nbr ,invoice_type_code ,invoice_date ,invoice_nbr ,invoice_currency_code   ) SELECT  EDW1.instance_id ,EDW1.bill_instance_nbr ,EDW2.invoice_type ,EDW2.invoice_date ,EDW2.invoice_nbr ,EDW2.invoice_currency_code FROM  ${EEDDWWDDBB2}.invoice_line EDW1, ${EEDDWWDDBB2}.customer_invoice   EDW2 WHERE  EDW1.source_country_code = EDW2.source_country_code AND    EDW1.customer_trx_id = EDW2.customer_trx_id AND    EDW1.bill_instance_nbr IS NOT NULL GROUP BY 1,2,3,4,5,6  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2           SELECT
            EDW1.instance_id,
            EDW1.bill_instance_nbr,
            EDW2.invoice_type,
            EDW2.invoice_date,
            EDW2.invoice_nbr,
            EDW2.invoice_currency_code  
        FROM
            ${EEDDWWDDBB2}.invoice_line    AS EDW1   ,
            ${EEDDWWDDBB2}.customer_invoice    AS EDW2   
        WHERE
            upper(trim(EDW1.source_country_code)) = upper(trim(EDW2.source_country_code))  
            AND EDW1.customer_trx_id = EDW2.customer_trx_id 
            AND EDW1.bill_instance_nbr  IS NOT NULL   
        GROUP BY
            EDW1.instance_id ,
            EDW1.bill_instance_nbr ,
            EDW2.invoice_type ,
            EDW2.invoice_date ,
            EDW2.invoice_nbr ,
            EDW2.invoice_currency_code;

--Original Query:
  --INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2 SELECT * FROM  ttmp.d8t_contr_bill_schd_dn_t8_keep WHERE  (instance_id ,bill_instance_nbr ,invoice_type_code , 	invoice_date ,invoice_nbr ,Invoice_currency_code ) NOT IN 	(SELECT  instance_id ,bill_instance_nbr ,invoice_type_code , 	invoice_date ,invoice_nbr ,Invoice_currency_code  	    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2) 

--Translated Query: STATUS SUCCESS

    WITH qq1 AS (    SELECT
        d8t_contr_bill_schd_dn_t8_keep.*  
    FROM
        ${TTMMPPDDBB1}.d8t_contr_bill_schd_dn_t8_keep AS d8t_contr_bill_schd_dn_t8_keep
    LEFT OUTER JOIN
        (SELECT
            DISTINCT instance_id AS auto_c00,
            bill_instance_nbr AS auto_c01,
            invoice_type_code AS auto_c02,
            invoice_date AS auto_c03,
            invoice_nbr AS auto_c04,
            Invoice_currency_code AS auto_c05  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2 ) AS autoAlias_37 
            ON ( upper(trim(instance_id)) = upper(trim(autoAlias_37.AUTO_C00)) 
            AND upper(trim(bill_instance_nbr)) = upper(trim(autoAlias_37.AUTO_C01)) 
            AND upper(trim(invoice_type_code)) = upper(trim(autoAlias_37.AUTO_C02)) 
            AND invoice_date = autoAlias_37.AUTO_C03 
            AND upper(trim(invoice_nbr)) = upper(trim(autoAlias_37.AUTO_C04)) 
            AND upper(trim(Invoice_currency_code)) = upper(trim(autoAlias_37.AUTO_C05)) ) 
    WHERE
        autoAlias_37.AUTO_C00 IS  NULL  
        AND autoAlias_37.AUTO_C01 IS  NULL  
        AND autoAlias_37.AUTO_C02 IS  NULL  
        AND autoAlias_37.AUTO_C03 IS  NULL  
        AND autoAlias_37.AUTO_C04 IS  NULL  
        AND autoAlias_37.AUTO_C05 IS  NULL         ) INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2       SELECT
                * 
            FROM
                qq1;

--Original Query:
  --COLLECT STATISTICS ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2 COLUMN (INSTANCE_ID ,BILL_INSTANCE_NBR) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2 COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,BILL_INSTANCE_NBR;
