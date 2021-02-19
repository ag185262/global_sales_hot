----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:54 
--Script Name: d8_contract_bill_sched_dn_44_trg3.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3;

--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1 FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3 (  service_subline_id ,bill_from_date ,bill_to_date ,instance_id ,creation_date ,invoice_type_code ,invoice_date ,invoice_currency_code ,invoice_nbr  ,bill_instance_nbr <WM_COMMENT_START>new added field* under TAR#EDW_GCA-741<WM_COMMENT_END> ,bill_amt_ent   ) SELECT  EDW1.service_subline_id ,EDW1.bill_from_date ,EDW1.bill_to_date ,EDW1.instance_id ,EDW1.creation_date ,EDW2.invoice_type_code ,EDW2.invoice_date ,EDW2.invoice_currency_code ,EDW2.invoice_nbr ,EDW1.bill_instance_nbr ,SUM(EDW1.bill_amt_ent) FROM	${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1 EDW1, ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2 EDW2 WHERE	EDW1.bill_instance_nbr = EDW2.bill_instance_nbr AND	EDW1.instance_id       = EDW2.instance_id GROUP BY 1,2,3,4,5,6,7,8,9,10  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3           SELECT
            EDW1.service_subline_id,
            EDW1.bill_from_date,
            EDW1.bill_to_date,
            EDW1.instance_id,
            EDW1.creation_date,
            EDW2.invoice_type_code,
            EDW2.invoice_date,
            EDW2.invoice_nbr,
            EDW2.invoice_currency_code,
            SUM( EDW1.bill_amt_ent ) AS auto_c010,
            EDW1.bill_instance_nbr  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg1    AS EDW1   
        INNER JOIN
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg2    AS EDW2   
        ON
            EDW1.bill_instance_nbr = EDW2.bill_instance_nbr  
            AND EDW1.instance_id = EDW2.instance_id   
        GROUP BY
            EDW1.service_subline_id ,
            EDW1.bill_from_date ,
            EDW1.bill_to_date ,
            EDW1.instance_id ,
            EDW1.creation_date ,
            EDW2.invoice_type_code ,
            EDW2.invoice_date ,
            EDW2.invoice_currency_code ,
            EDW2.invoice_nbr ,
            EDW1.bill_instance_nbr;

--Original Query:
  --UPDATE EDW1 FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 EDW1, ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3 EDW2 SET bill_from_date=EDW2.bill_from_date, bill_to_date=EDW2.bill_to_date, creation_date=EDW2.creation_date, invoice_type_code=EDW2.invoice_type_code, invoice_date=EDW2.invoice_date, invoice_currency_code=EDW2.invoice_currency_code, invoice_nbr=EDW2.invoice_nbr, bill_amt_ent=EDW2.bill_amt_ent WHERE EDW1.instance_id=EDW2.instance_id AND EDW1.service_subline_id=EDW2.service_subline_id AND EDW1.bill_instance_nbr=EDW2.bill_instance_nbr 

--Translated Query: Corrected

    -- INSERT OVERWRITE TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 SELECT EDW1.service_subline_id ,CASE WHEN EDW1.instance_id = EDW2.instance_id AND EDW1.service_subline_id = EDW2.service_subline_id AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr THEN EDW2.bill_from_date ELSE EDW1.bill_from_date END AS bill_from_date ,CASE WHEN EDW1.instance_id = EDW2.instance_id AND EDW1.service_subline_id = EDW2.service_subline_id AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr THEN EDW2.bill_to_date ELSE EDW1.bill_to_date END AS bill_to_date ,EDW1.instance_id ,CASE WHEN EDW1.instance_id = EDW2.instance_id AND EDW1.service_subline_id = EDW2.service_subline_id AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr THEN EDW2.creation_date ELSE EDW1.creation_date END AS creation_date ,CASE WHEN EDW1.instance_id = EDW2.instance_id AND EDW1.service_subline_id = EDW2.service_subline_id AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr THEN EDW2.invoice_type_code ELSE EDW1.invoice_type_code END AS invoice_type_code ,CASE WHEN EDW1.instance_id = EDW2.instance_id AND EDW1.service_subline_id = EDW2.service_subline_id AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr THEN EDW2.invoice_date ELSE EDW1.invoice_date END AS invoice_date ,CASE WHEN EDW1.instance_id = EDW2.instance_id AND EDW1.service_subline_id = EDW2.service_subline_id AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr THEN EDW2.invoice_nbr ELSE EDW1.invoice_nbr END AS invoice_nbr ,CASE WHEN EDW1.instance_id = EDW2.instance_id AND EDW1.service_subline_id = EDW2.service_subline_id AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr THEN EDW2.invoice_currency_code ELSE EDW1.invoice_currency_code END AS invoice_currency_code ,CASE WHEN EDW1.instance_id = EDW2.instance_id AND EDW1.service_subline_id = EDW2.service_subline_id AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr THEN EDW2.bill_amt_ent ELSE EDW1.bill_amt_ent END AS bill_amt_ent ,EDW1.bill_instance_nbr FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 AS EDW1 LEFT JOIN ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3 AS EDW2 ON EDW1.instance_id = EDW2.instance_id AND EDW1.service_subline_id = EDW2.service_subline_id AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr;

--Translated Query(Modified, modified the CASE statement and replaced the join condition on every CASE with NOT NULL check:
INSERT OVERWRITE TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 
SELECT 
  EDW1.service_subline_id, 
  CASE WHEN EDW2.instance_id IS NOT NULL
  AND EDW2.service_subline_id IS NOT NULL
  AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.bill_from_date ELSE EDW1.bill_from_date END AS bill_from_date, 
  CASE WHEN EDW2.instance_id IS NOT NULL
  AND EDW2.service_subline_id IS NOT NULL
  AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.bill_to_date ELSE EDW1.bill_to_date END AS bill_to_date, 
  EDW1.instance_id, 
  CASE WHEN EDW2.instance_id IS NOT NULL
  AND EDW2.service_subline_id IS NOT NULL
  AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.creation_date ELSE EDW1.creation_date END AS creation_date, 
  CASE WHEN EDW2.instance_id IS NOT NULL
  AND EDW2.service_subline_id IS NOT NULL
  AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.invoice_type_code ELSE EDW1.invoice_type_code END AS invoice_type_code, 
  CASE WHEN EDW2.instance_id IS NOT NULL
  AND EDW2.service_subline_id IS NOT NULL
  AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.invoice_date ELSE EDW1.invoice_date END AS invoice_date, 
  CASE WHEN EDW2.instance_id IS NOT NULL
  AND EDW2.service_subline_id IS NOT NULL
  AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.invoice_nbr ELSE EDW1.invoice_nbr END AS invoice_nbr, 
  CASE WHEN EDW2.instance_id IS NOT NULL
  AND EDW2.service_subline_id IS NOT NULL
  AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.invoice_currency_code ELSE EDW1.invoice_currency_code END AS invoice_currency_code, 
  CASE WHEN EDW2.instance_id IS NOT NULL
  AND EDW2.service_subline_id IS NOT NULL
  AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.bill_amt_ent ELSE EDW1.bill_amt_ent END AS bill_amt_ent, 
  EDW1.bill_instance_nbr 
FROM 
  ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 AS EDW1 
  LEFT JOIN ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3 AS EDW2 
  ON 
      EDW1.instance_id = EDW2.instance_id 
  AND EDW1.service_subline_id = EDW2.service_subline_id 
  AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr;


--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3 FOR ACCESS INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 ( service_subline_id ,bill_from_date ,bill_to_date ,instance_id ,creation_date ,invoice_type_code ,invoice_date ,invoice_currency_code ,invoice_nbr ,bill_instance_nbr ,bill_amt_ent ) SELECT EDW2.service_subline_id ,EDW2.bill_from_date ,EDW2.bill_to_date ,EDW2.instance_id ,EDW2.creation_date ,EDW2.invoice_type_code ,EDW2.invoice_date ,EDW2.invoice_currency_code ,EDW2.invoice_nbr ,EDW2.bill_instance_nbr ,EDW2.bill_amt_ent FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3 EDW2 WHERE NOT EXISTS (SELECT 1 FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 EDW1 WHERE EDW2.instance_id = EDW1.instance_id AND EDW2.service_subline_id = EDW1.service_subline_id AND EDW2.bill_instance_nbr = EDW1.bill_instance_nbr) 

--Translated Query: STATUS SUCCESS

    -- WITH qq1 AS (    SELECT
        -- EDW2.service_subline_id,
        -- EDW2.bill_from_date,
        -- EDW2.bill_to_date,
        -- EDW2.instance_id,
        -- EDW2.creation_date,
        -- EDW2.invoice_type_code,
        -- EDW2.invoice_date,
        -- EDW2.invoice_nbr,
        -- EDW2.invoice_currency_code,
        -- EDW2.bill_amt_ent,
        -- EDW2.bill_instance_nbr  
    -- FROM
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3    AS EDW2           ) INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4       SELECT
            -- * 
        -- FROM
            -- qq1;

--Translated Query(Modified, The was no check of NOT EXISTS condition as in the Source query, hence modified the query by adding left join and null check):
INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4
SELECT
        A.service_subline_id,
        A.bill_from_date,
        A.bill_to_date,
        A.instance_id,
        A.creation_date,
        A.invoice_type_code,
        A.invoice_date,
        A.invoice_nbr,
        A.invoice_currency_code,
        A.bill_amt_ent,
        A.bill_instance_nbr
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg3 A 
LEFT JOIN
        ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 B
ON
   A.instance_id = B.instance_id AND A.service_subline_id = B.service_subline_id AND A.bill_instance_nbr = B.bill_instance_nbr
WHERE B.instance_id IS NULL AND B.service_subline_id IS NULL AND B.bill_instance_nbr IS NULL;


--Original Query:
  --DEL FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 WHERE (instance_id,service_subline_id) IN (SEL instance_id,contract_line_id FROM ${EEDDWWDDBB}.contract_subline_bill_ld WHERE tran_code='D' GROUP BY 1,2) 

--Translated Query: STATUS SUCCESS

    -- INSERT OVERWRITE
        -- TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4      
            -- ) AS Q2 
                -- ON COALESCE( Q1.service_subline_id ,
            -- '1' ) = COALESCE( Q2.service_subline_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.bill_from_date ,
            -- CURRENT_DATE() ) = COALESCE( Q2.bill_from_date ,
            -- CURRENT_DATE() ) 
            -- AND COALESCE( Q1.bill_to_date ,
            -- CURRENT_DATE() ) = COALESCE( Q2.bill_to_date ,
            -- CURRENT_DATE() ) 
            -- AND COALESCE( Q1.instance_id ,
            -- '1' ) = COALESCE( Q2.instance_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_date ,
            -- CURRENT_DATE() ) = COALESCE( Q2.creation_date ,
            -- CURRENT_DATE() ) 
            -- AND COALESCE( Q1.invoice_type_code ,
            -- '1' ) = COALESCE( Q2.invoice_type_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.invoice_date ,
            -- CURRENT_DATE() ) = COALESCE( Q2.invoice_date ,
            -- CURRENT_DATE() ) 
            -- AND COALESCE( Q1.invoice_nbr ,
            -- '1' ) = COALESCE( Q2.invoice_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.invoice_currency_code ,
            -- '1' ) = COALESCE( Q2.invoice_currency_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.bill_amt_ent ,
            -- 1) = COALESCE( Q2.bill_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.bill_instance_nbr ,
            -- '1' ) = COALESCE( Q2.bill_instance_nbr ,
            -- '1' ) 
        -- WHERE
            -- Q2.service_subline_id IS NULL 
            -- AND Q2.bill_from_date IS NULL 
            -- AND Q2.bill_to_date IS NULL 
            -- AND Q2.instance_id IS NULL 
            -- AND Q2.creation_date IS NULL 
            -- AND Q2.invoice_type_code IS NULL 
            -- AND Q2.invoice_date IS NULL 
            -- AND Q2.invoice_nbr IS NULL 
            -- AND Q2.invoice_currency_code IS NULL 
            -- AND Q2.bill_amt_ent IS NULL 
            -- AND Q2.bill_instance_nbr IS NULL;

--Translated Query(Modified, The was no check of NOT EXISTS condition as in the Source query, hence modified the query by adding left join and null check):

INSERT OVERWRITE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4
SELECT A.*
FROM 
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sch_dn_t44_trg4 A 
LEFT JOIN 
${EEDDWWDDBB}.contract_subline_bill_ld B
ON A.instance_id = B.instance_id AND A.service_subline_id = B.contract_line_id
WHERE B.instance_id IS NULL AND B.contract_line_id IS NULL AND B.tran_code='D' ;
