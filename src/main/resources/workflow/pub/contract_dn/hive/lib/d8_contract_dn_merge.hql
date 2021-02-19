
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_gl_code_combo_01a.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------



--Original Query:
  --DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};


--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01;


--Original Query:
  --LOCK TABLE  ${EEDDWWDDBB}.gl_code_combinations FOR ACCESSLOCK TABLE  ${EEDDWWDDBB}.contract_revn_dist FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_gl_code_combo_t01      (      contract_line_id,      instance_id,      revenue_dist_id,      line_revn_dist_csub_code,      line_revn_dist_enable_flag,      line_revn_dist_facct_code,      line_revn_dist_fin_proj_id,      line_revn_dist_fml_org_code,      line_revn_dist_iet_code,      line_revn_dist_pct,      line_revn_dist_ps_code,      line_revn_dist_sobp_code,      tran_code,      use_line      )SELECT      EDW1.contract_line_id,      EDW1.instance_id,      EDW1.revenue_dist_id,      EDW2.common_sub_account_code,      EDW2.enabled_flag,      EDW2.fml_account_code,      EDW2.financial_project_id,      EDW2.fml_organization_code,      EDW2.inter_entity_code,      EDW1.distribution_pct,      EDW2.fml_product_service_code,      EDW2.set_of_books_partition_code,      EDW1.tran_code,     (CASE <WM_COMMENT_START> use_line <WM_COMMENT_END>      WHEN EDW1.tran_code = 'D'      THEN 0      ELSE 1      END)FROM  ${EEDDWWDDBB}.contract_revn_dist EDW1,      ${EEDDWWDDBB}.gl_code_combinations EDW2WHERE EDW1.instance_id = EDW2.instance_idAND   EDW1.gl_code_combination_id = EDW2.gl_code_combination_idAND   EDW2.set_of_books_id = 0

--Translated Query: SUCCESS
--INSERT INTO TABLE  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01           SELECT EDW1.contract_line_id, EDW1.instance_id, EDW1.revenue_dist_id, EDW2.common_sub_account_code, EDW2.enabled_flag, EDW2.fml_account_code, EDW2.financial_project_id, EDW2.fml_organization_code, EDW2.inter_entity_code, EDW1.distribution_pct, EDW2.fml_product_service_code, EDW2.set_of_books_partition_code, EDW1.tran_code, CASE WHEN upper(trim(EDW1.tran_code)) = upper(trim('D'))  THEN 0  ELSE 1  END AS auto_c013  FROM  ${EEDDWWDDBB}.contract_revn_dist    AS EDW1   ,  ${EEDDWWDDBB}.gl_code_combinations    AS EDW2   WHERE upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))  AND EDW1.gl_code_combination_id = EDW2.gl_code_combination_id   AND EDW2.set_of_books_id = 0
--Chngd NATASHA(CORRECTED the transformed query removed the cross join)

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01           SELECT distinct
            EDW1.contract_line_id,
            EDW1.instance_id,
            EDW1.revenue_dist_id,
            EDW2.common_sub_account_code,
            EDW2.enabled_flag,
            EDW2.fml_account_code,
            EDW2.financial_project_id,
            EDW2.fml_organization_code,
            EDW2.inter_entity_code,
            EDW1.distribution_pct,
            EDW2.fml_product_service_code,
            EDW2.set_of_books_partition_code,
            EDW1.tran_code,
            CASE 
                WHEN trim(EDW1.tran_code) = trim('D')  THEN 0  
                ELSE 1  
            END AS auto_c013  
        FROM
            ${EEDDWWDDBB}.contract_revn_dist    AS EDW1   
        INNER JOIN
            ${EEDDWWDDBB}.gl_code_combinations    AS EDW2   
                ON trim(EDW1.instance_id) = trim(EDW2.instance_id)  
                AND EDW1.gl_code_combination_id = EDW2.gl_code_combination_id   
        WHERE
            EDW2.set_of_books_id = 0;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_gl_code_combo_01b.upd.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};


--Original Query:
  --UPDATE ${AAPPLLIIDD1EENNVV}_gl_code_combo_t01SET   use_line = 1WHERE tran_code = 'D'AND   (      contract_line_id,      instance_id) NOT IN (SELECT      contract_line_id,      instance_idFROM  ${AAPPLLIIDD1EENNVV}_gl_code_combo_t01WHERE tran_code <> 'D'GROUP BY 1,2)

--Translated Query: SUCCESS
--Changed the 5 step update approach to insert overwrite
--      DROP TABLE IF EXISTS ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01Temp
--CREATE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01Temp Like ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01

    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 SELECT distinct
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id AS contract_line_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.revenue_dist_id AS revenue_dist_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_csub_code AS line_revn_dist_csub_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_enable_flag AS line_revn_dist_enable_flag ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_facct_code AS line_revn_dist_facct_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_fin_proj_id AS line_revn_dist_fin_proj_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_fml_org_code AS line_revn_dist_fml_org_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_iet_code AS line_revn_dist_iet_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_pct AS line_revn_dist_pct ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_ps_code AS line_revn_dist_ps_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_sobp_code AS line_revn_dist_sobp_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.tran_code AS tran_code ,
            CASE 
                WHEN upper(trim(tran_code)) = upper(trim('D')) 
                AND autoAlias_61.auto_c00 IS null THEN 1 
                ELSE AAPPLLIIDD1EENNVV_gl_code_combo_t01.use_line 
            END AS use_line 
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 AS AAPPLLIIDD1EENNVV_gl_code_combo_t01 
        Left Outer Join
            (
                SELECT  
                    contract_line_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01     
                WHERE
                    UPPER(TRIM(TRAN_CODE)) <> UPPER(TRIM('D'))  
                GROUP BY
                    contract_line_id ,
                    instance_id
            )autoAlias_61 
                ON (
                    autoAlias_61.auto_c00 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id 
                    AND autoAlias_61.auto_c01 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id
                );
--DROP table IF EXISTS ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01
--Alter table ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01Temp rename to ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01


--Original Query:
  --UPDATE ${AAPPLLIIDD1EENNVV}_gl_code_combo_t01SET   use_line = 0WHERE use_line = 1AND   (      contract_line_id,      instance_id) IN (SELECT      contract_line_id,      instance_idFROM  ${AAPPLLIIDD1EENNVV}_gl_code_combo_t01WHERE use_line = 1AND   line_revn_dist_pct = 100.0000GROUP BY 1,2HAVING COUNT(*) > 1)AND   (      contract_line_id,      instance_id,      revenue_dist_id) NOT IN (SELECT      contract_line_id,      instance_id,      MIN(revenue_dist_id)FROM  ${AAPPLLIIDD1EENNVV}_gl_code_combo_t01WHERE use_line = 1AND   line_revn_dist_pct = 100.0000GROUP BY 1,2HAVING COUNT(*) > 1)

--Translated Query: SUCCESS
--      DROP TABLE IF EXISTS ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01Temp
--CREATE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01Temp Like ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01
--INSERT OVERWRITE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 SELECT AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id AS contract_line_id , AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id AS instance_id , AAPPLLIIDD1EENNVV_gl_code_combo_t01.revenue_dist_id AS revenue_dist_id , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_csub_code AS line_revn_dist_csub_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_enable_flag AS line_revn_dist_enable_flag , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_facct_code AS line_revn_dist_facct_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_fin_proj_id AS line_revn_dist_fin_proj_id , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_fml_org_code AS line_revn_dist_fml_org_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_iet_code AS line_revn_dist_iet_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_pct AS line_revn_dist_pct , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_ps_code AS line_revn_dist_ps_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_sobp_code AS line_revn_dist_sobp_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.tran_code AS tran_code , CASE WHEN use_line = 1 AND autoAlias_61.auto_c00 IS not null AND autoAlias_61.auto_c00 IS null THEN 0 ELSE AAPPLLIIDD1EENNVV_gl_code_combo_t01.use_line END AS use_line FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 AS AAPPLLIIDD1EENNVV_gl_code_combo_t01 Left Outer Join(SELECT  DISTINCT contract_line_id AS auto_c00, instance_id AS auto_c01, MIN( revenue_dist_id ) AS auto_c02  FROM  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01     WHERE USE_LINE = 1  AND LINE_REVN_DIST_PCT = 100.0000   GROUP BY contract_line_id , instance_id HAVING COUNT(*) > 1)autoAlias_61 ON (autoAlias_61.auto_c00 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id AND autoAlias_61.auto_c01 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id AND autoAlias_61.auto_c02 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.revenue_dist_id)
--DROP table IF EXISTS ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01
--Alter table ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01Temp rename to ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01
--Chngd NATASHA(CORRECTED the transformed query)

    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 SELECT  distinct
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id AS contract_line_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.revenue_dist_id AS revenue_dist_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_csub_code AS line_revn_dist_csub_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_enable_flag AS line_revn_dist_enable_flag ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_facct_code AS line_revn_dist_facct_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_fin_proj_id AS line_revn_dist_fin_proj_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_fml_org_code AS line_revn_dist_fml_org_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_iet_code AS line_revn_dist_iet_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_pct AS line_revn_dist_pct ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_ps_code AS line_revn_dist_ps_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_sobp_code AS line_revn_dist_sobp_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.tran_code AS tran_code ,
            CASE 
                WHEN use_line = 1 
                AND autoAlias_47.auto_c00 IS not null 
                AND autoAlias_48.auto_c00 IS null THEN 0 
                ELSE AAPPLLIIDD1EENNVV_gl_code_combo_t01.use_line 
            END AS use_line 
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 AS AAPPLLIIDD1EENNVV_gl_code_combo_t01 
        Left Outer Join
            (
                SELECT 
                    contract_line_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01     
                WHERE
                    USE_LINE = 1  
                    AND LINE_REVN_DIST_PCT = 100.0000   
                GROUP BY
                    contract_line_id ,
                    instance_id 
                HAVING
                    COUNT(*) > 1
            )autoAlias_47 
                ON (
                    autoAlias_47.auto_c00 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id 
                    AND autoAlias_47.auto_c01 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id
                ) 
        Left Outer Join
            (
                SELECT 
                    contract_line_id AS auto_c00,
                    instance_id AS auto_c01,
                    MIN( revenue_dist_id ) AS auto_c02  
                FROM
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01     
                WHERE
                    USE_LINE = 1  
                    AND LINE_REVN_DIST_PCT = 100.0000   
                GROUP BY
                    contract_line_id ,
                    instance_id 
                HAVING
                    COUNT(*) > 1
            )autoAlias_48 
                ON (
                    autoAlias_48.auto_c00 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id 
                    AND autoAlias_48.auto_c01 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id 
                    AND autoAlias_48.auto_c02 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.revenue_dist_id
                );


--Original Query:
  --UPDATE ${AAPPLLIIDD1EENNVV}_gl_code_combo_t01SET   use_line = 2WHERE use_line = 1AND   (      contract_line_id,      instance_id) IN (SELECT      contract_line_id,      instance_idFROM  ${AAPPLLIIDD1EENNVV}_gl_code_combo_t01WHERE use_line = 1GROUP BY 1,2HAVING COUNT(*) > 1)AND   (      contract_line_id,      instance_id,      revenue_dist_id) NOT IN (SELECT      contract_line_id,      instance_id,      MIN(revenue_dist_id)FROM  ${AAPPLLIIDD1EENNVV}_gl_code_combo_t01WHERE use_line = 1GROUP BY 1,2HAVING COUNT(*) > 1)

--Translated Query: SUCCESS
--      DROP TABLE IF EXISTS ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01Temp
--CREATE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01Temp Like ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01
--INSERT OVERWRITE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 SELECT AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id AS contract_line_id , AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id AS instance_id , AAPPLLIIDD1EENNVV_gl_code_combo_t01.revenue_dist_id AS revenue_dist_id , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_csub_code AS line_revn_dist_csub_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_enable_flag AS line_revn_dist_enable_flag , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_facct_code AS line_revn_dist_facct_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_fin_proj_id AS line_revn_dist_fin_proj_id , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_fml_org_code AS line_revn_dist_fml_org_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_iet_code AS line_revn_dist_iet_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_pct AS line_revn_dist_pct , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_ps_code AS line_revn_dist_ps_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_sobp_code AS line_revn_dist_sobp_code , AAPPLLIIDD1EENNVV_gl_code_combo_t01.tran_code AS tran_code , CASE WHEN use_line = 1 AND autoAlias_61.auto_c00 IS not null AND autoAlias_61.auto_c00 IS null THEN 2 ELSE AAPPLLIIDD1EENNVV_gl_code_combo_t01.use_line END AS use_line FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 AS AAPPLLIIDD1EENNVV_gl_code_combo_t01 Left Outer Join(SELECT  DISTINCT contract_line_id AS auto_c00, instance_id AS auto_c01, MIN( revenue_dist_id ) AS auto_c02  FROM  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01     WHERE USE_LINE = 1  GROUP BY contract_line_id , instance_id HAVING COUNT(*) > 1)autoAlias_61 ON (autoAlias_61.auto_c00 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id AND autoAlias_61.auto_c01 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id AND autoAlias_61.auto_c02 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.revenue_dist_id)
--DROP table IF EXISTS ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01
--Alter table ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01Temp rename to ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01
--Chngd NATASHA(CORRECTED the transformed query )

    INSERT OVERWRITE
        TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 SELECT distinct
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id AS contract_line_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.revenue_dist_id AS revenue_dist_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_csub_code AS line_revn_dist_csub_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_enable_flag AS line_revn_dist_enable_flag ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_facct_code AS line_revn_dist_facct_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_fin_proj_id AS line_revn_dist_fin_proj_id ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_fml_org_code AS line_revn_dist_fml_org_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_iet_code AS line_revn_dist_iet_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_pct AS line_revn_dist_pct ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_ps_code AS line_revn_dist_ps_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.line_revn_dist_sobp_code AS line_revn_dist_sobp_code ,
            AAPPLLIIDD1EENNVV_gl_code_combo_t01.tran_code AS tran_code ,
            CASE 
                WHEN use_line = 1 
                AND autoAlias_47.auto_c00 IS not null 
                AND autoAlias_48.auto_c00 IS null THEN 2 
                ELSE AAPPLLIIDD1EENNVV_gl_code_combo_t01.use_line 
            END AS use_line 
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 AS AAPPLLIIDD1EENNVV_gl_code_combo_t01 
        Left Outer Join
            (
                SELECT
                    contract_line_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01     
                WHERE
                    USE_LINE = 1  
                GROUP BY
                    contract_line_id ,
                    instance_id 
                HAVING
                    COUNT(*) > 1
            )autoAlias_47 
                ON (
                    autoAlias_47.auto_c00 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id 
                    AND autoAlias_47.auto_c01 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id
                ) 
        Left Outer Join
            (
                SELECT
                    contract_line_id AS auto_c00,
                    instance_id AS auto_c01,
                    MIN( revenue_dist_id ) AS auto_c02  
                FROM
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01     
                WHERE
                    USE_LINE = 1  
                GROUP BY
                    contract_line_id ,
                    instance_id 
                HAVING
                    COUNT(*) > 1
            )autoAlias_48 
                ON (
                    autoAlias_48.auto_c00 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.contract_line_id 
                    AND autoAlias_48.auto_c01 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.instance_id 
                    AND autoAlias_48.auto_c02 = AAPPLLIIDD1EENNVV_gl_code_combo_t01.revenue_dist_id
                );

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_area_branch_xref.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};


--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_branch_area_xref ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_branch_area_xref;


--Original Query:
  --INSERT INTO ${AAPPLLIIDD1EENNVV}_branch_area_xref      (      branch_code,      country_area_code,      country_area_desc      )SELECT      EDW1.branch_code,      EDW2.country_area_code,      EDW2.country_area_descFROM  ${EEDDWWDDBB}.branch_country_area_xref EDW1,      ${EEDDWWDDBB}.wcs_country_area EDW2WHERE EDW1.country_area_code = EDW2.country_area_codeAND   CHAR(EDW1.branch_code) > 3

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_branch_area_xref           SELECT
            EDW1.branch_code,
            EDW2.country_area_code,
            EDW2.country_area_desc  
        FROM
            ${EEDDWWDDBB}.branch_country_area_xref    AS EDW1   ,
            ${EEDDWWDDBB}.wcs_country_area    AS EDW2   
        WHERE
            upper(trim(EDW1.country_area_code)) = upper(trim(EDW2.country_area_code))  
            AND LENGTH( EDW1.branch_code )  > 3;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn_01a.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1;


--Original Query:
  --LOCK TABLE  ${EEDDWWDDBB1}.contract_header FOR ACCESSLOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1(      contract_header_id     ,instance_id     ,agreement_record_id     ,approved_date_time     ,archived_flag     ,area_code     ,area_desc     ,author_ncr_unit_id     ,author_oper_unit_country_code     ,author_operating_unit_id     ,auto_renew_day_cnt     ,cancelled_date_time     ,character_type_code     ,contract_est_amt_ent     ,contract_intent_code     ,contract_nbr     ,contract_nbr_modifier     ,country_desc     ,created_by_name     ,creation_date_time     ,curr_code     ,cust_po_nbr     ,cust_po_nbr_rqrd_flag     ,deleted_flag     ,end_date     ,est_renew_amt_ent     ,final_price_modifier_text     ,gsa_code     ,inventory_org_id     ,invoice_zero_amt_flag     ,issue_receive_code     ,last_update_date_time     ,last_update_login_name     ,last_updated_by_name     ,migration_contract_nbr     ,orig_sys_id     ,orig_sys_source_code     ,prepay_rqrd_flag     ,qa_check_list_id     ,region_code     ,region_desc     ,renew_curr_code     ,renew_date     ,signed_date     ,solution_portfolio_id     ,start_date     ,status_code     ,subclass_code     ,template_flag     ,template_used_name     ,termination_code     ,termination_date     ,termination_reason_code     ,tran_code     ,bill_to_site_use_id     ,curr_cnvrsn_rate     ,curr_cnvrsn_rate_date     ,curr_cnvrsn_type_code     ,document_id     ,invoice_rule_id     ,invoice_term_id     ,price_list_header_id     ,renewal_end_date     ,renewal_type_code     ,ship_to_site_use_id     ,contract_subtype_code     ,new_for_new_duration       ,renewal_approval_type     ,suspend_credits_flag)SELECT       EDW1.contract_header_id     ,EDW1.instance_id     ,EDW1.agreement_record_id     ,CAST(CAST(EDW1.approved_date_time AS CHAR(19)) AS TIMESTAMP(0))     ,EDW1.archived_flag      <WM_COMMENT_START> area_code  <WM_COMMENT_END>     ,CASE      WHEN EDW1.author_oper_unit_country_code = 'US'      THEN 'A6'      ELSE      EDW2.area_code      END      <WM_COMMENT_START> area_desc  <WM_COMMENT_END>     ,CASE      WHEN EDW1.author_oper_unit_country_code = 'US'      THEN 'US Other'      ELSE      EDW2.area_desc      END     ,EDW1.author_ncr_unit_id     ,EDW1.author_oper_unit_country_code     ,EDW1.author_operating_unit_id     ,EDW1.auto_renew_day_cnt     ,CAST(CAST(EDW1.cancelled_date_time AS CHAR(19)) AS TIMESTAMP(0))     ,EDW1.character_type_code     ,EDW1.contract_est_amt_ent     ,EDW1.contract_intent_code     ,EDW1.contract_nbr     ,EDW1.contract_nbr_modifier     ,EDW2.country_desc     ,EDW1.created_by_name     ,CAST(CAST(EDW1.creation_date_time AS CHAR(19)) AS TIMESTAMP(0))     ,EDW1.curr_code     ,EDW1.cust_po_nbr     ,EDW1.cust_po_nbr_rqrd_flag     ,EDW1.deleted_flag     ,CAST(EDW1.end_date_time AS DATE)     ,EDW1.est_renew_amt_ent     ,EDW1.final_price_modifier_text     ,EDW1.gsa_code     ,EDW1.inventory_org_id     ,EDW1.invoice_zero_amt_flag     ,EDW1.issue_receive_code     ,CAST(CAST(EDW1.last_update_date_time AS CHAR(19)) AS TIMESTAMP(0))     ,EDW1.last_update_login_name     ,EDW1.last_updated_by_name     ,EDW1.migration_contract_nbr     ,EDW1.orig_sys_id     ,EDW1.orig_sys_source_code     ,EDW1.prepay_rqrd_flag     ,EDW1.qa_check_list_id     ,EDW2.region_code     ,EDW2.region_desc     ,EDW1.renew_curr_code     ,CAST(EDW1.renew_date_time AS DATE)     ,CAST(EDW1.signed_date_time AS DATE)     ,EDW1.solution_portfolio_id     ,CAST(EDW1.start_date_time AS DATE)     ,EDW1.status_code     ,EDW1.subclass_code     ,EDW1.template_flag     ,EDW1.template_used_name     ,EDW1.termination_code     ,CAST(EDW1.termination_date_time AS DATE)     ,EDW1.termination_reason_code     ,EDW1.tran_code     ,EDW1.bill_to_site_use_id     ,EDW1.curr_cnvrsn_rate     ,EDW1.curr_cnvrsn_rate_date     ,EDW1.curr_cnvrsn_type_code     ,EDW1.document_id     ,EDW1.invoice_rule_id     ,EDW1.invoice_term_id     ,EDW1.price_list_header_id     ,EDW1.renewal_end_date     ,EDW1.renewal_type_code     ,EDW1.ship_to_site_use_id     ,EDW1.contract_subtype_code     <WM_COMMENT_START> Added for HSR ENTITLEMENT CORE <WM_COMMENT_END>     ,EDW1.new_for_new_duration     ,EDW1.renewal_approval_type     ,EDW1.suspend_credits_flagFROM  ${EEDDWWDDBB1}.contract_header EDW1,      ${VVIIEEWWDDBB1}.geo_rollup_xref_cs_view EDW2WHERE EDW1.author_oper_unit_country_code = EDW2.country_codeAND   (      EDW1.instance_id,      EDW1.alt_batch_nbr) IN (SELECT      instance_id,      batch_nbrFROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig)AND   EDW1.subclass_code IN ('SERVICE', 'WARRANTY')

--Translated Query: MANUAL
--      INSERT INTO TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1           SELECT EDW1.contract_header_id, EDW1.instance_id, EDW1.agreement_record_id, CAST (CAST (EDW1.approved_date_time AS CHAR(19)) AS timestamp), EDW1.archived_flag, CASE WHEN upper(trim(EDW1.author_oper_unit_country_code)) = upper(trim('US'))  THEN 'A6'  ELSE EDW2.area_code  END AS auto_c05, CASE WHEN upper(trim(EDW1.author_oper_unit_country_code)) = upper(trim('US'))  THEN 'US Other'  ELSE EDW2.area_desc  END AS auto_c06, EDW1.author_ncr_unit_id, EDW1.author_oper_unit_country_code, EDW1.author_operating_unit_id, EDW1.auto_renew_day_cnt, CAST (CAST (EDW1.cancelled_date_time AS CHAR(19)) AS timestamp), EDW1.character_type_code, EDW1.contract_est_amt_ent, EDW1.contract_intent_code, EDW1.contract_nbr, EDW1.contract_nbr_modifier, EDW1.contract_subtype_code, EDW2.country_desc, EDW1.created_by_name, CAST (CAST (EDW1.creation_date_time AS CHAR(19)) AS timestamp), EDW1.curr_code, EDW1.cust_po_nbr, EDW1.cust_po_nbr_rqrd_flag, EDW1.deleted_flag, CAST (EDW1.end_date_time AS date) AS auto_c024, EDW1.est_renew_amt_ent, EDW1.final_price_modifier_text, EDW1.gsa_code, EDW1.inventory_org_id, EDW1.invoice_zero_amt_flag, EDW1.issue_receive_code, CAST (CAST (EDW1.last_update_date_time AS CHAR(19)) AS timestamp), EDW1.last_update_login_name, EDW1.last_updated_by_name, EDW1.migration_contract_nbr, EDW1.orig_sys_id, EDW1.orig_sys_source_code, EDW1.prepay_rqrd_flag, EDW1.qa_check_list_id, EDW2.region_code, EDW2.region_desc, EDW1.renew_curr_code, CAST (EDW1.renew_date_time AS date) AS auto_c042, CAST (EDW1.signed_date_time AS date) AS auto_c043, EDW1.solution_portfolio_id, CAST (EDW1.start_date_time AS date) AS auto_c045, EDW1.status_code, EDW1.subclass_code, EDW1.template_flag, EDW1.template_used_name, EDW1.termination_code, CAST (EDW1.termination_date_time AS date) AS auto_c051, EDW1.termination_reason_code, EDW1.tran_code, EDW1.bill_to_site_use_id, EDW1.curr_cnvrsn_rate, EDW1.curr_cnvrsn_rate_date, EDW1.curr_cnvrsn_type_code, EDW1.document_id, EDW1.invoice_rule_id, EDW1.invoice_term_id, EDW1.price_list_header_id, EDW1.renewal_end_date, EDW1.renewal_type_code, EDW1.ship_to_site_use_id, EDW1.new_for_new_duration, EDW1.renewal_approval_type, EDW1.suspend_credits_flag, null, null, null  FROM  ${EEDDWWDDBB1}.contract_header    AS EDW1   ,  ${VVIIEEWWDDBB1}.geo_rollup_xref_cs_view    AS EDW2 INNER JOIN (SELECT DISTINCT instance_id AS auto_c00, batch_nbr AS auto_c01  FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig ) AS autoAlias_9 ON ( upper(trim(EDW1.instance_id)) = upper(trim(autoAlias_9.AUTO_C00)) AND EDW1.alt_batch_nbr = autoAlias_9.AUTO_C01 ) WHERE EDW1.author_oper_unit_country_code = EDW2.country_code  AND upper(trim(EDW1.subclass_code))  IN (  upper(trim('SERVICE')) , upper(trim('WARRANTY'))  )   AND upper(trim(EDW1.subclass_code))  IN (  upper(trim('SERVICE')) , upper(trim('WARRANTY'))  )
--Chngd NATASHA(CORRECTED the transformed query removed the cross join)
--Removed Distinct(Nikhil)
with qq1 as (SELECT * from ${EEDDWWDDBB1}.contract_header 
                                                                                WHERE
                        upper(trim(subclass_code))  IN (
                            'SERVICE','WARRANTY'  
                        ))
    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1           SELECT 
            EDW1.contract_header_id,
            EDW1.instance_id,
            EDW1.agreement_record_id,
            CAST (CAST (EDW1.approved_date_time AS CHAR(19)) AS timestamp),
            EDW1.archived_flag,
            CASE 
                WHEN trim(EDW1.author_oper_unit_country_code) = trim('US')  THEN 'A6'  
                ELSE EDW2.area_code  
            END AS auto_c05,
            CASE 
                WHEN trim(EDW1.author_oper_unit_country_code) = trim('US')  THEN 'US Other'  
                ELSE EDW2.area_desc  
            END AS auto_c06,
            EDW1.author_ncr_unit_id,
            EDW1.author_oper_unit_country_code,
            EDW1.author_operating_unit_id,
            EDW1.auto_renew_day_cnt,
            CAST (CAST (EDW1.cancelled_date_time AS CHAR(19)) AS timestamp),
            EDW1.character_type_code,
            EDW1.contract_est_amt_ent,
            EDW1.contract_intent_code,
            EDW1.contract_nbr,
            EDW1.contract_nbr_modifier,
            EDW1.contract_subtype_code,
            EDW2.country_desc,
            EDW1.created_by_name,
            CAST (CAST (EDW1.creation_date_time AS CHAR(19)) AS timestamp),
            EDW1.curr_code,
            EDW1.cust_po_nbr,
            EDW1.cust_po_nbr_rqrd_flag,
            EDW1.deleted_flag,
            CAST (EDW1.end_date_time AS date) AS auto_c024,
            EDW1.est_renew_amt_ent,
            EDW1.final_price_modifier_text,
            EDW1.gsa_code,
            EDW1.inventory_org_id,
            EDW1.invoice_zero_amt_flag,
            EDW1.issue_receive_code,
            CAST (CAST (EDW1.last_update_date_time AS CHAR(19)) AS timestamp),
            EDW1.last_update_login_name,
            EDW1.last_updated_by_name,
            EDW1.migration_contract_nbr,
            EDW1.orig_sys_id,
            EDW1.orig_sys_source_code,
            EDW1.prepay_rqrd_flag,
            EDW1.qa_check_list_id,
            EDW2.region_code,
            EDW2.region_desc,
            EDW1.renew_curr_code,
            CAST (EDW1.renew_date_time AS date) AS auto_c042,
            CAST (EDW1.signed_date_time AS date) AS auto_c043,
            EDW1.solution_portfolio_id,
            CAST (EDW1.start_date_time AS date) AS auto_c045,
            EDW1.status_code,
            EDW1.subclass_code,
            EDW1.template_flag,
            EDW1.template_used_name,
            EDW1.termination_code,
            CAST (EDW1.termination_date_time AS date) AS auto_c051,
            EDW1.termination_reason_code,
            EDW1.tran_code,
            EDW1.bill_to_site_use_id,
            EDW1.curr_cnvrsn_rate,
            EDW1.curr_cnvrsn_rate_date,
            EDW1.curr_cnvrsn_type_code,
            EDW1.document_id,
            EDW1.invoice_rule_id,
            EDW1.invoice_term_id,
            EDW1.price_list_header_id,
            EDW1.renewal_end_date,
            EDW1.renewal_type_code,
            EDW1.ship_to_site_use_id,
            EDW1.new_for_new_duration,
            EDW1.renewal_approval_type,
            EDW1.suspend_credits_flag,
            EDW1.hold_reason_description,
            EDW1.agreement_start_date,
            EDW1.agreement_end_date  
        FROM
            ${EEDDWWDDBB1}.contract_header    AS EDW1   
        INNER JOIN
            ${VVIIEEWWDDBB1}.geo_rollup_xref_cs_view    AS EDW2 
                ON EDW1.author_oper_unit_country_code = EDW2.country_code 
        INNER JOIN
            (
                SELECT
                     instance_id AS auto_c00,
                    batch_nbr AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig 
                    group by instance_id, batch_nbr
            ) AS autoAlias_9 
                ON (
                    upper(trim(EDW1.instance_id)) = upper(trim(autoAlias_9.AUTO_C00)) 
                    AND EDW1.alt_batch_nbr = autoAlias_9.AUTO_C01 
                ); 
        

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn_01b.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --LOCK TABLE  ${EEDDWWDDBB1}.contract_header_ld FOR ACCESSLOCK TABLE  ${EEDDWWDDBB1}.contract_line_ld FOR ACCESSLOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1(   contract_header_id ,instance_id    ,agreement_record_id    ,approved_date_time ,archived_flag  ,area_code  ,area_desc  ,author_ncr_unit_id ,author_oper_unit_country_code  ,author_operating_unit_id   ,auto_renew_day_cnt ,cancelled_date_time    ,character_type_code    ,contract_est_amt_ent   ,contract_intent_code   ,contract_nbr   ,contract_nbr_modifier  ,country_desc   ,created_by_name    ,creation_date_time ,curr_code  ,cust_po_nbr    ,cust_po_nbr_rqrd_flag  ,deleted_flag   ,end_date   ,est_renew_amt_ent  ,final_price_modifier_text  ,gsa_code   ,inventory_org_id   ,invoice_zero_amt_flag  ,issue_receive_code ,last_update_date_time  ,last_update_login_name ,last_updated_by_name   ,migration_contract_nbr ,orig_sys_id    ,orig_sys_source_code   ,prepay_rqrd_flag   ,qa_check_list_id   ,region_code    ,region_desc    ,renew_curr_code    ,renew_date ,signed_date    ,solution_portfolio_id  ,start_date ,status_code    ,subclass_code  ,template_flag  ,template_used_name ,termination_code   ,termination_date   ,termination_reason_code    ,tran_code  ,bill_to_site_use_id    ,curr_cnvrsn_rate   ,curr_cnvrsn_rate_date  ,curr_cnvrsn_type_code  ,document_id    ,invoice_rule_id    ,invoice_term_id    ,price_list_header_id   ,renewal_end_date   ,renewal_type_code  ,ship_to_site_use_id        ,contract_subtype_code  ,new_for_new_duration   ,renewal_approval_type        ,suspend_credits_flag)SELECT   contract_header_id ,instance_id    ,agreement_record_id    ,CAST(CAST(approved_date_time AS CHAR(19)) AS TIMESTAMP(0)) ,archived_flag  <WM_COMMENT_START> area_code  <WM_COMMENT_END>                                      ,CASE                                                WHEN EDW1.author_oper_unit_country_code = 'US'          THEN 'A6'                                           ELSE                                                EDW2.area_code                                      END                                                      <WM_COMMENT_START> area_desc  <WM_COMMENT_END>                                         ,CASE                                                   WHEN EDW1.author_oper_unit_country_code = 'US'      THEN 'US Other'                                     ELSE                                                EDW2.area_desc                                      END                                                ,author_ncr_unit_id ,author_oper_unit_country_code  ,author_operating_unit_id   ,auto_renew_day_cnt ,CAST(CAST(cancelled_date_time AS CHAR(19)) AS TIMESTAMP(0))    ,character_type_code    ,contract_est_amt_ent   ,contract_intent_code   ,contract_nbr   ,contract_nbr_modifier  ,EDW2.country_desc  ,created_by_name    ,CAST(CAST(creation_date_time AS CHAR(19)) AS TIMESTAMP(0)) ,curr_code  ,cust_po_nbr    ,cust_po_nbr_rqrd_flag  ,deleted_flag   ,CAST(end_date_time AS DATE)    ,est_renew_amt_ent  ,final_price_modifier_text  ,gsa_code   ,inventory_org_id   ,invoice_zero_amt_flag  ,issue_receive_code ,CAST(CAST(last_update_date_time AS CHAR(19)) AS TIMESTAMP(0))  ,last_update_login_name ,last_updated_by_name   ,migration_contract_nbr ,orig_sys_id    ,orig_sys_source_code   ,prepay_rqrd_flag   ,qa_check_list_id   ,EDW2.region_code   ,EDW2.region_desc   ,renew_curr_code    ,CAST(renew_date_time AS DATE)  ,CAST(signed_date_time AS DATE) ,solution_portfolio_id  ,CAST(start_date_time AS DATE)  ,status_code    ,subclass_code  ,template_flag  ,template_used_name ,termination_code   ,CAST(termination_date_time AS DATE)    ,termination_reason_code    ,tran_code  ,bill_to_site_use_id    ,curr_cnvrsn_rate   ,curr_cnvrsn_rate_date  ,curr_cnvrsn_type_code  ,document_id    ,invoice_rule_id    ,invoice_term_id    ,price_list_header_id   ,renewal_end_date   ,renewal_type_code  ,ship_to_site_use_id         ,EDW1.contract_subtype_code    <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>    ,EDW1.new_for_new_duration  ,EDW1.renewal_approval_type        ,EDW1.suspend_credits_flagFROM ${EEDDWWDDBB1}.contract_header_ld EDW1,     ${VVIIEEWWDDBB1}.geo_rollup_xref_cs_view EDW2WHERE EDW1.author_oper_unit_country_code = EDW2.country_codeAND  (   EDW1.instance_id,   EDW1.contract_header_id) IN (SELECT instance_id,    alt_contract_header_idFROM ${EEDDWWDDBB1}.contract_line_ldWHERE contract_line_style_id IN (1, 9, 10, 14, 18, 19, 25)AND  (  instance_id,    alt_batch_nbr) IN  (SELECT  instance_id,    batch_nbrFROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig)GROUP BY 1, 2 )AND EDW1.subclass_code IN ('SERVICE', 'WARRANTY')AND   (    EDW1.instance_id,   EDW1.contract_header_id) NOT IN  (SELECT    instance_id,    contract_header_idFROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1)

--Translated Query: MANUAL
with qq2 as (SELECT * from ${EEDDWWDDBB1}.contract_header_ld 
                                                                                WHERE
                        upper(trim(subclass_code))  IN (
                            'SERVICE','WARRANTY'  
                        )),
     qq1 AS ( SELECT distinct
        EDW1.contract_header_id ,
        EDW1.instance_id ,
        agreement_record_id ,
        CAST(CAST(approved_date_time AS CHAR(19)) AS TIMESTAMP) ,
        archived_flag ,
        CASE 
            WHEN upper(trim(EDW1.author_oper_unit_country_code)) = upper(trim('US')) THEN 'A6' 
            ELSE EDW2.area_code 
        END AS auto_c05 ,
        CASE 
            WHEN upper(trim(EDW1.author_oper_unit_country_code)) = upper(trim('US')) THEN 'US Other' 
            ELSE EDW2.area_desc 
        END AS auto_c06 ,
        author_ncr_unit_id ,
        author_oper_unit_country_code ,
        author_operating_unit_id ,
        auto_renew_day_cnt ,
        CAST(CAST(cancelled_date_time AS CHAR(19)) AS TIMESTAMP) ,
        character_type_code ,
        contract_est_amt_ent ,
        contract_intent_code ,
        contract_nbr ,
        contract_nbr_modifier ,
        EDW1.contract_subtype_code ,
        EDW2.country_desc ,
        created_by_name ,
        CAST(CAST(creation_date_time AS CHAR(19)) AS TIMESTAMP) ,
        curr_code ,
        cust_po_nbr ,
        cust_po_nbr_rqrd_flag ,
        deleted_flag ,
        CAST(end_date_time AS DATE) AS auto_c024 ,
        est_renew_amt_ent ,
        final_price_modifier_text ,
        gsa_code ,
        inventory_org_id ,
        invoice_zero_amt_flag ,
        issue_receive_code ,
        CAST(CAST(last_update_date_time AS CHAR(19)) AS TIMESTAMP) ,
        last_update_login_name ,
        last_updated_by_name ,
        migration_contract_nbr ,
        orig_sys_id ,
        orig_sys_source_code ,
        prepay_rqrd_flag ,
        qa_check_list_id ,
        EDW2.region_code ,
        EDW2.region_desc ,
        renew_curr_code ,
        CAST(renew_date_time AS DATE) AS auto_c042 ,
        CAST(signed_date_time AS DATE) AS auto_c043 ,
        solution_portfolio_id ,
        CAST(start_date_time AS DATE) AS auto_c045 ,
        status_code ,
        subclass_code ,
        template_flag ,
        template_used_name ,
        termination_code ,
        CAST(termination_date_time AS DATE) AS auto_c051 ,
        termination_reason_code ,
        tran_code ,
        bill_to_site_use_id ,
        curr_cnvrsn_rate ,
        curr_cnvrsn_rate_date ,
        curr_cnvrsn_type_code ,
        document_id ,
        invoice_rule_id ,
        invoice_term_id ,
        price_list_header_id ,
        renewal_end_date ,
        renewal_type_code ,
        ship_to_site_use_id ,
        EDW1.new_for_new_duration ,
        EDW1.renewal_approval_type ,
        EDW1.suspend_credits_flag ,
        EDW1.hold_reason_description ,
        EDW1.agreement_start_date,
        EDW1.agreement_end_date  
FROM qq2 AS EDW1 
INNER JOIN ${VVIIEEWWDDBB1}.geo_rollup_xref_cs_view AS EDW2 
    ON EDW1.author_oper_unit_country_code = EDW2.country_code LEFT SEMI JOIN (
    SELECT  CL.instance_id , CL.alt_contract_header_id 
    FROM    ${EEDDWWDDBB1}.contract_line_ld CL LEFT SEMI JOIN ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig 
    BT 
        on CL.instance_id = 
    BT.instance_id 
        AND CL.alt_batch_nbr = 
    BT.batch_nbr 
    WHERE   contract_line_style_id IN (1, 9, 10, 14, 18, 19, 25) 
    GROUP BY 1, 2 )tb1 
    on EDW1.instance_id = tb1.instance_id 
    and EDW1.contract_header_id = tb1.alt_contract_header_id 
LEFT OUTER JOIN (select  instance_id,contract_header_id from ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1) tb2 
    ON EDW1.instance_id = tb2.instance_id 
    AND EDW1.contract_header_id = tb2.contract_header_id 
WHERE tb2.instance_id is NULL 
     ) INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 SELECT
                * 
            FROM
                qq1;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn_02.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2;


--Original Query:
  --LOCK TABLE  ${EEDDWWDDBB1}.contract_line_ld FOR ACCESSLOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 FOR ACCESSLOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2(      contract_line_id     ,instance_id     ,alt_contract_header_id     ,contract_header_id     ,contract_line_nbr     ,contract_line_style_id     ,created_by_name     ,creation_date_time     ,curr_code     ,display_sequence_nbr     ,end_date     ,exception_flag     ,last_update_date_time     ,last_update_login_name     ,last_updated_by_name     ,negot_renew_price_amt_ent     ,orig_sys_id     ,orig_sys_source_code     ,parent_contract_line_id     ,price_level_flag     ,renew_curr_code     ,renew_date     ,start_date     ,status_code     ,termination_reason_code     ,termination_code     ,termination_date     ,tran_code     ,upg_orig_sys_ref_name     ,warranty_exp_reason_code     ,fml_org_code     ,line_bill_to_site_use_id     ,line_cust_acct_id     ,line_invoice_rule_id     ,line_renewal_type_code     ,line_ship_to_site_use_id     ,reverse_logistics_text  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>     ,routing_comment_text     ,source_location_name)SELECT       EDW1.contract_line_id     ,EDW1.instance_id     ,EDW1.alt_contract_header_id     ,EDW1.contract_header_id     ,EDW1.contract_line_nbr     ,EDW1.contract_line_style_id     ,EDW1.created_by_name     ,CAST(CAST(EDW1.creation_date_time AS CHAR(19)) AS TIMESTAMP(0))     ,EDW1.curr_code     ,EDW1.display_sequence_nbr     ,CAST(EDW1.end_date_time AS DATE)     ,EDW1.exception_flag     ,CAST(CAST(EDW1.last_update_date_time AS CHAR(19)) AS TIMESTAMP(0))     ,EDW1.last_update_login_name     ,EDW1.last_updated_by_name     ,EDW1.negot_renew_price_amt_ent     ,EDW1.orig_sys_id     ,EDW1.orig_sys_source_code     ,EDW1.parent_contract_line_id     ,EDW1.price_level_flag     ,EDW1.renew_curr_code     ,CAST(EDW1.renew_date_time AS DATE)     ,CAST(EDW1.start_date_time AS DATE)     ,EDW1.status_code     ,EDW1.termination_reason_code     ,EDW1.termination_code     ,CAST(EDW1.termination_date_time AS DATE)     ,EDW1.tran_code     ,EDW1.upg_orig_sys_ref_name     ,EDW1.warranty_exp_reason_code     ,COALESCE(EDW2.line_revn_dist_fml_org_code,0)     ,EDW1.bill_to_site_use_id     ,EDW1.cust_acct_id     ,EDW1.invoice_rule_id     ,EDW1.line_renewal_type_code     ,EDW1.ship_to_site_use_id     ,EDW1.reverse_logistics_text  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>     ,EDW1.routing_comment_text     ,EDW1.source_location_nameFROM  ${EEDDWWDDBB1}.contract_line_ld EDW1LEFT OUTER JOIN      ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01 EDW2ON    EDW1.contract_line_id = EDW2.contract_line_idAND   EDW1.instance_id = EDW2.instance_idAND   EDW2.use_line = 1WHERE contract_line_style_id IN (1, 14, 19)AND   (      EDW1.instance_id,      EDW1.alt_contract_header_id) IN (SELECT     instance_id,     contract_header_id FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1)AND EDW1.parent_contract_line_id = ' '

--Translated Query: MANUAL
--      INSERT INTO TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2           SELECT EDW1.contract_line_id, EDW1.instance_id, EDW1.alt_contract_header_id, EDW1.contract_header_id, EDW1.contract_line_nbr, EDW1.contract_line_style_id, EDW1.created_by_name, CAST (CAST (EDW1.creation_date_time AS CHAR(19)) AS timestamp), EDW1.curr_code, EDW1.display_sequence_nbr, CAST (EDW1.end_date_time AS date) AS auto_c010, EDW1.exception_flag, CAST (CAST (EDW1.last_update_date_time AS CHAR(19)) AS timestamp), EDW1.last_update_login_name, EDW1.last_updated_by_name, EDW1.negot_renew_price_amt_ent, EDW1.orig_sys_id, EDW1.orig_sys_source_code, EDW1.parent_contract_line_id, EDW1.price_level_flag, EDW1.renew_curr_code, CAST (EDW1.renew_date_time AS date) AS auto_c021, CAST (EDW1.start_date_time AS date) AS auto_c022, EDW1.status_code, EDW1.termination_reason_code, EDW1.termination_code, CAST (EDW1.termination_date_time AS date) AS auto_c026, EDW1.tran_code, EDW1.upg_orig_sys_ref_name, EDW1.warranty_exp_reason_code, COALESCE( EDW2.line_revn_dist_fml_org_code , 0 ) AS auto_c030, EDW1.bill_to_site_use_id, EDW1.cust_acct_id, EDW1.invoice_rule_id, EDW1.line_renewal_type_code, EDW1.ship_to_site_use_id, null, EDW1.reverse_logistics_text, EDW1.routing_comment_text, EDW1.source_location_name, null, null  FROM  ${EEDDWWDDBB1}.contract_line_ld    AS EDW1   LEFT OUTER  JOIN  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01    AS EDW2    ON EDW1.contract_line_id = EDW2.contract_line_id  AND EDW1.instance_id = EDW2.instance_id   AND EDW2.use_line = 1 INNER JOIN (SELECT DISTINCT instance_id AS auto_c00, contract_header_id AS auto_c01  FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 ) AS autoAlias_15 ON ( EDW1.instance_id = autoAlias_15.AUTO_C00 AND EDW1.alt_contract_header_id = autoAlias_15.AUTO_C01 ) WHERE contract_line_style_id  IN (  1,14,19  )  AND upper(trim(EDW1.parent_contract_line_id)) = upper(trim(' '))
--Chngd NATASHA(Changed QUERY removed trim with space)
--REmoved distinct
    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2           SELECT 
            EDW1.contract_line_id,
            EDW1.instance_id,
            EDW1.alt_contract_header_id,
            EDW1.contract_header_id,
            EDW1.contract_line_nbr,
            EDW1.contract_line_style_id,
            EDW1.created_by_name,
            CAST (CAST (EDW1.creation_date_time AS CHAR(19)) AS timestamp),
            EDW1.curr_code,
            EDW1.display_sequence_nbr,
            CAST (EDW1.end_date_time AS date) AS auto_c010,
            EDW1.exception_flag,
            CAST (CAST (EDW1.last_update_date_time AS CHAR(19)) AS timestamp),
            EDW1.last_update_login_name,
            EDW1.last_updated_by_name,
            EDW1.negot_renew_price_amt_ent,
            EDW1.orig_sys_id,
            EDW1.orig_sys_source_code,
            EDW1.parent_contract_line_id,
            EDW1.price_level_flag,
            EDW1.renew_curr_code,
            CAST (EDW1.renew_date_time AS date) AS auto_c021,
            CAST (EDW1.start_date_time AS date) AS auto_c022,
            EDW1.status_code,
            EDW1.termination_reason_code,
            EDW1.termination_code,
            CAST (EDW1.termination_date_time AS date) AS auto_c026,
            EDW1.tran_code,
            EDW1.upg_orig_sys_ref_name,
            EDW1.warranty_exp_reason_code,
            COALESCE( EDW2.line_revn_dist_fml_org_code ,
            0 ) AS auto_c030,
            EDW1.bill_to_site_use_id,
            EDW1.cust_acct_id,
            EDW1.invoice_rule_id,
            EDW1.line_renewal_type_code,
            EDW1.ship_to_site_use_id,
            null,
            EDW1.reverse_logistics_text,
            EDW1.routing_comment_text,
            EDW1.source_location_name,
            EDW1.mdm_product_identifier,
            EDW1.mdm_solution_identifier
        FROM
            ${EEDDWWDDBB1}.contract_line_ld    AS EDW1   
        LEFT OUTER  JOIN
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_gl_code_combo_t01    AS EDW2    
                ON EDW1.contract_line_id = EDW2.contract_line_id  
                AND EDW1.instance_id = EDW2.instance_id   
                AND EDW2.use_line = 1 
        INNER JOIN
            (
                SELECT
                     instance_id AS auto_c00,
                    contract_header_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 
                    group by instance_id,contract_header_id
            ) AS autoAlias_15 
                ON (
                    upper(trim(EDW1.instance_id)) = upper(trim(autoAlias_15.AUTO_C00)) 
                    AND upper(trim(EDW1.alt_contract_header_id)) = upper(trim(autoAlias_15.AUTO_C01)) 
                ) 
        WHERE
            contract_line_style_id  IN (
                1,14,19  
            )  
            AND trim(
                EDW1.parent_contract_line_id
            ) = trim(
                ' '
            );

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn_03.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t3 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t3;


--Original Query:
  --LOCK TABLE  ${EEDDWWDDBB}.contract_line_ld FOR ACCESSLOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t3(         contract_line_id        ,instance_id        ,alt_contract_header_id        ,contract_header_id        ,contract_line_nbr        ,contract_line_style_id        ,created_by_name        ,creation_date_time        ,curr_code        ,display_sequence_nbr        ,end_date        ,exception_flag        ,last_update_date_time        ,last_update_login_name        ,last_updated_by_name        ,negot_price_amt_ent        ,negot_renew_price_amt_ent        ,orig_sys_id        ,orig_sys_source_code        ,parent_contract_line_id        ,price_level_flag        ,renew_curr_code        ,renew_date        ,sline_sot_order_nbr        ,sline_sot_order_sot_line_nbr        ,sline_sot_line_nbr        ,start_date        ,status_code        ,termination_reason_code        ,termination_code        ,termination_date        ,tran_code        ,unit_price_amt_ent        ,upg_orig_sys_ref_id        ,upg_orig_sys_ref_name        ,warranty_exp_reason_code        ,sline_invoice_rule_id        ,sline_list_price_amt_ent        ,sline_renewal_type_code        ,sline_serviced_qty        ,reverse_logistics_text <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>        ,routing_comment_text        ,source_location_name   ,crb_invoice_nbr_text        ,crb_reference_nbr_text)SELECT         contract_line_id        ,instance_id        ,alt_contract_header_id        ,contract_header_id        ,contract_line_nbr        ,contract_line_style_id        ,created_by_name        ,CAST(CAST(creation_date_time AS CHAR(19)) AS TIMESTAMP(0))        ,curr_code        ,display_sequence_nbr        ,CAST(end_date_time AS DATE)        ,exception_flag        ,CAST(CAST(last_update_date_time AS CHAR(19)) AS TIMESTAMP(0))        ,last_update_login_name        ,last_updated_by_name        ,negot_price_amt_ent        ,negot_renew_price_amt_ent        ,orig_sys_id        ,orig_sys_source_code        ,parent_contract_line_id        ,price_level_flag        ,renew_curr_code        ,CAST(renew_date_time AS DATE)        <WM_COMMENT_START> sline_sot_order_nbr <WM_COMMENT_END>        ,CASE         WHEN POSITION('-' IN sot_order_sot_line_nbr ) <> 0         THEN SUBSTR(sot_order_sot_line_nbr, 1,                POSITION('-' IN  sot_order_sot_line_nbr) -1)         ELSE ' '         END        ,sot_order_sot_line_nbr        <WM_COMMENT_START> sline_sot_line_nbr <WM_COMMENT_END>        ,CASE         WHEN POSITION('-' IN sot_order_sot_line_nbr ) <> 0         THEN SUBSTR(sot_order_sot_line_nbr,                (POSITION('-' IN  sot_order_sot_line_nbr) +1) ,(CHAR(TRIM(sot_order_sot_line_nbr))))         ELSE ' '         END        ,CAST(start_date_time AS DATE)        ,status_code        ,termination_reason_code        ,termination_code        ,CAST(termination_date_time AS DATE)        ,tran_code        ,unit_price_amt_ent        ,upg_orig_sys_ref_id        ,upg_orig_sys_ref_name        ,warranty_exp_reason_code        ,invoice_rule_id        ,line_list_price_amt_ent        ,line_renewal_type_code        ,serviced_qty        ,reverse_logistics_text        ,routing_comment_text        ,source_location_name   ,crb_invoice_nbr_text        ,crb_reference_nbr_textFROM ${EEDDWWDDBB}.contract_line_ld CLWHERE contract_line_style_id IN (9, 10, 18, 25)AND   (        instance_id,        alt_contract_header_id) IN  ( SELECT        instance_id,        contract_header_idFROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1)AND CL.contract_header_id = ''

--Translated Query: MANUAL
--      INSERT INTO TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t3 SELECT contract_line_id ,instance_id ,alt_contract_header_id ,contract_header_id ,contract_line_nbr ,contract_line_style_id ,created_by_name ,CAST(CAST(creation_date_time AS CHAR(19)) AS TIMESTAMP) ,curr_code ,display_sequence_nbr ,CAST(end_date_time AS DATE) AS auto_c010 ,exception_flag ,CAST(CAST(last_update_date_time AS CHAR(19)) AS TIMESTAMP) ,last_update_login_name ,last_updated_by_name ,negot_price_amt_ent ,negot_renew_price_amt_ent ,orig_sys_id ,orig_sys_source_code ,parent_contract_line_id ,price_level_flag ,renew_curr_code ,CAST(renew_date_time AS DATE) AS auto_c022 ,CASE WHEN INSTR(sot_order_sot_line_nbr,'-') <> 0 THEN SUBSTR(sot_order_sot_line_nbr, 1, INSTR(sot_order_sot_line_nbr,'-') - 1) ELSE ' ' END AS auto_c023 ,sot_order_sot_line_nbr ,CASE WHEN INSTR(sot_order_sot_line_nbr,'-') <> 0 THEN SUBSTR(sot_order_sot_line_nbr, (INSTR(sot_order_sot_line_nbr,'-') + 1), (LENGTH(TRIM(sot_order_sot_line_nbr)))) ELSE ' ' END AS auto_c025 ,CAST(start_date_time AS DATE) AS auto_c026 ,status_code ,termination_reason_code ,termination_code ,CAST(termination_date_time AS DATE) AS auto_c030 ,tran_code ,unit_price_amt_ent ,upg_orig_sys_ref_id ,upg_orig_sys_ref_name ,warranty_exp_reason_code ,invoice_rule_id ,line_list_price_amt_ent ,line_renewal_type_code ,serviced_qty ,NULL ,reverse_logistics_text ,routing_comment_text ,source_location_name ,crb_invoice_nbr_text ,crb_reference_nbr_text FROM ${EEDDWWDDBB}.contract_line_ld AS CL INNER JOIN ( SELECT DISTINCT instance_id AS auto_c00 ,contract_header_id AS auto_c01 FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 ) AS autoAlias_17 ON ( upper(trim(instance_id)) = upper(trim(autoAlias_17.AUTO_C00)) AND upper(trim(alt_contract_header_id)) = upper(trim(autoAlias_17.AUTO_C01)) ) WHERE contract_line_style_id IN ( 9 ,10 ,18 ,25 ) AND upper(trim(CL.contract_header_id)) = upper(trim(''))
--Chngd NATASHA(Changed QUERY removed trim with space)
--REmoved distinct
    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t3 SELECT 
            contract_line_id,
            instance_id,
            alt_contract_header_id,
            contract_header_id,
            contract_line_nbr,
            contract_line_style_id,
            created_by_name,
            CAST (CAST (creation_date_time AS CHAR(19)) AS timestamp),
            curr_code,
            display_sequence_nbr,
            CAST (end_date_time AS date) AS auto_c010,
            exception_flag,
            CAST (CAST (last_update_date_time AS CHAR(19)) AS timestamp),
            last_update_login_name,
            last_updated_by_name,
            negot_price_amt_ent,
            negot_renew_price_amt_ent,
            orig_sys_id,
            orig_sys_source_code,
            parent_contract_line_id,
            price_level_flag,
            renew_curr_code,
            CAST (renew_date_time AS date) AS auto_c022,
            CASE 
                WHEN INSTR(sot_order_sot_line_nbr,
                '-' ) <> 0  THEN SUBSTR( sot_order_sot_line_nbr ,
                1 ,
                INSTR( sot_order_sot_line_nbr,
                '-') - 1  )   
                ELSE ' '  
            END AS auto_c023,
            sot_order_sot_line_nbr,
            CASE 
                WHEN INSTR(  sot_order_sot_line_nbr,
                '-') <> 0  THEN SUBSTR( sot_order_sot_line_nbr ,
                ( INSTR(  sot_order_sot_line_nbr,
                '-') + 1 ) ,
                (LENGTH( TRIM( sot_order_sot_line_nbr ) ) ) )   
                ELSE ' '  
            END AS auto_c025,
            CAST (start_date_time AS date) AS auto_c026,
            status_code,
            termination_reason_code,
            termination_code,
            CAST (termination_date_time AS date) AS auto_c030,
            tran_code,
            unit_price_amt_ent,
            upg_orig_sys_ref_id,
            upg_orig_sys_ref_name,
            warranty_exp_reason_code,
            invoice_rule_id,
            line_list_price_amt_ent,
            line_renewal_type_code,
            serviced_qty,
            null,
            reverse_logistics_text,
            routing_comment_text,
            source_location_name,
            crb_invoice_nbr_text,
            crb_reference_nbr_text  
        FROM
            ${EEDDWWDDBB}.contract_line_ld    AS CL 
        INNER JOIN
            (
                SELECT
                     instance_id AS auto_c00,
                    contract_header_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 
                    group by instance_id,contract_header_id
            ) AS autoAlias_17 
                ON (
                    upper(trim(instance_id)) = upper(trim(autoAlias_17.AUTO_C00)) 
                    AND upper(trim(alt_contract_header_id)) = upper(trim(autoAlias_17.AUTO_C01)) 
                ) 
        WHERE
            contract_line_style_id  IN (
                9,10,18,25  
            )  
            AND (
                CL.contract_header_id
            ) = (
                ''
            );

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn_04a.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4;


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_branch_area_xref FOR ACCESSLOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2 FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4(      contract_line_id     ,instance_id     ,alt_contract_header_id     ,contract_header_id     ,contract_line_nbr     ,contract_line_style_id     ,created_by_name     ,creation_date_time     ,curr_code     ,display_sequence_nbr     ,end_date     ,exception_flag     ,last_update_date_time     ,last_update_login_name     ,last_updated_by_name     ,negot_renew_price_amt_ent     ,orig_sys_id     ,orig_sys_source_code     ,parent_contract_line_id     ,price_level_flag     ,renew_curr_code     ,renew_date     ,start_date     ,status_code     ,termination_reason_code     ,termination_code     ,termination_date     ,tran_code     ,upg_orig_sys_ref_name     ,warranty_exp_reason_code     ,fml_org_code     ,area_code     ,area_desc     ,line_bill_to_site_use_id     ,line_cust_acct_id     ,line_invoice_rule_id     ,line_renewal_type_code     ,line_ship_to_site_use_id     ,reverse_logistics_text  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>     ,routing_comment_text      ,source_location_name )SELECT       EDW1.contract_line_id     ,EDW1.instance_id     ,EDW1.alt_contract_header_id     ,EDW1.contract_header_id     ,EDW1.contract_line_nbr     ,EDW1.contract_line_style_id     ,EDW1.created_by_name     ,EDW1.creation_date_time     ,EDW1.curr_code     ,EDW1.display_sequence_nbr     ,EDW1.end_date     ,EDW1.exception_flag     ,EDW1.last_update_date_time     ,EDW1.last_update_login_name     ,EDW1.last_updated_by_name     ,EDW1.negot_renew_price_amt_ent     ,EDW1.orig_sys_id     ,EDW1.orig_sys_source_code     ,EDW1.parent_contract_line_id     ,EDW1.price_level_flag     ,EDW1.renew_curr_code     ,EDW1.renew_date     ,EDW1.start_date     ,EDW1.status_code     ,EDW1.termination_reason_code     ,EDW1.termination_code     ,EDW1.termination_date     ,EDW1.tran_code     ,EDW1.upg_orig_sys_ref_name     ,EDW1.warranty_exp_reason_code     ,EDW1.fml_org_code     ,COALESCE(EDW2.country_area_code,'  ')     ,COALESCE(EDW2.country_area_desc,' ')     ,EDW1.line_bill_to_site_use_id     ,EDW1.line_cust_acct_id     ,EDW1.line_invoice_rule_id     ,EDW1.line_renewal_type_code     ,EDW1.line_ship_to_site_use_id     ,EDW1.reverse_logistics_text  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>     ,EDW1.routing_comment_text      ,EDW1.source_location_name FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2 EDW1,      ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_branch_area_xref EDW2WHERE EDW1.fml_org_code = EDW2.branch_codeAND   EDW1.fml_org_code > 0

--Translated Query: MANUAL
--      INSERT INTO TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4           SELECT EDW1.contract_line_id, EDW1.instance_id, EDW1.alt_contract_header_id, EDW1.contract_header_id, EDW1.contract_line_nbr, EDW1.contract_line_style_id, EDW1.created_by_name, EDW1.creation_date_time, EDW1.curr_code, EDW1.display_sequence_nbr, EDW1.end_date, EDW1.exception_flag, EDW1.last_update_date_time, EDW1.last_update_login_name, EDW1.last_updated_by_name, EDW1.negot_renew_price_amt_ent, EDW1.orig_sys_id, EDW1.orig_sys_source_code, EDW1.parent_contract_line_id, EDW1.price_level_flag, EDW1.renew_curr_code, EDW1.renew_date, EDW1.start_date, EDW1.status_code, EDW1.termination_reason_code, EDW1.termination_code, EDW1.termination_date, EDW1.tran_code, EDW1.upg_orig_sys_ref_name, EDW1.warranty_exp_reason_code, null, EDW1.fml_org_code, null, COALESCE( EDW2.country_area_code , '  ' ) AS auto_c031, COALESCE( EDW2.country_area_desc , ' ' ) AS auto_c032, EDW1.line_bill_to_site_use_id, EDW1.line_cust_acct_id, EDW1.line_invoice_rule_id, EDW1.line_renewal_type_code, EDW1.line_ship_to_site_use_id, null, EDW1.reverse_logistics_text, EDW1.routing_comment_text, EDW1.source_location_name, null, null  FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2    AS EDW1   ,  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_branch_area_xref    AS EDW2   WHERE EDW1.fml_org_code = EDW2.branch_code  AND EDW1.fml_org_code > 0
--Chngd NATASHA(Changed QUERY removed cross join)

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4           SELECT 
            EDW1.contract_line_id,
            EDW1.instance_id,
            EDW1.alt_contract_header_id,
            EDW1.contract_header_id,
            EDW1.contract_line_nbr,
            EDW1.contract_line_style_id,
            EDW1.created_by_name,
            EDW1.creation_date_time,
            EDW1.curr_code,
            EDW1.display_sequence_nbr,
            EDW1.end_date,
            EDW1.exception_flag,
            EDW1.last_update_date_time,
            EDW1.last_update_login_name,
            EDW1.last_updated_by_name,
            EDW1.negot_renew_price_amt_ent,
            EDW1.orig_sys_id,
            EDW1.orig_sys_source_code,
            EDW1.parent_contract_line_id,
            EDW1.price_level_flag,
            EDW1.renew_curr_code,
            EDW1.renew_date,
            EDW1.start_date,
            EDW1.status_code,
            EDW1.termination_reason_code,
            EDW1.termination_code,
            EDW1.termination_date,
            EDW1.tran_code,
            EDW1.upg_orig_sys_ref_name,
            EDW1.warranty_exp_reason_code,
            null,
            EDW1.fml_org_code,
            null,
            COALESCE( EDW2.country_area_code ,
            '  ' ) AS auto_c031,
            COALESCE( EDW2.country_area_desc ,
            ' ' ) AS auto_c032,
            EDW1.line_bill_to_site_use_id,
            EDW1.line_cust_acct_id,
            EDW1.line_invoice_rule_id,
            EDW1.line_renewal_type_code,
            EDW1.line_ship_to_site_use_id,
            null,
            EDW1.reverse_logistics_text,
            EDW1.routing_comment_text,
            EDW1.source_location_name,
            EDW1.mdm_product_identifier,
            EDW1.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2    AS EDW1   
        INNER JOIN
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_branch_area_xref    AS EDW2   
                ON EDW1.fml_org_code = EDW2.branch_code  
        WHERE
            EDW1.fml_org_code > 0;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn_04b.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2 FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4(      contract_line_id     ,instance_id     ,alt_contract_header_id     ,contract_header_id     ,contract_line_nbr     ,contract_line_style_id     ,created_by_name     ,creation_date_time     ,curr_code     ,display_sequence_nbr     ,end_date     ,exception_flag     ,last_update_date_time     ,last_update_login_name     ,last_updated_by_name     ,negot_renew_price_amt_ent     ,orig_sys_id     ,orig_sys_source_code     ,parent_contract_line_id     ,price_level_flag     ,renew_curr_code     ,renew_date     ,start_date     ,status_code     ,termination_reason_code     ,termination_code     ,termination_date     ,tran_code     ,upg_orig_sys_ref_name     ,warranty_exp_reason_code     ,fml_org_code     ,area_code     ,area_desc     ,line_bill_to_site_use_id     ,line_cust_acct_id     ,line_invoice_rule_id     ,line_renewal_type_code     ,line_ship_to_site_use_id     ,reverse_logistics_text  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>     ,routing_comment_text      ,source_location_name )SELECT       EDW1.contract_line_id     ,EDW1.instance_id     ,EDW1.alt_contract_header_id     ,EDW1.contract_header_id     ,EDW1.contract_line_nbr     ,EDW1.contract_line_style_id     ,EDW1.created_by_name     ,EDW1.creation_date_time     ,EDW1.curr_code     ,EDW1.display_sequence_nbr     ,EDW1.end_date     ,EDW1.exception_flag     ,EDW1.last_update_date_time     ,EDW1.last_update_login_name     ,EDW1.last_updated_by_name     ,EDW1.negot_renew_price_amt_ent     ,EDW1.orig_sys_id     ,EDW1.orig_sys_source_code     ,EDW1.parent_contract_line_id     ,EDW1.price_level_flag     ,EDW1.renew_curr_code     ,EDW1.renew_date     ,EDW1.start_date     ,EDW1.status_code     ,EDW1.termination_reason_code     ,EDW1.termination_code     ,EDW1.termination_date     ,EDW1.tran_code     ,EDW1.upg_orig_sys_ref_name     ,EDW1.warranty_exp_reason_code     ,EDW1.fml_org_code      <WM_COMMENT_START> area_code,<WM_COMMENT_END>     ,NULL      <WM_COMMENT_START> area_desc <WM_COMMENT_END>     ,NULL     ,EDW1.line_bill_to_site_use_id     ,EDW1.line_cust_acct_id     ,EDW1.line_invoice_rule_id     ,EDW1.line_renewal_type_code     ,EDW1.line_ship_to_site_use_id     ,EDW1.reverse_logistics_text  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>     ,EDW1.routing_comment_text      ,EDW1.source_location_name FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2 EDW1WHERE (      contract_line_id     ,instance_id) NOT IN (SELECT      contract_line_id     ,instance_idFROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4)

--Translated Query: SUCCESS

    WITH qq1 AS (    SELECT distinct
        EDW1.contract_line_id,
        EDW1.instance_id,
        EDW1.alt_contract_header_id,
        EDW1.contract_header_id,
        EDW1.contract_line_nbr,
        EDW1.contract_line_style_id,
        EDW1.created_by_name,
        EDW1.creation_date_time,
        EDW1.curr_code,
        EDW1.display_sequence_nbr,
        EDW1.end_date,
        EDW1.exception_flag,
        EDW1.last_update_date_time,
        EDW1.last_update_login_name,
        EDW1.last_updated_by_name,
        EDW1.negot_renew_price_amt_ent,
        EDW1.orig_sys_id,
        EDW1.orig_sys_source_code,
        EDW1.parent_contract_line_id,
        EDW1.price_level_flag,
        EDW1.renew_curr_code,
        EDW1.renew_date,
        EDW1.start_date,
        EDW1.status_code,
        EDW1.termination_reason_code,
        EDW1.termination_code,
        EDW1.termination_date,
        EDW1.tran_code,
        EDW1.upg_orig_sys_ref_name,
        EDW1.warranty_exp_reason_code,
        null,
        EDW1.fml_org_code,
        null,
        CAST( NULL AS STRING ),
        CAST( NULL AS STRING ),
        EDW1.line_bill_to_site_use_id,
        EDW1.line_cust_acct_id,
        EDW1.line_invoice_rule_id,
        EDW1.line_renewal_type_code,
        EDW1.line_ship_to_site_use_id,
        null,
        EDW1.reverse_logistics_text,
        EDW1.routing_comment_text,
        EDW1.source_location_name,
        EDW1.mdm_product_identifier,
        EDW1.mdm_solution_identifier
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t2    AS EDW1 
    LEFT OUTER JOIN
        (SELECT
            DISTINCT contract_line_id AS auto_c00,
            instance_id AS auto_c01  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4 ) AS autoAlias_19 
            ON ( upper(trim(contract_line_id)) = upper(trim(autoAlias_19.AUTO_C00)) 
            AND upper(trim(instance_id)) = upper(trim(autoAlias_19.AUTO_C01)) ) 
    WHERE
        autoAlias_19.AUTO_C00 IS  NULL  
        AND autoAlias_19.AUTO_C01 IS  NULL         ) INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4       SELECT
                * 
            FROM
                qq1;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn_05.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t5 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t5;


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 FOR ACCESSLOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4 FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t5(      contract_line_id     ,instance_id     ,agreement_record_id     ,approved_date_time     ,archived_flag     ,area_code     ,area_desc     ,author_ncr_unit_id     ,author_oper_unit_country_code     ,author_operating_unit_id     ,auto_renew_day_cnt     ,cancelled_date_time     ,character_type_code     ,contract_est_amt_ent     ,contract_header_id     ,contract_intent_code     ,contract_line_nbr     ,contract_line_style_id     ,contract_nbr     ,contract_nbr_modifier     ,country_desc     ,created_by_name     ,creation_date_time     ,curr_code     ,cust_po_nbr     ,cust_po_nbr_rqrd_flag     ,deleted_flag     ,end_date     ,entitlement_begin_date     ,entitlement_end_date     ,entitlement_term_date     ,est_renew_amt_ent     ,final_price_modifier_text     ,gsa_code     ,inventory_org_id     ,invoice_zero_amt_flag     ,issue_receive_code     ,last_update_date_time     ,last_update_login_name     ,last_updated_by_name     ,line_created_by_name     ,line_creation_date_time     ,line_curr_code     ,line_display_sequence_nbr     ,line_end_date     ,line_exception_flag     ,line_last_update_date_time     ,line_last_update_login_name     ,line_last_updated_by_name     ,line_ngt_renew_price_amt_ent     ,line_orig_sys_id     ,line_orig_sys_source_code     ,line_price_level_flag     ,line_renew_curr_code     ,line_renew_date     ,line_start_date     ,line_status_code     ,line_term_reason_code     ,line_termination_code     ,line_termination_date     ,line_tran_code     ,line_upg_orig_sys_ref_name     ,line_wrnty_exp_reason_code     ,line_fml_org_code     ,migration_contract_nbr     ,orig_sys_id     ,orig_sys_source_code     ,prepay_rqrd_flag     ,qa_check_list_id     ,region_code     ,region_desc     ,renew_curr_code     ,renew_date     ,signed_date     ,solution_portfolio_id     ,start_date     ,status_code     ,subclass_code     ,template_flag     ,template_used_name     ,termination_code     ,termination_date     ,termination_reason_code     ,tran_code     ,bill_to_site_use_id     ,curr_cnvrsn_rate     ,curr_cnvrsn_rate_date     ,curr_cnvrsn_type_code     ,document_id     ,invoice_rule_id     ,invoice_term_id     ,price_list_header_id     ,renewal_end_date     ,renewal_type_code     ,ship_to_site_use_id     ,line_bill_to_site_use_id     ,line_cust_acct_id     ,line_invoice_rule_id     ,line_renewal_type_code     ,line_ship_to_site_use_id     ,contract_subtype_code     ,new_for_new_duration  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>     ,reverse_logistics_text      ,routing_comment_text      ,source_location_name      ,renewal_approval_type     ,suspend_credits_flag)SELECT      EDW2.contract_line_id     ,EDW1.instance_id     ,EDW1.agreement_record_id     ,EDW1.approved_date_time     ,EDW1.archived_flag     ,COALESCE(EDW2.area_code,EDW1.area_code)     ,COALESCE(EDW2.area_desc,EDW1.area_desc)     ,EDW1.author_ncr_unit_id     ,EDW1.author_oper_unit_country_code     ,EDW1.author_operating_unit_id     ,EDW1.auto_renew_day_cnt     ,EDW1.cancelled_date_time     ,EDW1.character_type_code     ,EDW1.contract_est_amt_ent     ,EDW1.contract_header_id     ,EDW1.contract_intent_code     ,EDW2.contract_line_nbr     ,EDW2.contract_line_style_id     ,EDW1.contract_nbr     ,EDW1.contract_nbr_modifier     ,EDW1.country_desc     ,EDW1.created_by_name     ,EDW1.creation_date_time     ,EDW1.curr_code     ,EDW1.cust_po_nbr     ,EDW1.cust_po_nbr_rqrd_flag     ,EDW1.deleted_flag     ,EDW1.end_date     , <WM_COMMENT_START> entitlement_begin_date <WM_COMMENT_END>     (CASE      WHEN EDW1.start_date IS NULL      THEN EDW2.start_date      WHEN EDW2.start_date IS NULL      THEN EDW1.start_date      WHEN EDW1.start_date > EDW2.start_date      THEN EDW1.start_date      ELSE EDW2.start_date      END)     , <WM_COMMENT_START> entitlement_end_date <WM_COMMENT_END>     (CASE      WHEN EDW1.end_date IS NULL      THEN EDW2.end_date      WHEN EDW2.end_date IS NULL      THEN EDW1.end_date      WHEN EDW1.end_date < EDW2.end_date      THEN EDW1.end_date      ELSE EDW2.end_date      END)     , <WM_COMMENT_START> entitlement_term_date <WM_COMMENT_END>     (CASE      WHEN EDW1.termination_date IS NULL      THEN EDW2.termination_date      WHEN EDW2.termination_date IS NULL      THEN EDW1.termination_date      WHEN EDW1.termination_date < EDW2.termination_date      THEN EDW1.termination_date      ELSE EDW2.termination_date      END)     ,EDW1.est_renew_amt_ent     ,EDW1.final_price_modifier_text     ,EDW1.gsa_code     ,EDW1.inventory_org_id     ,EDW1.invoice_zero_amt_flag     ,EDW1.issue_receive_code     ,EDW1.last_update_date_time     ,EDW1.last_update_login_name     ,EDW1.last_updated_by_name     <WM_COMMENT_START> line_created_by_name  <WM_COMMENT_END>     ,EDW2.created_by_name     <WM_COMMENT_START> line_creation_date_time  <WM_COMMENT_END>     ,EDW2.creation_date_time     <WM_COMMENT_START> line_curr_code  <WM_COMMENT_END>     ,EDW2.curr_code     <WM_COMMENT_START> line_display_sequence_nbr  <WM_COMMENT_END>     ,EDW2.display_sequence_nbr     <WM_COMMENT_START> line_end_date  <WM_COMMENT_END>     ,EDW2.end_date     <WM_COMMENT_START> line_exception_flag  <WM_COMMENT_END>     ,EDW2.exception_flag     <WM_COMMENT_START> line_last_update_date_time  <WM_COMMENT_END>     ,EDW2.last_update_date_time     <WM_COMMENT_START> line_last_update_login_name  <WM_COMMENT_END>     ,EDW2.last_update_login_name     <WM_COMMENT_START> line_last_updated_by_name  <WM_COMMENT_END>     ,EDW2.last_updated_by_name     <WM_COMMENT_START> line_ngt_renew_price_amt_ent  <WM_COMMENT_END>     ,EDW2.negot_renew_price_amt_ent     <WM_COMMENT_START> line_orig_sys_id  <WM_COMMENT_END>     ,EDW2.orig_sys_id     <WM_COMMENT_START> line_orig_sys_source_code  <WM_COMMENT_END>     ,EDW2.orig_sys_source_code     <WM_COMMENT_START> line_price_level_flag  <WM_COMMENT_END>     ,EDW2.price_level_flag     <WM_COMMENT_START> line_renew_curr_code  <WM_COMMENT_END>     ,EDW2.renew_curr_code     <WM_COMMENT_START> line_renew_date  <WM_COMMENT_END>     ,EDW2.renew_date     <WM_COMMENT_START> line_start_date  <WM_COMMENT_END>     ,EDW2.start_date     <WM_COMMENT_START> line_status_code  <WM_COMMENT_END>     ,EDW2.status_code     <WM_COMMENT_START> line_termination_reason_code  <WM_COMMENT_END>     ,EDW2.termination_reason_code     <WM_COMMENT_START> line_termination_code  <WM_COMMENT_END>     ,EDW2.termination_code     <WM_COMMENT_START> line_termination_date  <WM_COMMENT_END>     ,EDW2.termination_date     <WM_COMMENT_START> line_tran_code  <WM_COMMENT_END>     ,EDW2.tran_code     <WM_COMMENT_START> line_upg_orig_sys_ref_name  <WM_COMMENT_END>     ,EDW2.upg_orig_sys_ref_name     <WM_COMMENT_START> line_wrnty_exp_reason_code  <WM_COMMENT_END>     ,EDW2.warranty_exp_reason_code     <WM_COMMENT_START> line_fml_org_code <WM_COMMENT_END>     ,EDW2.fml_org_code     ,EDW1.migration_contract_nbr     ,EDW1.orig_sys_id     ,EDW1.orig_sys_source_code     ,EDW1.prepay_rqrd_flag     ,EDW1.qa_check_list_id     ,EDW1.region_code     ,EDW1.region_desc     ,EDW1.renew_curr_code     ,EDW1.renew_date     ,EDW1.signed_date     ,EDW1.solution_portfolio_id     ,EDW1.start_date     ,EDW1.status_code     ,EDW1.subclass_code     ,EDW1.template_flag     ,EDW1.template_used_name     ,EDW1.termination_code     ,EDW1.termination_date     ,EDW1.termination_reason_code     ,EDW1.tran_code     ,EDW1.bill_to_site_use_id     ,EDW1.curr_cnvrsn_rate     ,EDW1.curr_cnvrsn_rate_date     ,EDW1.curr_cnvrsn_type_code     ,EDW1.document_id     ,EDW1.invoice_rule_id     ,EDW1.invoice_term_id     ,EDW1.price_list_header_id     ,EDW1.renewal_end_date     ,EDW1.renewal_type_code     ,EDW1.ship_to_site_use_id     ,EDW2.line_bill_to_site_use_id     ,EDW2.line_cust_acct_id     ,EDW2.line_invoice_rule_id     ,EDW2.line_renewal_type_code     ,EDW2.line_ship_to_site_use_id          ,EDW1.contract_subtype_code     ,EDW1.new_for_new_duration  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>     ,EDW2.reverse_logistics_text      ,EDW2.routing_comment_text      ,EDW2.source_location_name     ,EDW1.renewal_approval_type     ,EDW1.suspend_credits_flagFROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 EDW1,      ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4 EDW2WHERE EDW1.contract_header_id = EDW2.alt_contract_header_idAND   EDW1.instance_id = EDW2.instance_id

--Translated Query: MANUAL
--      INSERT INTO TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t5           SELECT EDW2.contract_line_id, EDW1.instance_id, EDW1.agreement_record_id, EDW1.approved_date_time, EDW1.archived_flag, COALESCE( EDW2.area_code , EDW1.area_code ) AS auto_c05, COALESCE( EDW2.area_desc , EDW1.area_desc ) AS auto_c06, EDW1.author_ncr_unit_id, EDW1.author_oper_unit_country_code, EDW1.author_operating_unit_id, EDW1.auto_renew_day_cnt, EDW1.cancelled_date_time, EDW1.character_type_code, EDW1.contract_est_amt_ent, EDW1.contract_header_id, EDW1.contract_intent_code, EDW2.contract_line_nbr, EDW2.contract_line_style_id, EDW1.contract_nbr, EDW1.contract_nbr_modifier, EDW1.contract_subtype_code, EDW1.country_desc, EDW1.created_by_name, EDW1.creation_date_time, EDW1.curr_code, EDW1.cust_po_nbr, EDW1.cust_po_nbr_rqrd_flag, EDW1.deleted_flag, EDW1.end_date, CASE WHEN EDW1.start_date  IS NULL  THEN EDW2.start_date  WHEN EDW2.start_date  IS NULL  THEN EDW1.start_date WHEN EDW1.start_date > EDW2.start_date  THEN EDW1.start_date ELSE EDW2.start_date  END AS auto_c028, CASE WHEN EDW1.end_date  IS NULL  THEN EDW2.end_date  WHEN EDW2.end_date  IS NULL  THEN EDW1.end_date WHEN EDW1.end_date < EDW2.end_date  THEN EDW1.end_date ELSE EDW2.end_date  END AS auto_c029, CASE WHEN EDW1.termination_date  IS NULL  THEN EDW2.termination_date  WHEN EDW2.termination_date  IS NULL  THEN EDW1.termination_date WHEN EDW1.termination_date < EDW2.termination_date  THEN EDW1.termination_date ELSE EDW2.termination_date  END AS auto_c030, EDW1.est_renew_amt_ent, EDW1.final_price_modifier_text, EDW1.gsa_code, EDW1.inventory_org_id, EDW1.invoice_zero_amt_flag, EDW1.issue_receive_code, EDW1.last_update_date_time, EDW1.last_update_login_name, EDW1.last_updated_by_name, EDW2.created_by_name, EDW2.creation_date_time, EDW2.curr_code, EDW2.display_sequence_nbr, EDW2.end_date, EDW2.exception_flag, EDW2.last_update_date_time, EDW2.last_update_login_name, EDW2.last_updated_by_name, EDW2.negot_renew_price_amt_ent, EDW2.orig_sys_id, EDW2.orig_sys_source_code, EDW2.price_level_flag, EDW2.renew_curr_code, EDW2.renew_date, EDW2.start_date, EDW2.status_code, EDW2.termination_reason_code, EDW2.termination_code, EDW2.termination_date, EDW2.tran_code, EDW2.upg_orig_sys_ref_name, EDW2.warranty_exp_reason_code, EDW2.fml_org_code, EDW1.migration_contract_nbr, EDW1.orig_sys_id, EDW1.orig_sys_source_code, EDW1.prepay_rqrd_flag, EDW1.qa_check_list_id, EDW1.region_code, EDW1.region_desc, EDW1.renew_curr_code, EDW1.renew_date, EDW1.signed_date, EDW1.solution_portfolio_id, EDW1.start_date, EDW1.status_code, EDW1.subclass_code, EDW1.template_flag, EDW1.template_used_name, EDW1.termination_code, EDW1.termination_date, EDW1.termination_reason_code, EDW1.tran_code, EDW1.bill_to_site_use_id, EDW1.curr_cnvrsn_rate, EDW1.curr_cnvrsn_rate_date, EDW1.curr_cnvrsn_type_code, EDW1.document_id, EDW1.invoice_rule_id, EDW1.invoice_term_id, EDW2.line_bill_to_site_use_id, EDW2.line_cust_acct_id, EDW2.line_invoice_rule_id, EDW2.line_renewal_type_code, EDW2.line_ship_to_site_use_id, EDW1.price_list_header_id, EDW1.renewal_end_date, EDW1.renewal_type_code, EDW1.ship_to_site_use_id, null, null, null, EDW1.new_for_new_duration, EDW2.reverse_logistics_text, EDW2.routing_comment_text, EDW2.source_location_name, EDW1.renewal_approval_type, EDW1.suspend_credits_flag, null, null, null, null, null  FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1    AS EDW1   ,  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4    AS EDW2   WHERE EDW1.contract_header_id = EDW2.alt_contract_header_id  AND EDW1.instance_id = EDW2.instance_id
--Chngd NATASHA(Changed QUERY removed cross join)

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t5           SELECT 
            EDW2.contract_line_id,
            EDW1.instance_id,
            EDW1.agreement_record_id,
            EDW1.approved_date_time,
            EDW1.archived_flag,
            COALESCE( EDW2.area_code ,
            EDW1.area_code ) AS auto_c05,
            COALESCE( EDW2.area_desc ,
            EDW1.area_desc ) AS auto_c06,
            EDW1.author_ncr_unit_id,
            EDW1.author_oper_unit_country_code,
            EDW1.author_operating_unit_id,
            EDW1.auto_renew_day_cnt,
            EDW1.cancelled_date_time,
            EDW1.character_type_code,
            EDW1.contract_est_amt_ent,
            EDW1.contract_header_id,
            EDW1.contract_intent_code,
            EDW2.contract_line_nbr,
            EDW2.contract_line_style_id,
            EDW1.contract_nbr,
            EDW1.contract_nbr_modifier,
            EDW1.contract_subtype_code,
            EDW1.country_desc,
            EDW1.created_by_name,
            EDW1.creation_date_time,
            EDW1.curr_code,
            EDW1.cust_po_nbr,
            EDW1.cust_po_nbr_rqrd_flag,
            EDW1.deleted_flag,
            EDW1.end_date,
            CASE 
                WHEN EDW1.start_date  IS NULL  THEN EDW2.start_date  
                WHEN EDW2.start_date  IS NULL  THEN EDW1.start_date 
                WHEN EDW1.start_date > EDW2.start_date  THEN EDW1.start_date 
                ELSE EDW2.start_date  
            END AS auto_c028,
            CASE 
                WHEN EDW1.end_date  IS NULL  THEN EDW2.end_date  
                WHEN EDW2.end_date  IS NULL  THEN EDW1.end_date 
                WHEN EDW1.end_date < EDW2.end_date  THEN EDW1.end_date 
                ELSE EDW2.end_date  
            END AS auto_c029,
            CASE 
                WHEN EDW1.termination_date  IS NULL  THEN EDW2.termination_date  
                WHEN EDW2.termination_date  IS NULL  THEN EDW1.termination_date 
                WHEN EDW1.termination_date < EDW2.termination_date  THEN EDW1.termination_date 
                ELSE EDW2.termination_date  
            END AS auto_c030,
            EDW1.est_renew_amt_ent,
            EDW1.final_price_modifier_text,
            EDW1.gsa_code,
            EDW1.inventory_org_id,
            EDW1.invoice_zero_amt_flag,
            EDW1.issue_receive_code,
            EDW1.last_update_date_time,
            EDW1.last_update_login_name,
            EDW1.last_updated_by_name,
            EDW2.created_by_name,
            EDW2.creation_date_time,
            EDW2.curr_code,
            EDW2.display_sequence_nbr,
            EDW2.end_date,
            EDW2.exception_flag,
            EDW2.last_update_date_time,
            EDW2.last_update_login_name,
            EDW2.last_updated_by_name,
            EDW2.negot_renew_price_amt_ent,
            EDW2.orig_sys_id,
            EDW2.orig_sys_source_code,
            EDW2.price_level_flag,
            EDW2.renew_curr_code,
            EDW2.renew_date,
            EDW2.start_date,
            EDW2.status_code,
            EDW2.termination_reason_code,
            EDW2.termination_code,
            EDW2.termination_date,
            EDW2.tran_code,
            EDW2.upg_orig_sys_ref_name,
            EDW2.warranty_exp_reason_code,
            EDW2.fml_org_code,
            EDW1.migration_contract_nbr,
            EDW1.orig_sys_id,
            EDW1.orig_sys_source_code,
            EDW1.prepay_rqrd_flag,
            EDW1.qa_check_list_id,
            EDW1.region_code,
            EDW1.region_desc,
            EDW1.renew_curr_code,
            EDW1.renew_date,
            EDW1.signed_date,
            EDW1.solution_portfolio_id,
            EDW1.start_date,
            EDW1.status_code,
            EDW1.subclass_code,
            EDW1.template_flag,
            EDW1.template_used_name,
            EDW1.termination_code,
            EDW1.termination_date,
            EDW1.termination_reason_code,
            EDW1.tran_code,
            EDW1.bill_to_site_use_id,
            EDW1.curr_cnvrsn_rate,
            EDW1.curr_cnvrsn_rate_date,
            EDW1.curr_cnvrsn_type_code,
            EDW1.document_id,
            EDW1.invoice_rule_id,
            EDW1.invoice_term_id,
            EDW2.line_bill_to_site_use_id,
            EDW2.line_cust_acct_id,
            EDW2.line_invoice_rule_id,
            EDW2.line_renewal_type_code,
            EDW2.line_ship_to_site_use_id,
            EDW1.price_list_header_id,
            EDW1.renewal_end_date,
            EDW1.renewal_type_code,
            EDW1.ship_to_site_use_id,
            null,
            null,
            null,
            EDW1.new_for_new_duration,
            EDW2.reverse_logistics_text,
            EDW2.routing_comment_text,
            EDW2.source_location_name,
            EDW1.renewal_approval_type,
            EDW1.suspend_credits_flag,
            EDW1.hold_reason_description,
            EDW1.agreement_start_date,
            EDW1.agreement_end_date  ,
            EDW2.mdm_product_identifier,
            EDW2.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t1    AS EDW1  
        INNER JOIN
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t4    AS EDW2   
                ON EDW1.contract_header_id = EDW2.alt_contract_header_id  
                AND EDW1.instance_id = EDW2.instance_id;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn_06.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t6 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t6;


--Original Query:
  --LOCK TABLE ${TTMMPPDDBB1}. ${AAPPLLIIDD1EENNVV}_contract_dn_t3 FOR ACCESSLOCK TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t5 FOR ACCESSINSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t6(      contract_subline_id     ,instance_id     ,agreement_record_id     ,approved_date_time     ,archived_flag     ,area_code     ,area_desc     ,author_ncr_unit_id     ,author_oper_unit_country_code     ,author_operating_unit_id     ,auto_renew_day_cnt     ,cancelled_date_time     ,character_type_code     ,contract_est_amt_ent     ,contract_header_id     ,contract_intent_code     ,contract_line_id     ,contract_line_nbr     ,contract_line_style_id     ,contract_nbr     ,contract_nbr_modifier     ,contract_subline_nbr     ,contract_subline_style_id     ,country_desc     ,created_by_name     ,creation_date_time     ,curr_code     ,cust_po_nbr     ,cust_po_nbr_rqrd_flag     ,deleted_flag     ,end_date     ,entitlement_begin_date     ,entitlement_end_date     ,entitlement_term_date     ,est_renew_amt_ent     ,final_price_modifier_text     ,gsa_code     ,inventory_org_id     ,invoice_zero_amt_flag     ,issue_receive_code     ,last_update_date_time     ,last_update_login_name     ,last_updated_by_name     ,line_created_by_name     ,line_creation_date_time     ,line_curr_code     ,line_display_sequence_nbr     ,line_end_date     ,line_exception_flag     ,line_last_update_date_time     ,line_last_update_login_name     ,line_last_updated_by_name     ,line_ngt_renew_price_amt_ent     ,line_orig_sys_id     ,line_orig_sys_source_code     ,line_price_level_flag     ,line_renew_curr_code     ,line_renew_date     ,line_start_date     ,line_status_code     ,line_term_reason_code     ,line_termination_code     ,line_termination_date     ,line_tran_code     ,line_upg_orig_sys_ref_name     ,line_wrnty_exp_reason_code     ,line_fml_org_code     ,migration_contract_nbr     ,orig_sys_id     ,orig_sys_source_code     ,prepay_rqrd_flag     ,qa_check_list_id     ,region_code     ,region_desc     ,renew_curr_code     ,renew_date     ,signed_date     ,sline_created_by_name     ,sline_creation_date_time     ,sline_curr_code     ,sline_display_seq_nbr     ,sline_end_date     ,sline_exception_flag     ,sline_last_update_date_time     ,sline_last_update_login_name     ,sline_last_updated_by_name     ,sline_ngt_price_amt_ent     ,sline_ngt_renew_price_amt_ent     ,sline_orig_sys_id     ,sline_orig_sys_source_code     ,sline_parent_contract_line_id     ,sline_price_level_flag     ,sline_renew_curr_code     ,sline_renew_date     ,sline_sot_order_nbr     ,sline_sot_order_sot_line_nbr     ,sline_sot_line_nbr     ,sline_start_date     ,sline_status_code     ,sline_term_reason_code     ,sline_termination_code     ,sline_termination_date     ,sline_tran_code     ,sline_unit_price_amt_ent     ,sline_upg_orig_sys_ref_id     ,sline_upg_orig_sys_ref_name     ,sline_wrnty_exp_reason_code     ,solution_portfolio_id     ,start_date     ,status_code     ,subclass_code     ,template_flag     ,template_used_name     ,termination_code     ,termination_date     ,termination_reason_code     ,tran_code     ,bill_to_site_use_id     ,curr_cnvrsn_rate     ,curr_cnvrsn_rate_date     ,curr_cnvrsn_type_code     ,document_id     ,invoice_rule_id     ,invoice_term_id     ,price_list_header_id     ,renewal_end_date     ,renewal_type_code     ,ship_to_site_use_id     ,line_bill_to_site_use_id     ,line_cust_acct_id     ,line_invoice_rule_id     ,line_renewal_type_code     ,line_ship_to_site_use_id     ,sline_invoice_rule_id     ,sline_list_price_amt_ent     ,sline_renewal_type_code     ,sline_serviced_qty     ,contract_subtype_code     ,new_for_new_duration  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>     ,reverse_logistics_text     ,routing_comment_text     ,source_location_name     ,renewal_approval_type     ,crb_invoice_nbr_text     ,crb_reference_nbr_text     ,suspend_credits_flag)SELECT      <WM_COMMENT_START> contract_subline_id  <WM_COMMENT_END>      EDW1.contract_line_id     ,EDW2.instance_id     ,EDW2.agreement_record_id     ,EDW2.approved_date_time     ,EDW2.archived_flag     ,EDW2.area_code     ,EDW2.area_desc     ,EDW2.author_ncr_unit_id     ,EDW2.author_oper_unit_country_code     ,EDW2.author_operating_unit_id     ,EDW2.auto_renew_day_cnt     ,EDW2.cancelled_date_time     ,EDW2.character_type_code     ,EDW2.contract_est_amt_ent     ,EDW2.contract_header_id     ,EDW2.contract_intent_code     ,EDW2.contract_line_id     ,EDW2.contract_line_nbr     ,EDW2.contract_line_style_id     ,EDW2.contract_nbr     ,EDW2.contract_nbr_modifier     ,EDW1.contract_line_nbr     ,EDW1.contract_line_style_id     ,EDW2.country_desc     ,EDW2.created_by_name     ,EDW2.creation_date_time     ,EDW2.curr_code     ,EDW2.cust_po_nbr     ,EDW2.cust_po_nbr_rqrd_flag     ,EDW2.deleted_flag     ,EDW2.end_date     , <WM_COMMENT_START> entitlement_begin_date <WM_COMMENT_END>     (CASE      WHEN EDW2.entitlement_begin_date IS NULL      THEN EDW1.start_date      WHEN EDW1.start_date IS NULL      THEN EDW2.entitlement_begin_date      WHEN EDW2.entitlement_begin_date > EDW1.start_date      THEN EDW2.entitlement_begin_date      ELSE EDW1.start_date      END)     , <WM_COMMENT_START> entitlement_end_date <WM_COMMENT_END>     (CASE      WHEN EDW2.entitlement_end_date IS NULL      THEN EDW1.end_date      WHEN EDW1.end_date IS NULL      THEN EDW2.entitlement_end_date      WHEN EDW2.entitlement_end_date < EDW1.end_date      THEN EDW2.entitlement_end_date      ELSE EDW1.end_date      END)     , <WM_COMMENT_START> entitlement_term_date <WM_COMMENT_END>     (CASE      WHEN EDW2.entitlement_term_date IS NULL      THEN EDW1.termination_date      WHEN EDW1.termination_date IS NULL      THEN EDW2.entitlement_term_date      WHEN EDW2.entitlement_term_date < EDW1.termination_date      THEN EDW2.entitlement_term_date      ELSE EDW1.termination_date      END)     ,EDW2.est_renew_amt_ent     ,EDW2.final_price_modifier_text     ,EDW2.gsa_code     ,EDW2.inventory_org_id     ,EDW2.invoice_zero_amt_flag     ,EDW2.issue_receive_code     ,EDW2.last_update_date_time     ,EDW2.last_update_login_name     ,EDW2.last_updated_by_name     ,EDW2.line_created_by_name     ,EDW2.line_creation_date_time     ,EDW2.line_curr_code     ,EDW2.line_display_sequence_nbr     ,EDW2.line_end_date     ,EDW2.line_exception_flag     ,EDW2.line_last_update_date_time     ,EDW2.line_last_update_login_name     ,EDW2.line_last_updated_by_name     ,EDW2.line_ngt_renew_price_amt_ent     ,EDW2.line_orig_sys_id     ,EDW2.line_orig_sys_source_code     ,EDW2.line_price_level_flag     ,EDW2.line_renew_curr_code     ,EDW2.line_renew_date     ,EDW2.line_start_date     ,EDW2.line_status_code     ,EDW2.line_term_reason_code     ,EDW2.line_termination_code     ,EDW2.line_termination_date     ,EDW2.line_tran_code     ,EDW2.line_upg_orig_sys_ref_name     ,EDW2.line_wrnty_exp_reason_code     ,EDW2.line_fml_org_code     ,EDW2.migration_contract_nbr     ,EDW2.orig_sys_id     ,EDW2.orig_sys_source_code     ,EDW2.prepay_rqrd_flag     ,EDW2.qa_check_list_id     ,EDW2.region_code     ,EDW2.region_desc     ,EDW2.renew_curr_code     ,EDW2.renew_date     ,EDW2.signed_date     <WM_COMMENT_START> sline_created_by_name  <WM_COMMENT_END>     ,EDW1.created_by_name     <WM_COMMENT_START> sline_creation_date_time  <WM_COMMENT_END>     ,EDW1.creation_date_time     <WM_COMMENT_START> sline_curr_code  <WM_COMMENT_END>     ,EDW1.curr_code     <WM_COMMENT_START> sline_display_sequence_nbr  <WM_COMMENT_END>     ,EDW1.display_sequence_nbr     <WM_COMMENT_START> sline_end_date  <WM_COMMENT_END>     ,EDW1.end_date     <WM_COMMENT_START> sline_exception_flag  <WM_COMMENT_END>     ,EDW1.exception_flag     <WM_COMMENT_START> sline_last_update_date_time  <WM_COMMENT_END>     ,EDW1.last_update_date_time     <WM_COMMENT_START> sline_last_update_login_name  <WM_COMMENT_END>     ,EDW1.last_update_login_name     <WM_COMMENT_START> sline_last_updated_by_name  <WM_COMMENT_END>     ,EDW1.last_updated_by_name     <WM_COMMENT_START> sline_ngt_price_amt_ent  <WM_COMMENT_END>     ,EDW1.negot_price_amt_ent     <WM_COMMENT_START> sline_ngt_renew_price_amt_ent  <WM_COMMENT_END>     ,EDW1.negot_renew_price_amt_ent     <WM_COMMENT_START> sline_orig_sys_id  <WM_COMMENT_END>     ,EDW1.orig_sys_id     <WM_COMMENT_START> sline_orig_sys_source_code  <WM_COMMENT_END>     ,EDW1.orig_sys_source_code     <WM_COMMENT_START> sline_parent_contract_line_id  <WM_COMMENT_END>     ,EDW1.parent_contract_line_id     <WM_COMMENT_START> sline_price_level_flag  <WM_COMMENT_END>     ,EDW1.price_level_flag     <WM_COMMENT_START> sline_renew_curr_code  <WM_COMMENT_END>     ,EDW1.renew_curr_code     <WM_COMMENT_START> sline_renew_date  <WM_COMMENT_END>     ,EDW1.renew_date     <WM_COMMENT_START> sline_sot_order_nbr <WM_COMMENT_END>     ,EDW1.sline_sot_order_nbr     <WM_COMMENT_START> sline_sot_order_sot_line_nbr  <WM_COMMENT_END>     ,EDW1.sline_sot_order_sot_line_nbr     <WM_COMMENT_START> sline_sot_line_nbr  <WM_COMMENT_END>     ,EDW1.sline_sot_line_nbr     <WM_COMMENT_START> sline_start_date  <WM_COMMENT_END>     ,EDW1.start_date     <WM_COMMENT_START> sline_status_code  <WM_COMMENT_END>     ,EDW1.status_code     <WM_COMMENT_START> sline_term_reason_code  <WM_COMMENT_END>     ,EDW1.termination_reason_code     <WM_COMMENT_START> sline_termination_code  <WM_COMMENT_END>     ,EDW1.termination_code     <WM_COMMENT_START> sline_termination_date  <WM_COMMENT_END>     ,EDW1.termination_date     <WM_COMMENT_START> sline_tran_code  <WM_COMMENT_END>     ,EDW1.tran_code     <WM_COMMENT_START> sline_unit_price_amt_ent  <WM_COMMENT_END>     ,EDW1.unit_price_amt_ent     <WM_COMMENT_START> sline_upg_orig_sys_ref_id  <WM_COMMENT_END>     ,EDW1.upg_orig_sys_ref_id     <WM_COMMENT_START> sline_upg_orig_sys_ref_name  <WM_COMMENT_END>     ,EDW1.upg_orig_sys_ref_name     <WM_COMMENT_START> sline_wrnty_exp_reason_code  <WM_COMMENT_END>     ,EDW1.warranty_exp_reason_code     ,EDW2.solution_portfolio_id     ,EDW2.start_date     ,EDW2.status_code     ,EDW2.subclass_code     ,EDW2.template_flag     ,EDW2.template_used_name     ,EDW2.termination_code     ,EDW2.termination_date     ,EDW2.termination_reason_code     ,EDW2.tran_code     ,EDW2.bill_to_site_use_id     ,EDW2.curr_cnvrsn_rate     ,EDW2.curr_cnvrsn_rate_date     ,EDW2.curr_cnvrsn_type_code     ,EDW2.document_id     ,EDW2.invoice_rule_id     ,EDW2.invoice_term_id     ,EDW2.price_list_header_id     ,EDW2.renewal_end_date     ,EDW2.renewal_type_code     ,EDW2.ship_to_site_use_id     ,EDW2.line_bill_to_site_use_id     ,EDW2.line_cust_acct_id     ,EDW2.line_invoice_rule_id     ,EDW2.line_renewal_type_code     ,EDW2.line_ship_to_site_use_id     ,EDW1.sline_invoice_rule_id     ,EDW1.sline_list_price_amt_ent     ,EDW1.sline_renewal_type_code     ,EDW1.sline_serviced_qty     ,EDW2.contract_subtype_code     ,EDW2.new_for_new_duration  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>     ,EDW2.reverse_logistics_text     ,EDW2.routing_comment_text     ,EDW2.source_location_name     ,EDW2.renewal_approval_type     ,EDW1.crb_invoice_nbr_text     ,EDW1.crb_reference_nbr_text     ,EDW2.suspend_credits_flagFROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t3 EDW1,      ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t5 EDW2WHERE EDW1.parent_contract_line_id = EDW2.contract_line_idAND   EDW1.instance_id = EDW2.instance_id

--Translated Query: MANUAL
--      INSERT INTO TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t6           SELECT EDW1.contract_line_id, EDW2.instance_id, EDW2.agreement_record_id, EDW2.approved_date_time, EDW2.archived_flag, EDW2.area_code, EDW2.area_desc, EDW2.author_ncr_unit_id, EDW2.author_oper_unit_country_code, EDW2.author_operating_unit_id, EDW2.auto_renew_day_cnt, EDW2.cancelled_date_time, EDW2.character_type_code, EDW2.contract_est_amt_ent, EDW2.contract_header_id, EDW2.contract_intent_code, EDW2.contract_line_id, EDW2.contract_line_nbr, EDW2.contract_line_style_id, EDW2.contract_nbr, EDW2.contract_nbr_modifier, EDW1.contract_line_nbr, EDW1.contract_line_style_id, EDW2.contract_subtype_code, EDW2.country_desc, EDW2.created_by_name, EDW2.creation_date_time, EDW2.curr_code, EDW2.cust_po_nbr, EDW2.cust_po_nbr_rqrd_flag, EDW2.deleted_flag, EDW2.end_date, CASE WHEN EDW2.entitlement_begin_date  IS NULL  THEN EDW1.start_date  WHEN EDW1.start_date  IS NULL  THEN EDW2.entitlement_begin_date WHEN EDW2.entitlement_begin_date > EDW1.start_date  THEN EDW2.entitlement_begin_date ELSE EDW1.start_date  END AS auto_c031, CASE WHEN EDW2.entitlement_end_date  IS NULL  THEN EDW1.end_date  WHEN EDW1.end_date  IS NULL  THEN EDW2.entitlement_end_date WHEN EDW2.entitlement_end_date < EDW1.end_date  THEN EDW2.entitlement_end_date ELSE EDW1.end_date  END AS auto_c032, CASE WHEN EDW2.entitlement_term_date  IS NULL  THEN EDW1.termination_date  WHEN EDW1.termination_date  IS NULL  THEN EDW2.entitlement_term_date WHEN EDW2.entitlement_term_date < EDW1.termination_date  THEN EDW2.entitlement_term_date ELSE EDW1.termination_date  END AS auto_c033, EDW2.est_renew_amt_ent, EDW2.final_price_modifier_text, EDW2.gsa_code, EDW2.inventory_org_id, EDW2.invoice_zero_amt_flag, EDW2.issue_receive_code, EDW2.last_update_date_time, EDW2.last_update_login_name, EDW2.last_updated_by_name, EDW2.line_created_by_name, EDW2.line_creation_date_time, EDW2.line_curr_code, EDW2.line_display_sequence_nbr, EDW2.line_end_date, EDW2.line_exception_flag, EDW2.line_last_update_date_time, EDW2.line_last_update_login_name, EDW2.line_last_updated_by_name, EDW2.line_ngt_renew_price_amt_ent, EDW2.line_orig_sys_id, EDW2.line_orig_sys_source_code, EDW2.line_price_level_flag, EDW2.line_renew_curr_code, EDW2.line_renew_date, EDW2.line_start_date, EDW2.line_status_code, EDW2.line_term_reason_code, EDW2.line_termination_code, EDW2.line_termination_date, EDW2.line_tran_code, EDW2.line_upg_orig_sys_ref_name, EDW2.line_wrnty_exp_reason_code, EDW2.line_fml_org_code, EDW2.migration_contract_nbr, EDW2.orig_sys_id, EDW2.orig_sys_source_code, EDW2.prepay_rqrd_flag, EDW2.qa_check_list_id, EDW2.region_code, EDW2.region_desc, EDW2.renew_curr_code, EDW2.renew_date, EDW2.signed_date, EDW1.created_by_name, EDW1.creation_date_time, EDW1.curr_code, EDW1.display_sequence_nbr, EDW1.end_date, EDW1.exception_flag, EDW1.last_update_date_time, EDW1.last_update_login_name, EDW1.last_updated_by_name, EDW1.negot_price_amt_ent, EDW1.negot_renew_price_amt_ent, EDW1.orig_sys_id, EDW1.orig_sys_source_code, EDW1.parent_contract_line_id, EDW1.price_level_flag, EDW1.renew_curr_code, EDW1.renew_date, EDW1.sline_sot_order_nbr, EDW1.sline_sot_order_sot_line_nbr, EDW1.sline_sot_line_nbr, EDW1.start_date, EDW1.status_code, EDW1.termination_reason_code, EDW1.termination_code, EDW1.termination_date, EDW1.tran_code, EDW1.unit_price_amt_ent, EDW1.upg_orig_sys_ref_id, EDW1.upg_orig_sys_ref_name, EDW1.warranty_exp_reason_code, EDW2.solution_portfolio_id, EDW2.start_date, EDW2.status_code, EDW2.subclass_code, EDW2.template_flag, EDW2.template_used_name, EDW2.termination_code, EDW2.termination_date, EDW2.termination_reason_code, EDW2.tran_code, EDW2.bill_to_site_use_id, EDW2.curr_cnvrsn_rate, EDW2.curr_cnvrsn_rate_date, EDW2.curr_cnvrsn_type_code, EDW2.document_id, EDW2.invoice_rule_id, EDW2.invoice_term_id, EDW2.line_bill_to_site_use_id, EDW2.line_cust_acct_id, EDW2.line_invoice_rule_id, EDW2.line_renewal_type_code, EDW2.line_ship_to_site_use_id, EDW2.price_list_header_id, EDW2.renewal_end_date, EDW2.renewal_type_code, EDW2.ship_to_site_use_id, EDW1.sline_invoice_rule_id, EDW1.sline_list_price_amt_ent, EDW1.sline_renewal_type_code, EDW1.sline_serviced_qty, EDW2.new_for_new_duration, EDW2.reverse_logistics_text, EDW2.routing_comment_text, EDW2.source_location_name, EDW2.renewal_approval_type, EDW1.crb_invoice_nbr_text, EDW1.crb_reference_nbr_text, EDW2.suspend_credits_flag, null, null, null, null, null  FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t3    AS EDW1   ,  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t5    AS EDW2   WHERE EDW1.parent_contract_line_id = EDW2.contract_line_id  AND EDW1.instance_id = EDW2.instance_id
--Chngd NATASHA(Changed QUERY removed cross join)
--REmoved distinct
    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t6           SELECT 
            EDW1.contract_line_id,
            EDW2.instance_id,
            EDW2.agreement_record_id,
            EDW2.approved_date_time,
            EDW2.archived_flag,
            EDW2.area_code,
            EDW2.area_desc,
            EDW2.author_ncr_unit_id,
            EDW2.author_oper_unit_country_code,
            EDW2.author_operating_unit_id,
            EDW2.auto_renew_day_cnt,
            EDW2.cancelled_date_time,
            EDW2.character_type_code,
            EDW2.contract_est_amt_ent,
            EDW2.contract_header_id,
            EDW2.contract_intent_code,
            EDW2.contract_line_id,
            EDW2.contract_line_nbr,
            EDW2.contract_line_style_id,
            EDW2.contract_nbr,
            EDW2.contract_nbr_modifier,
            EDW1.contract_line_nbr,
            EDW1.contract_line_style_id,
            EDW2.contract_subtype_code,
            EDW2.country_desc,
            EDW2.created_by_name,
            EDW2.creation_date_time,
            EDW2.curr_code,
            EDW2.cust_po_nbr,
            EDW2.cust_po_nbr_rqrd_flag,
            EDW2.deleted_flag,
            EDW2.end_date,
            CASE 
                WHEN EDW2.entitlement_begin_date  IS NULL  THEN EDW1.start_date  
                WHEN EDW1.start_date  IS NULL  THEN EDW2.entitlement_begin_date 
                WHEN EDW2.entitlement_begin_date > EDW1.start_date  THEN EDW2.entitlement_begin_date 
                ELSE EDW1.start_date  
            END AS auto_c031,
            CASE 
                WHEN EDW2.entitlement_end_date  IS NULL  THEN EDW1.end_date  
                WHEN EDW1.end_date  IS NULL  THEN EDW2.entitlement_end_date 
                WHEN EDW2.entitlement_end_date < EDW1.end_date  THEN EDW2.entitlement_end_date 
                ELSE EDW1.end_date  
            END AS auto_c032,
            CASE 
                WHEN EDW2.entitlement_term_date  IS NULL  THEN EDW1.termination_date  
                WHEN EDW1.termination_date  IS NULL  THEN EDW2.entitlement_term_date 
                WHEN EDW2.entitlement_term_date < EDW1.termination_date  THEN EDW2.entitlement_term_date 
                ELSE EDW1.termination_date  
            END AS auto_c033,
            EDW2.est_renew_amt_ent,
            EDW2.final_price_modifier_text,
            EDW2.gsa_code,
            EDW2.inventory_org_id,
            EDW2.invoice_zero_amt_flag,
            EDW2.issue_receive_code,
            EDW2.last_update_date_time,
            EDW2.last_update_login_name,
            EDW2.last_updated_by_name,
            EDW2.line_created_by_name,
            EDW2.line_creation_date_time,
            EDW2.line_curr_code,
            EDW2.line_display_sequence_nbr,
            EDW2.line_end_date,
            EDW2.line_exception_flag,
            EDW2.line_last_update_date_time,
            EDW2.line_last_update_login_name,
            EDW2.line_last_updated_by_name,
            EDW2.line_ngt_renew_price_amt_ent,
            EDW2.line_orig_sys_id,
            EDW2.line_orig_sys_source_code,
            EDW2.line_price_level_flag,
            EDW2.line_renew_curr_code,
            EDW2.line_renew_date,
            EDW2.line_start_date,
            EDW2.line_status_code,
            EDW2.line_term_reason_code,
            EDW2.line_termination_code,
            EDW2.line_termination_date,
            EDW2.line_tran_code,
            EDW2.line_upg_orig_sys_ref_name,
            EDW2.line_wrnty_exp_reason_code,
            EDW2.line_fml_org_code,
            EDW2.migration_contract_nbr,
            EDW2.orig_sys_id,
            EDW2.orig_sys_source_code,
            EDW2.prepay_rqrd_flag,
            EDW2.qa_check_list_id,
            EDW2.region_code,
            EDW2.region_desc,
            EDW2.renew_curr_code,
            EDW2.renew_date,
            EDW2.signed_date,
            EDW1.created_by_name,
            EDW1.creation_date_time,
            EDW1.curr_code,
            EDW1.display_sequence_nbr,
            EDW1.end_date,
            EDW1.exception_flag,
            EDW1.last_update_date_time,
            EDW1.last_update_login_name,
            EDW1.last_updated_by_name,
            EDW1.negot_price_amt_ent,
            EDW1.negot_renew_price_amt_ent,
            EDW1.orig_sys_id,
            EDW1.orig_sys_source_code,
            EDW1.parent_contract_line_id,
            EDW1.price_level_flag,
            EDW1.renew_curr_code,
            EDW1.renew_date,
            EDW1.sline_sot_order_nbr,
            EDW1.sline_sot_order_sot_line_nbr,
            EDW1.sline_sot_line_nbr,
            EDW1.start_date,
            EDW1.status_code,
            EDW1.termination_reason_code,
            EDW1.termination_code,
            EDW1.termination_date,
            EDW1.tran_code,
            EDW1.unit_price_amt_ent,
            EDW1.upg_orig_sys_ref_id,
            EDW1.upg_orig_sys_ref_name,
            EDW1.warranty_exp_reason_code,
            EDW2.solution_portfolio_id,
            EDW2.start_date,
            EDW2.status_code,
            EDW2.subclass_code,
            EDW2.template_flag,
            EDW2.template_used_name,
            EDW2.termination_code,
            EDW2.termination_date,
            EDW2.termination_reason_code,
            EDW2.tran_code,
            EDW2.bill_to_site_use_id,
            EDW2.curr_cnvrsn_rate,
            EDW2.curr_cnvrsn_rate_date,
            EDW2.curr_cnvrsn_type_code,
            EDW2.document_id,
            EDW2.invoice_rule_id,
            EDW2.invoice_term_id,
            EDW2.line_bill_to_site_use_id,
            EDW2.line_cust_acct_id,
            EDW2.line_invoice_rule_id,
            EDW2.line_renewal_type_code,
            EDW2.line_ship_to_site_use_id,
            EDW2.price_list_header_id,
            EDW2.renewal_end_date,
            EDW2.renewal_type_code,
            EDW2.ship_to_site_use_id,
            EDW1.sline_invoice_rule_id,
            EDW1.sline_list_price_amt_ent,
            EDW1.sline_renewal_type_code,
            EDW1.sline_serviced_qty,
            EDW2.new_for_new_duration,
            EDW2.reverse_logistics_text,
            EDW2.routing_comment_text,
            EDW2.source_location_name,
            EDW2.renewal_approval_type,
            EDW1.crb_invoice_nbr_text,
            EDW1.crb_reference_nbr_text,
            EDW2.suspend_credits_flag,
            EDW2.hold_reason_description,
            EDW2.agreement_start_date,
            EDW2.agreement_end_date,  
            EDW2.mdm_product_identifier,
            EDW2.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t3    AS EDW1   
        INNER JOIN
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t5    AS EDW2   
                ON EDW1.parent_contract_line_id = EDW2.contract_line_id  
                AND EDW1.instance_id = EDW2.instance_id;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn_ld_1.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${EEDDWWDDBB}.contract_dn_ld CDLWHERE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t6.contract_subline_id = CDL.contract_subline_idAND    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t6.instance_id = CDL.instance_id

--Translated Query: MANUAL
--INSERT OVERWRITE TABLE ${EEDDWWDDBB}.contract_dn_ld SELECT CDL.* FROM  ${EEDDWWDDBB}.contract_dn_ld  AS CDL LEFT JOIN ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_dn_t6 CD ON CD.contract_subline_id = CDL.contract_subline_id AND CD.instance_id = CDL.instance_id WHERE  CD.contract_subline_id IS NULL AND  CD.instance_id IS NULL
--Transformed Query (Mohit, Comments: Removed NOT while using join condition and replaced OR conditions with IS NULLs)

    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB}.contract_dn_ld SELECT distinct 
            CDL.* 
        FROM
            ${EEDDWWDDBB}.contract_dn_ld    AS CDL  
        LEFT JOIN
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t6 AS DN 
                ON DN.contract_subline_id = CDL.contract_subline_id  
                AND DN.instance_id = CDL.instance_id 
        WHERE
            DN.contract_subline_id IS NULL 
            AND DN.instance_id IS NULL;


--Original Query:
  --DROP INDEX  (     sline_sot_order_nbr  )ON ${EEDDWWDDBB}.contract_dn_ld

--Translated Query: MANUAL
--      DROP INDEX IF EXISTS sline_sot_order_nbr ON ${EEDDWWDDBB}.contract_dn_ld


--Original Query:
  --DROP INDEX  (     contract_nbr  )ON ${EEDDWWDDBB}.contract_dn_ld

--Translated Query: MANUAL
--      DROP INDEX IF EXISTS contract_nbr ON ${EEDDWWDDBB}.contract_dn_ld


--Original Query:
  --LOCKING TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t6 FOR ACCESSLOCKING TABLE ${EEDDWWDDBB}.geo_rollup_xref  FOR ACCESSINSERT INTO ${EEDDWWDDBB}.contract_dn_ld  (       contract_subline_id      ,instance_id      ,agreement_record_id      ,approved_date_time      ,archived_flag      ,area_code      ,area_desc      ,author_ncr_unit_id      ,author_oper_unit_country_code      ,author_operating_unit_id      ,auto_renew_day_cnt      ,cancelled_date_time      ,character_type_code      ,contract_est_amt_ent      ,contract_header_id      ,contract_intent_code      ,contract_line_id      ,contract_line_nbr      ,contract_line_style_id      ,contract_nbr      ,contract_nbr_modifier      ,contract_subline_nbr      ,contract_subline_style_id      ,country_desc      ,created_by_name      ,creation_date_time      ,curr_code      ,cust_po_nbr      ,cust_po_nbr_rqrd_flag      ,deleted_flag      ,end_date      ,entitlement_begin_date      ,entitlement_end_date      ,entitlement_final_date      ,entitlement_term_date      ,entitlement_tran_code      ,est_renew_amt_ent      ,final_price_modifier_text      ,gsa_code      ,inventory_org_id      ,invoice_zero_amt_flag      ,issue_receive_code      ,last_update_date_time      ,last_update_login_name      ,last_updated_by_name      ,line_created_by_name      ,line_creation_date_time      ,line_curr_code      ,line_display_sequence_nbr      ,line_end_date      ,line_exception_flag      ,line_last_update_date_time      ,line_last_update_login_name      ,line_last_updated_by_name      ,line_ngt_renew_price_amt_ent      ,line_orig_sys_id      ,line_orig_sys_source_code      ,line_price_level_flag      ,line_renew_curr_code      ,line_renew_date      ,line_start_date      ,line_status_code      ,line_term_reason_code      ,line_termination_code      ,line_termination_date      ,line_tran_code      ,line_upg_orig_sys_ref_name      ,line_fml_org_code      ,line_wrnty_exp_reason_code      ,migration_contract_nbr      ,orig_sys_id      ,orig_sys_source_code      ,prepay_rqrd_flag      ,qa_check_list_id      ,region_code      ,region_desc      ,renew_curr_code      ,renew_date      ,signed_date      ,sline_created_by_name      ,sline_creation_date_time      ,sline_curr_code      ,sline_display_seq_nbr      ,sline_end_date      ,sline_exception_flag      ,sline_last_update_date_time      ,sline_last_update_login_name      ,sline_last_updated_by_name      ,sline_ngt_price_amt_ent      ,sline_ngt_renew_price_amt_ent      ,sline_orig_sys_id      ,sline_orig_sys_source_code      ,sline_parent_contract_line_id      ,sline_price_level_flag      ,sline_renew_curr_code      ,sline_renew_date      ,sline_sot_order_nbr      ,sline_sot_order_sot_line_nbr      ,sline_sot_line_nbr      ,sline_start_date      ,sline_status_code      ,sline_term_reason_code      ,sline_termination_code      ,sline_termination_date      ,sline_tran_code      ,sline_unit_price_amt_ent      ,sline_upg_orig_sys_ref_id      ,sline_upg_orig_sys_ref_name      ,sline_wrnty_exp_reason_code      ,solution_portfolio_id      ,start_date      ,status_code      ,subclass_code      ,template_flag      ,template_used_name      ,termination_code      ,termination_date      ,termination_reason_code      ,tran_code      ,update_date_time      ,bill_to_site_use_id      ,curr_cnvrsn_rate      ,curr_cnvrsn_rate_date      ,curr_cnvrsn_type_code      ,document_id      ,invoice_rule_id      ,invoice_term_id      ,line_bill_to_site_use_id      ,line_cust_acct_id      ,line_invoice_rule_id      ,line_renewal_type_code      ,line_ship_to_site_use_id      ,price_list_header_id      ,renewal_end_date      ,renewal_type_code      ,ship_to_site_use_id      ,sline_invoice_rule_id      ,sline_list_price_amt_ent      ,sline_renewal_type_code      ,sline_serviced_qty      ,sub_region_code      ,sub_region_name      ,contract_subtype_code      ,new_for_new_duration  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>      ,reverse_logistics_text      ,routing_comment_text      ,source_location_name      ,renewal_approval_type      ,crb_invoice_nbr_text      ,crb_reference_nbr_text      ,suspend_credits_flag  )SELECT       EDW1.contract_subline_id      ,EDW1.instance_id      ,COALESCE(EDW1.agreement_record_id,' ')      ,EDW1.approved_date_time      ,COALESCE(EDW1.archived_flag,' ')      ,COALESCE(EDW1.area_code,' ')      ,COALESCE(EDW1.area_desc,' ')      ,COALESCE(EDW1.author_ncr_unit_id,' ')      ,COALESCE(EDW1.author_oper_unit_country_code,' ')      ,COALESCE(EDW1.author_operating_unit_id,0)      ,COALESCE(EDW1.auto_renew_day_cnt,0)      ,EDW1.cancelled_date_time      ,COALESCE(EDW1.character_type_code,' ')      ,COALESCE(EDW1.contract_est_amt_ent,0)      ,COALESCE(EDW1.contract_header_id,' ')      ,COALESCE(EDW1.contract_intent_code,' ')      ,COALESCE(EDW1.contract_line_id,' ')      ,COALESCE(EDW1.contract_line_nbr,' ')      ,COALESCE(EDW1.contract_line_style_id,0)      ,COALESCE(EDW1.contract_nbr,' ')      ,COALESCE(EDW1.contract_nbr_modifier,' ')      ,COALESCE(EDW1.contract_subline_nbr ,' ')      ,COALESCE(EDW1.contract_subline_style_id,0)      ,COALESCE(EDW1.country_desc,' ')      ,COALESCE(EDW1.created_by_name,' ')      ,EDW1.creation_date_time      ,COALESCE(EDW1.curr_code,' ')      ,COALESCE(EDW1.cust_po_nbr,' ')      ,COALESCE(EDW1.cust_po_nbr_rqrd_flag,' ')      ,COALESCE(EDW1.deleted_flag,' ')      ,EDW1.end_date      ,EDW1.entitlement_begin_date      , <WM_COMMENT_START> entitlement_end_date <WM_COMMENT_END>      (CASE       WHEN EDW1.entitlement_term_date IS NULL       THEN EDW1.entitlement_end_date       WHEN EDW1.entitlement_term_date < EDW1.entitlement_end_date       THEN EDW1.entitlement_term_date       ELSE EDW1.entitlement_end_date       END)       <WM_COMMENT_START> entitlement_final_date <WM_COMMENT_END>      ,EDW1.entitlement_end_date      ,EDW1.entitlement_term_date      , <WM_COMMENT_START> entitlement_tran_code <WM_COMMENT_END>      (CASE       WHEN EDW1.tran_code = 'D'       OR   EDW1.line_tran_code = 'D'       OR   EDW1.sline_tran_code = 'D'       OR   EDW1.tran_code IS NULL       OR   EDW1.line_tran_code IS NULL       OR   EDW1.sline_tran_code IS NULL       THEN 'D'       ELSE 'U'       END)      ,COALESCE(EDW1.est_renew_amt_ent,0)      ,COALESCE(EDW1.final_price_modifier_text,' ')      ,COALESCE(EDW1.gsa_code,' ')      ,COALESCE(EDW1.inventory_org_id,0)      ,COALESCE(EDW1.invoice_zero_amt_flag,' ')      ,COALESCE(EDW1.issue_receive_code,' ')      ,EDW1.last_update_date_time      ,COALESCE(EDW1.last_update_login_name,' ')      ,COALESCE(EDW1.last_updated_by_name,' ')      ,COALESCE(EDW1.line_created_by_name,' ')      ,EDW1.line_creation_date_time      ,COALESCE(EDW1.line_curr_code,' ')      ,COALESCE(EDW1.line_display_sequence_nbr,0)      ,EDW1.line_end_date      ,COALESCE(EDW1.line_exception_flag,' ')      ,EDW1.line_last_update_date_time      ,COALESCE(EDW1.line_last_update_login_name,' ')      ,COALESCE(EDW1.line_last_updated_by_name,' ')      ,COALESCE(EDW1.line_ngt_renew_price_amt_ent,0)      ,COALESCE(EDW1.line_orig_sys_id,' ')      ,COALESCE(EDW1.line_orig_sys_source_code,' ')      ,COALESCE(EDW1.line_price_level_flag,' ')      ,COALESCE(EDW1.line_renew_curr_code,' ')      ,EDW1.line_renew_date      ,EDW1.line_start_date      ,COALESCE(EDW1.line_status_code,' ')      ,COALESCE(EDW1.line_term_reason_code,' ')      ,COALESCE(EDW1.line_termination_code,' ')      ,EDW1.line_termination_date      ,COALESCE(EDW1.line_tran_code,' ')      ,COALESCE(EDW1.line_upg_orig_sys_ref_name,' ')      ,COALESCE(EDW1.line_fml_org_code,' ')      ,COALESCE(EDW1.line_wrnty_exp_reason_code,' ')      ,COALESCE(EDW1.migration_contract_nbr,' ')      ,COALESCE(EDW1.orig_sys_id,' ')      ,COALESCE(EDW1.orig_sys_source_code,' ')      ,COALESCE(EDW1.prepay_rqrd_flag,' ')      ,COALESCE(EDW1.qa_check_list_id,' ')      ,COALESCE(EDW1.region_code,' ')      ,COALESCE(EDW1.region_desc,' ')      ,COALESCE(EDW1.renew_curr_code,' ')      ,EDW1.renew_date      ,EDW1.signed_date      ,COALESCE(EDW1.sline_created_by_name ,' ')      ,EDW1.sline_creation_date_time      ,COALESCE(EDW1.sline_curr_code,' ')      ,COALESCE(EDW1.sline_display_seq_nbr,0)      ,EDW1.sline_end_date      ,COALESCE(EDW1.sline_exception_flag,' ')      ,EDW1.sline_last_update_date_time      ,COALESCE(EDW1.sline_last_update_login_name,' ')      ,COALESCE(EDW1.sline_last_updated_by_name,' ')      ,COALESCE(EDW1.sline_ngt_price_amt_ent,0)      ,COALESCE(EDW1.sline_ngt_renew_price_amt_ent,0)      ,COALESCE(EDW1.sline_orig_sys_id,' ')      ,COALESCE(EDW1.sline_orig_sys_source_code,' ')      ,COALESCE(EDW1.sline_parent_contract_line_id,' ')      ,COALESCE(EDW1.sline_price_level_flag,' ')      ,COALESCE(EDW1.sline_renew_curr_code,' ')      ,EDW1.sline_renew_date      ,COALESCE(EDW1.sline_sot_order_nbr,' ')      ,COALESCE(EDW1.sline_sot_order_sot_line_nbr,' ')      ,COALESCE(EDW1.sline_sot_line_nbr,' ')      ,EDW1.sline_start_date      ,COALESCE(EDW1.sline_status_code,' ')      ,COALESCE(EDW1.sline_term_reason_code,' ')      ,COALESCE(EDW1.sline_termination_code,' ')      ,EDW1.sline_termination_date      ,COALESCE(EDW1.sline_tran_code,' ')      ,COALESCE(EDW1.sline_unit_price_amt_ent,0)      ,COALESCE(EDW1.sline_upg_orig_sys_ref_id,' ')      ,COALESCE(EDW1.sline_upg_orig_sys_ref_name,' ')      ,COALESCE(EDW1.sline_wrnty_exp_reason_code,' ')      ,COALESCE(EDW1.solution_portfolio_id,' ')      ,EDW1.start_date      ,COALESCE(EDW1.status_code,' ')      ,COALESCE(EDW1.subclass_code,' ')      ,COALESCE(EDW1.template_flag,' ')      ,COALESCE(EDW1.template_used_name,' ')      ,COALESCE(EDW1.termination_code,' ')      ,EDW1.termination_date      ,COALESCE(EDW1.termination_reason_code,' ')      ,COALESCE(EDW1.tran_code,' ')      ,CURRENT_TIMESTAMP(0)      ,COALESCE(EDW1.bill_to_site_use_id,0)      ,COALESCE(EDW1.curr_cnvrsn_rate,0)      ,EDW1.curr_cnvrsn_rate_date      ,COALESCE(EDW1.curr_cnvrsn_type_code,' ')      ,COALESCE(EDW1.document_id,0)      ,COALESCE(EDW1.invoice_rule_id,0)      ,COALESCE(EDW1.invoice_term_id,0)      ,COALESCE(EDW1.line_bill_to_site_use_id,0)      ,COALESCE(EDW1.line_cust_acct_id,0)      ,COALESCE(EDW1.line_invoice_rule_id,0)      ,COALESCE(EDW1.line_renewal_type_code,' ')      ,COALESCE(EDW1.line_ship_to_site_use_id,0)      ,COALESCE(EDW1.price_list_header_id,0)      ,EDW1.renewal_end_date      ,COALESCE(EDW1.renewal_type_code,' ')      ,COALESCE(EDW1.ship_to_site_use_id,0)      ,COALESCE(EDW1.sline_invoice_rule_id,0)      ,COALESCE(EDW1.sline_list_price_amt_ent,0)      ,COALESCE(EDW1.sline_renewal_type_code,' ')      ,COALESCE(EDW1.sline_serviced_qty,0)      ,COALESCE(EDW2.sub_region_code,' ')      ,COALESCE(EDW2.sub_region_name,' ')      ,COALESCE(EDW1.contract_subtype_code,' ')      ,EDW1.new_for_new_duration  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>      ,EDW1.reverse_logistics_text      ,EDW1.routing_comment_text      ,EDW1.source_location_name      ,EDW1.renewal_approval_type      ,EDW1.crb_invoice_nbr_text      ,EDW1.crb_reference_nbr_text      ,EDW1.suspend_credits_flag FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t6 EDW1 LEFT OUTER JOIN      ${EEDDWWDDBB}.geo_rollup_xref  EDW2 ON   EDW1.author_oper_unit_country_code = EDW2.country_code AND  EDW2.business_unit_code = 'B4' WHERE sline_sot_order_nbr = ' '

--Translated Query: MANUAL
--Removed distinct

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB}.contract_dn_ld           SELECT
            COALESCE( EDW1.contract_nbr ,
            ' ' ) AS auto_c019,
            COALESCE( EDW1.contract_nbr_modifier ,
            ' ' ) AS auto_c020,
            COALESCE( EDW1.contract_line_nbr ,
            ' ' ) AS auto_c017,
            COALESCE( EDW1.contract_subline_nbr ,
            ' ' ) AS auto_c021,
            EDW1.instance_id,
            COALESCE( EDW1.agreement_record_id ,
            ' ' ) AS auto_c02,
            EDW1.approved_date_time,
            COALESCE( EDW1.archived_flag ,
            ' ' ) AS auto_c04,
            COALESCE( EDW1.area_code ,
            ' ' ) AS auto_c05,
            COALESCE( EDW1.area_desc ,
            ' ' ) AS auto_c06,
            COALESCE( EDW1.author_ncr_unit_id ,
            ' ' ) AS auto_c07,
            COALESCE( EDW1.author_oper_unit_country_code ,
            ' ' ) AS auto_c08,
            COALESCE( EDW1.author_operating_unit_id ,
            0 ) AS auto_c09,
            COALESCE( EDW1.auto_renew_day_cnt ,
            0 ) AS auto_c010,
            EDW1.cancelled_date_time,
            COALESCE( EDW1.character_type_code ,
            ' ' ) AS auto_c012,
            COALESCE( EDW1.contract_est_amt_ent ,
            0 ) AS auto_c013,
            COALESCE( EDW1.contract_header_id ,
            ' ' ) AS auto_c014,
            COALESCE( EDW1.contract_intent_code ,
            ' ' ) AS auto_c015,
            COALESCE( EDW1.contract_line_id ,
            ' ' ) AS auto_c016,
            COALESCE( EDW1.contract_line_style_id ,
            0 ) AS auto_c018,
            EDW1.contract_subline_id,
            COALESCE( EDW1.contract_subline_style_id ,
            0 ) AS auto_c022,
            COALESCE( EDW1.country_desc ,
            ' ' ) AS auto_c023,
            COALESCE( EDW1.created_by_name ,
            ' ' ) AS auto_c024,
            EDW1.creation_date_time,
            COALESCE( EDW1.curr_code ,
            ' ' ) AS auto_c026,
            COALESCE( EDW1.cust_po_nbr ,
            ' ' ) AS auto_c027,
            COALESCE( EDW1.cust_po_nbr_rqrd_flag ,
            ' ' ) AS auto_c028,
            COALESCE( EDW1.deleted_flag ,
            ' ' ) AS auto_c029,
            EDW1.end_date,
            EDW1.entitlement_begin_date,
            CASE 
                WHEN EDW1.entitlement_term_date  IS NULL  THEN EDW1.entitlement_end_date  
                WHEN EDW1.entitlement_term_date < EDW1.entitlement_end_date  THEN EDW1.entitlement_term_date 
                ELSE EDW1.entitlement_end_date  
            END AS auto_c032,
            EDW1.entitlement_end_date,
            EDW1.entitlement_term_date,
            CASE 
                WHEN EDW1.tran_code = 'D'  
                OR EDW1.line_tran_code = 'D'   
                OR EDW1.sline_tran_code = 'D'   
                OR EDW1.tran_code  IS NULL   
                OR EDW1.line_tran_code  IS NULL   
                OR EDW1.sline_tran_code  IS NULL   THEN 'D'  
                ELSE 'U'  
            END AS auto_c035,
            COALESCE( EDW1.est_renew_amt_ent ,
            0 ) AS auto_c036,
            COALESCE( EDW1.final_price_modifier_text ,
            ' ' ) AS auto_c037,
            COALESCE( EDW1.gsa_code ,
            ' ' ) AS auto_c038,
            COALESCE( EDW1.inventory_org_id ,
            0 ) AS auto_c039,
            COALESCE( EDW1.invoice_zero_amt_flag ,
            ' ' ) AS auto_c040,
            COALESCE( EDW1.issue_receive_code ,
            ' ' ) AS auto_c041,
            EDW1.last_update_date_time,
            COALESCE( EDW1.last_update_login_name ,
            ' ' ) AS auto_c043,
            COALESCE( EDW1.last_updated_by_name ,
            ' ' ) AS auto_c044,
            COALESCE( EDW1.line_created_by_name ,
            ' ' ) AS auto_c045,
            EDW1.line_creation_date_time,
            COALESCE( EDW1.line_curr_code ,
            ' ' ) AS auto_c047,
            COALESCE( EDW1.line_display_sequence_nbr ,
            0 ) AS auto_c048,
            EDW1.line_end_date,
            COALESCE( EDW1.line_exception_flag ,
            ' ' ) AS auto_c050,
            EDW1.line_last_update_date_time,
            COALESCE( EDW1.line_last_update_login_name ,
            ' ' ) AS auto_c052,
            COALESCE( EDW1.line_last_updated_by_name ,
            ' ' ) AS auto_c053,
            COALESCE( EDW1.line_ngt_renew_price_amt_ent ,
            0 ) AS auto_c054,
            COALESCE( EDW1.line_orig_sys_id ,
            ' ' ) AS auto_c055,
            COALESCE( EDW1.line_orig_sys_source_code ,
            ' ' ) AS auto_c056,
            COALESCE( EDW1.line_price_level_flag ,
            ' ' ) AS auto_c057,
            COALESCE( EDW1.line_renew_curr_code ,
            ' ' ) AS auto_c058,
            EDW1.line_renew_date,
            EDW1.line_start_date,
            COALESCE( EDW1.line_status_code ,
            ' ' ) AS auto_c061,
            COALESCE( EDW1.line_term_reason_code ,
            ' ' ) AS auto_c062,
            COALESCE( EDW1.line_termination_code ,
            ' ' ) AS auto_c063,
            EDW1.line_termination_date,
            COALESCE( EDW1.line_tran_code ,
            ' ' ) AS auto_c065,
            COALESCE( EDW1.line_upg_orig_sys_ref_name ,
            ' ' ) AS auto_c066,
            COALESCE( EDW1.line_wrnty_exp_reason_code ,
            ' ' ) AS auto_c068,
            COALESCE( EDW1.line_fml_org_code ,
            ' ' ) AS auto_c067,
            COALESCE( EDW1.migration_contract_nbr ,
            ' ' ) AS auto_c069,
            COALESCE( EDW1.orig_sys_id ,
            ' ' ) AS auto_c070,
            COALESCE( EDW1.orig_sys_source_code ,
            ' ' ) AS auto_c071,
            COALESCE( EDW1.prepay_rqrd_flag ,
            ' ' ) AS auto_c072,
            COALESCE( EDW1.qa_check_list_id ,
            ' ' ) AS auto_c073,
            COALESCE( EDW1.region_code ,
            ' ' ) AS auto_c074,
            COALESCE( EDW1.region_desc ,
            ' ' ) AS auto_c075,
            COALESCE( EDW1.renew_curr_code ,
            ' ' ) AS auto_c076,
            EDW1.renew_date,
            EDW1.signed_date,
            COALESCE( EDW1.sline_created_by_name ,
            ' ' ) AS auto_c079,
            EDW1.sline_creation_date_time,
            COALESCE( EDW1.sline_curr_code ,
            ' ' ) AS auto_c081,
            COALESCE( EDW1.sline_display_seq_nbr ,
            0 ) AS auto_c082,
            EDW1.sline_end_date,
            COALESCE( EDW1.sline_exception_flag ,
            ' ' ) AS auto_c084,
            EDW1.sline_last_update_date_time,
            COALESCE( EDW1.sline_last_update_login_name ,
            ' ' ) AS auto_c086,
            COALESCE( EDW1.sline_last_updated_by_name ,
            ' ' ) AS auto_c087,
            COALESCE( EDW1.sline_ngt_price_amt_ent ,
            0 ) AS auto_c088,
            COALESCE( EDW1.sline_ngt_renew_price_amt_ent ,
            0 ) AS auto_c089,
            COALESCE( EDW1.sline_orig_sys_id ,
            ' ' ) AS auto_c090,
            COALESCE( EDW1.sline_orig_sys_source_code ,
            ' ' ) AS auto_c091,
            COALESCE( EDW1.sline_parent_contract_line_id ,
            ' ' ) AS auto_c092,
            COALESCE( EDW1.sline_price_level_flag ,
            ' ' ) AS auto_c093,
            COALESCE( EDW1.sline_renew_curr_code ,
            ' ' ) AS auto_c094,
            EDW1.sline_renew_date,
            COALESCE( EDW1.sline_sot_order_nbr ,
            ' ' ) AS auto_c096,
            COALESCE( EDW1.sline_sot_order_sot_line_nbr ,
            ' ' ) AS auto_c097,
            COALESCE( EDW1.sline_sot_line_nbr ,
            ' ' ) AS auto_c098,
            EDW1.sline_start_date,
            COALESCE( EDW1.sline_status_code ,
            ' ' ) AS auto_c0100,
            COALESCE( EDW1.sline_term_reason_code ,
            ' ' ) AS auto_c0101,
            COALESCE( EDW1.sline_termination_code ,
            ' ' ) AS auto_c0102,
            EDW1.sline_termination_date,
            COALESCE( EDW1.sline_tran_code ,
            ' ' ) AS auto_c0104,
            COALESCE( EDW1.sline_unit_price_amt_ent ,
            0 ) AS auto_c0105,
            COALESCE( EDW1.sline_upg_orig_sys_ref_id ,
            ' ' ) AS auto_c0106,
            COALESCE( EDW1.sline_upg_orig_sys_ref_name ,
            ' ' ) AS auto_c0107,
            COALESCE( EDW1.sline_wrnty_exp_reason_code ,
            ' ' ) AS auto_c0108,
            COALESCE( EDW1.solution_portfolio_id ,
            ' ' ) AS auto_c0109,
            EDW1.start_date,
            COALESCE( EDW1.status_code ,
            ' ' ) AS auto_c0111,
            COALESCE( EDW1.subclass_code ,
            ' ' ) AS auto_c0112,
            COALESCE( EDW1.template_flag ,
            ' ' ) AS auto_c0113,
            COALESCE( EDW1.template_used_name ,
            ' ' ) AS auto_c0114,
            COALESCE( EDW1.termination_code ,
            ' ' ) AS auto_c0115,
            EDW1.termination_date,
            COALESCE( EDW1.termination_reason_code ,
            ' ' ) AS auto_c0117,
            COALESCE( EDW1.tran_code ,
            ' ' ) AS auto_c0118,
            CURRENT_TIMESTAMP() AS auto_c0119,
            COALESCE( EDW1.bill_to_site_use_id ,
            0 ) AS auto_c0120,
            COALESCE( EDW1.curr_cnvrsn_rate ,
            0 ) AS auto_c0121,
            EDW1.curr_cnvrsn_rate_date,
            COALESCE( EDW1.curr_cnvrsn_type_code ,
            ' ' ) AS auto_c0123,
            COALESCE( EDW1.document_id ,
            0 ) AS auto_c0124,
            COALESCE( EDW1.invoice_rule_id ,
            0 ) AS auto_c0125,
            COALESCE( EDW1.invoice_term_id ,
            0 ) AS auto_c0126,
            COALESCE( EDW1.line_bill_to_site_use_id ,
            0 ) AS auto_c0127,
            COALESCE( EDW1.line_cust_acct_id ,
            0 ) AS auto_c0128,
            COALESCE( EDW1.line_invoice_rule_id ,
            0 ) AS auto_c0129,
            COALESCE( EDW1.line_renewal_type_code ,
            ' ' ) AS auto_c0130,
            COALESCE( EDW1.line_ship_to_site_use_id ,
            0 ) AS auto_c0131,
            COALESCE( EDW1.price_list_header_id ,
            0 ) AS auto_c0132,
            EDW1.renewal_end_date,
            COALESCE( EDW1.renewal_type_code ,
            ' ' ) AS auto_c0134,
            COALESCE( EDW1.ship_to_site_use_id ,
            0 ) AS auto_c0135,
            COALESCE( EDW1.sline_invoice_rule_id ,
            0 ) AS auto_c0136,
            COALESCE( EDW1.sline_list_price_amt_ent ,
            0 ) AS auto_c0137,
            COALESCE( EDW1.sline_renewal_type_code ,
            ' ' ) AS auto_c0138,
            COALESCE( EDW1.sline_serviced_qty ,
            0 ) AS auto_c0139,
            COALESCE( EDW2.sub_region_code ,
            ' ' ) AS auto_c0140,
            COALESCE( EDW2.sub_region_name ,
            ' ' ) AS auto_c0141,
            COALESCE( EDW1.contract_subtype_code ,
            ' ' ) AS auto_c0142,
            EDW1.new_for_new_duration,
            EDW1.routing_comment_text,
            EDW1.reverse_logistics_text,
            EDW1.source_location_name,
            EDW1.renewal_approval_type,
            EDW1.crb_invoice_nbr_text,
            EDW1.crb_reference_nbr_text,
            EDW1.suspend_credits_flag,
            EDW1.hold_reason_description,
            EDW1.agreement_start_date,
            EDW1.agreement_end_date  ,
            EDW1.mdm_product_identifier,
            EDW1.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t6    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB}.geo_rollup_xref    AS EDW2    
                ON EDW1.author_oper_unit_country_code = EDW2.country_code  
                AND upper(trim(EDW2.business_unit_code)) = upper(trim('B4'))    
        WHERE
            upper(trim(sline_sot_order_nbr)) = upper(trim(' '));


--Original Query:
  --LOCKING TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t6 FOR ACCESSLOCKING TABLE ${EEDDWWDDBB}.geo_rollup_xref  FOR ACCESSINSERT INTO ${EEDDWWDDBB}.contract_dn_ld  (       contract_subline_id      ,instance_id      ,agreement_record_id      ,approved_date_time      ,archived_flag      ,area_code      ,area_desc      ,author_ncr_unit_id      ,author_oper_unit_country_code      ,author_operating_unit_id      ,auto_renew_day_cnt      ,cancelled_date_time      ,character_type_code      ,contract_est_amt_ent      ,contract_header_id      ,contract_intent_code      ,contract_line_id      ,contract_line_nbr      ,contract_line_style_id      ,contract_nbr      ,contract_nbr_modifier      ,contract_subline_nbr      ,contract_subline_style_id      ,country_desc      ,created_by_name      ,creation_date_time      ,curr_code      ,cust_po_nbr      ,cust_po_nbr_rqrd_flag      ,deleted_flag      ,end_date      ,entitlement_begin_date      ,entitlement_end_date      ,entitlement_final_date      ,entitlement_term_date      ,entitlement_tran_code      ,est_renew_amt_ent      ,final_price_modifier_text      ,gsa_code      ,inventory_org_id      ,invoice_zero_amt_flag      ,issue_receive_code      ,last_update_date_time      ,last_update_login_name      ,last_updated_by_name      ,line_created_by_name      ,line_creation_date_time      ,line_curr_code      ,line_display_sequence_nbr      ,line_end_date      ,line_exception_flag      ,line_last_update_date_time      ,line_last_update_login_name      ,line_last_updated_by_name      ,line_ngt_renew_price_amt_ent      ,line_orig_sys_id      ,line_orig_sys_source_code      ,line_price_level_flag      ,line_renew_curr_code      ,line_renew_date      ,line_start_date      ,line_status_code      ,line_term_reason_code      ,line_termination_code      ,line_termination_date      ,line_tran_code      ,line_upg_orig_sys_ref_name      ,line_fml_org_code      ,line_wrnty_exp_reason_code      ,migration_contract_nbr      ,orig_sys_id      ,orig_sys_source_code      ,prepay_rqrd_flag      ,qa_check_list_id      ,region_code      ,region_desc      ,renew_curr_code      ,renew_date      ,signed_date      ,sline_created_by_name      ,sline_creation_date_time      ,sline_curr_code      ,sline_display_seq_nbr      ,sline_end_date      ,sline_exception_flag      ,sline_last_update_date_time      ,sline_last_update_login_name      ,sline_last_updated_by_name      ,sline_ngt_price_amt_ent      ,sline_ngt_renew_price_amt_ent      ,sline_orig_sys_id      ,sline_orig_sys_source_code      ,sline_parent_contract_line_id      ,sline_price_level_flag      ,sline_renew_curr_code      ,sline_renew_date      ,sline_sot_order_nbr      ,sline_sot_order_sot_line_nbr      ,sline_sot_line_nbr      ,sline_start_date      ,sline_status_code      ,sline_term_reason_code      ,sline_termination_code      ,sline_termination_date      ,sline_tran_code      ,sline_unit_price_amt_ent      ,sline_upg_orig_sys_ref_id      ,sline_upg_orig_sys_ref_name      ,sline_wrnty_exp_reason_code      ,solution_portfolio_id      ,start_date      ,status_code      ,subclass_code      ,template_flag      ,template_used_name      ,termination_code      ,termination_date      ,termination_reason_code      ,tran_code      ,update_date_time      ,bill_to_site_use_id      ,curr_cnvrsn_rate      ,curr_cnvrsn_rate_date      ,curr_cnvrsn_type_code      ,document_id      ,invoice_rule_id      ,invoice_term_id      ,line_bill_to_site_use_id      ,line_cust_acct_id      ,line_invoice_rule_id      ,line_renewal_type_code      ,line_ship_to_site_use_id      ,price_list_header_id      ,renewal_end_date      ,renewal_type_code      ,ship_to_site_use_id      ,sline_invoice_rule_id      ,sline_list_price_amt_ent      ,sline_renewal_type_code      ,sline_serviced_qty      ,sub_region_code      ,sub_region_name      ,contract_subtype_code      ,new_for_new_duration  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>      ,reverse_logistics_text      ,routing_comment_text      ,source_location_name      ,renewal_approval_type      ,crb_invoice_nbr_text      ,crb_reference_nbr_text      ,suspend_credits_flag  )SELECT       EDW1.contract_subline_id      ,EDW1.instance_id      ,COALESCE(EDW1.agreement_record_id,' ')      ,EDW1.approved_date_time      ,COALESCE(EDW1.archived_flag,' ')      ,COALESCE(EDW1.area_code,' ')      ,COALESCE(EDW1.area_desc,' ')      ,COALESCE(EDW1.author_ncr_unit_id,' ')      ,COALESCE(EDW1.author_oper_unit_country_code,' ')      ,COALESCE(EDW1.author_operating_unit_id,0)      ,COALESCE(EDW1.auto_renew_day_cnt,0)      ,EDW1.cancelled_date_time      ,COALESCE(EDW1.character_type_code,' ')      ,COALESCE(EDW1.contract_est_amt_ent,0)      ,COALESCE(EDW1.contract_header_id,' ')      ,COALESCE(EDW1.contract_intent_code,' ')      ,COALESCE(EDW1.contract_line_id,' ')      ,COALESCE(EDW1.contract_line_nbr,' ')      ,COALESCE(EDW1.contract_line_style_id,0)      ,COALESCE(EDW1.contract_nbr,' ')      ,COALESCE(EDW1.contract_nbr_modifier,' ')      ,COALESCE(EDW1.contract_subline_nbr ,' ')      ,COALESCE(EDW1.contract_subline_style_id,0)      ,COALESCE(EDW1.country_desc,' ')      ,COALESCE(EDW1.created_by_name,' ')      ,EDW1.creation_date_time      ,COALESCE(EDW1.curr_code,' ')      ,COALESCE(EDW1.cust_po_nbr,' ')      ,COALESCE(EDW1.cust_po_nbr_rqrd_flag,' ')      ,COALESCE(EDW1.deleted_flag,' ')      ,EDW1.end_date      ,EDW1.entitlement_begin_date      , <WM_COMMENT_START> entitlement_end_date <WM_COMMENT_END>      (CASE       WHEN EDW1.entitlement_term_date IS NULL       THEN EDW1.entitlement_end_date       WHEN EDW1.entitlement_term_date < EDW1.entitlement_end_date       THEN EDW1.entitlement_term_date       ELSE EDW1.entitlement_end_date       END)       <WM_COMMENT_START> entitlement_final_date <WM_COMMENT_END>      ,EDW1.entitlement_end_date      ,EDW1.entitlement_term_date      , <WM_COMMENT_START> entitlement_tran_code <WM_COMMENT_END>      (CASE       WHEN EDW1.tran_code = 'D'       OR   EDW1.line_tran_code = 'D'       OR   EDW1.sline_tran_code = 'D'       OR   EDW1.tran_code IS NULL       OR   EDW1.line_tran_code IS NULL       OR   EDW1.sline_tran_code IS NULL       THEN 'D'       ELSE 'U'       END)      ,COALESCE(EDW1.est_renew_amt_ent,0)      ,COALESCE(EDW1.final_price_modifier_text,' ')      ,COALESCE(EDW1.gsa_code,' ')      ,COALESCE(EDW1.inventory_org_id,0)      ,COALESCE(EDW1.invoice_zero_amt_flag,' ')      ,COALESCE(EDW1.issue_receive_code,' ')      ,EDW1.last_update_date_time      ,COALESCE(EDW1.last_update_login_name,' ')      ,COALESCE(EDW1.last_updated_by_name,' ')      ,COALESCE(EDW1.line_created_by_name,' ')      ,EDW1.line_creation_date_time      ,COALESCE(EDW1.line_curr_code,' ')      ,COALESCE(EDW1.line_display_sequence_nbr,0)      ,EDW1.line_end_date      ,COALESCE(EDW1.line_exception_flag,' ')      ,EDW1.line_last_update_date_time      ,COALESCE(EDW1.line_last_update_login_name,' ')      ,COALESCE(EDW1.line_last_updated_by_name,' ')      ,COALESCE(EDW1.line_ngt_renew_price_amt_ent,0)      ,COALESCE(EDW1.line_orig_sys_id,' ')      ,COALESCE(EDW1.line_orig_sys_source_code,' ')      ,COALESCE(EDW1.line_price_level_flag,' ')      ,COALESCE(EDW1.line_renew_curr_code,' ')      ,EDW1.line_renew_date      ,EDW1.line_start_date      ,COALESCE(EDW1.line_status_code,' ')      ,COALESCE(EDW1.line_term_reason_code,' ')      ,COALESCE(EDW1.line_termination_code,' ')      ,EDW1.line_termination_date      ,COALESCE(EDW1.line_tran_code,' ')      ,COALESCE(EDW1.line_upg_orig_sys_ref_name,' ')      ,COALESCE(EDW1.line_fml_org_code,' ')      ,COALESCE(EDW1.line_wrnty_exp_reason_code,' ')      ,COALESCE(EDW1.migration_contract_nbr,' ')      ,COALESCE(EDW1.orig_sys_id,' ')      ,COALESCE(EDW1.orig_sys_source_code,' ')      ,COALESCE(EDW1.prepay_rqrd_flag,' ')      ,COALESCE(EDW1.qa_check_list_id,' ')      ,COALESCE(EDW1.region_code,' ')      ,COALESCE(EDW1.region_desc,' ')      ,COALESCE(EDW1.renew_curr_code,' ')      ,EDW1.renew_date      ,EDW1.signed_date      ,COALESCE(EDW1.sline_created_by_name ,' ')      ,EDW1.sline_creation_date_time      ,COALESCE(EDW1.sline_curr_code,' ')      ,COALESCE(EDW1.sline_display_seq_nbr,0)      ,EDW1.sline_end_date      ,COALESCE(EDW1.sline_exception_flag,' ')      ,EDW1.sline_last_update_date_time      ,COALESCE(EDW1.sline_last_update_login_name,' ')      ,COALESCE(EDW1.sline_last_updated_by_name,' ')      ,COALESCE(EDW1.sline_ngt_price_amt_ent,0)      ,COALESCE(EDW1.sline_ngt_renew_price_amt_ent,0)      ,COALESCE(EDW1.sline_orig_sys_id,' ')      ,COALESCE(EDW1.sline_orig_sys_source_code,' ')      ,COALESCE(EDW1.sline_parent_contract_line_id,' ')      ,COALESCE(EDW1.sline_price_level_flag,' ')      ,COALESCE(EDW1.sline_renew_curr_code,' ')      ,EDW1.sline_renew_date      ,COALESCE(EDW1.sline_sot_order_nbr,' ')      ,COALESCE(EDW1.sline_sot_order_sot_line_nbr,' ')      ,COALESCE(EDW1.sline_sot_line_nbr,' ')      ,EDW1.sline_start_date      ,COALESCE(EDW1.sline_status_code,' ')      ,COALESCE(EDW1.sline_term_reason_code,' ')      ,COALESCE(EDW1.sline_termination_code,' ')      ,EDW1.sline_termination_date      ,COALESCE(EDW1.sline_tran_code,' ')      ,COALESCE(EDW1.sline_unit_price_amt_ent,0)      ,COALESCE(EDW1.sline_upg_orig_sys_ref_id,' ')      ,COALESCE(EDW1.sline_upg_orig_sys_ref_name,' ')      ,COALESCE(EDW1.sline_wrnty_exp_reason_code,' ')      ,COALESCE(EDW1.solution_portfolio_id,' ')      ,EDW1.start_date      ,COALESCE(EDW1.status_code,' ')      ,COALESCE(EDW1.subclass_code,' ')      ,COALESCE(EDW1.template_flag,' ')      ,COALESCE(EDW1.template_used_name,' ')      ,COALESCE(EDW1.termination_code,' ')      ,EDW1.termination_date      ,COALESCE(EDW1.termination_reason_code,' ')      ,COALESCE(EDW1.tran_code,' ')      ,CURRENT_TIMESTAMP(0)      ,COALESCE(EDW1.bill_to_site_use_id,0)      ,COALESCE(EDW1.curr_cnvrsn_rate,0)      ,EDW1.curr_cnvrsn_rate_date      ,COALESCE(EDW1.curr_cnvrsn_type_code,' ')      ,COALESCE(EDW1.document_id,0)      ,COALESCE(EDW1.invoice_rule_id,0)      ,COALESCE(EDW1.invoice_term_id,0)      ,COALESCE(EDW1.line_bill_to_site_use_id,0)      ,COALESCE(EDW1.line_cust_acct_id,0)      ,COALESCE(EDW1.line_invoice_rule_id,0)      ,COALESCE(EDW1.line_renewal_type_code,' ')      ,COALESCE(EDW1.line_ship_to_site_use_id,0)      ,COALESCE(EDW1.price_list_header_id,0)      ,EDW1.renewal_end_date      ,COALESCE(EDW1.renewal_type_code,' ')      ,COALESCE(EDW1.ship_to_site_use_id,0)      ,COALESCE(EDW1.sline_invoice_rule_id,0)      ,COALESCE(EDW1.sline_list_price_amt_ent,0)      ,COALESCE(EDW1.sline_renewal_type_code,' ')      ,COALESCE(EDW1.sline_serviced_qty,0)      ,COALESCE(EDW2.sub_region_code,' ')      ,COALESCE(EDW2.sub_region_name,' ')      ,COALESCE(EDW1.contract_subtype_code,' ')      ,EDW1.new_for_new_duration  <WM_COMMENT_START>Added for HSR ENTITLEMENT CORE project<WM_COMMENT_END>      ,EDW1.reverse_logistics_text      ,EDW1.routing_comment_text      ,EDW1.source_location_name      ,EDW1.renewal_approval_type      ,EDW1.crb_invoice_nbr_text      ,EDW1.crb_reference_nbr_text      ,EDW1.suspend_credits_flagFROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t6  EDW1LEFT OUTER JOIN     ${EEDDWWDDBB}.geo_rollup_xref EDW2ON   EDW1.author_oper_unit_country_code = EDW2.country_codeAND  EDW2.business_unit_code = 'B4'WHERE sline_sot_order_nbr<>' 'AND  (     contract_subline_id    ,instance_id  )NOT IN  (     SELECT         contract_subline_id        ,instance_id     FROM ${EEDDWWDDBB}.contract_dn_ld  )
--Transformed Query: MANUAL

    WITH qq1 AS (    SELECT
        COALESCE( EDW1.contract_nbr ,
        ' ' ) AS auto_c019,
        COALESCE( EDW1.contract_nbr_modifier ,
        ' ' ) AS auto_c020,
        COALESCE( EDW1.contract_line_nbr ,
        ' ' ) AS auto_c017,
        COALESCE( EDW1.contract_subline_nbr ,
        ' ' ) AS auto_c021,
        EDW1.instance_id,
        COALESCE( EDW1.agreement_record_id ,
        ' ' ) AS auto_c02,
        EDW1.approved_date_time,
        COALESCE( EDW1.archived_flag ,
        ' ' ) AS auto_c04,
        COALESCE( EDW1.area_code ,
        ' ' ) AS auto_c05,
        COALESCE( EDW1.area_desc ,
        ' ' ) AS auto_c06,
        COALESCE( EDW1.author_ncr_unit_id ,
        ' ' ) AS auto_c07,
        COALESCE( EDW1.author_oper_unit_country_code ,
        ' ' ) AS auto_c08,
        COALESCE( EDW1.author_operating_unit_id ,
        0 ) AS auto_c09,
        COALESCE( EDW1.auto_renew_day_cnt ,
        0 ) AS auto_c010,
        EDW1.cancelled_date_time,
        COALESCE( EDW1.character_type_code ,
        ' ' ) AS auto_c012,
        COALESCE( EDW1.contract_est_amt_ent ,
        0 ) AS auto_c013,
        COALESCE( EDW1.contract_header_id ,
        ' ' ) AS auto_c014,
        COALESCE( EDW1.contract_intent_code ,
        ' ' ) AS auto_c015,
        COALESCE( EDW1.contract_line_id ,
        ' ' ) AS auto_c016,
        COALESCE( EDW1.contract_line_style_id ,
        0 ) AS auto_c018,
        EDW1.contract_subline_id,
        COALESCE( EDW1.contract_subline_style_id ,
        0 ) AS auto_c022,
        COALESCE( EDW1.country_desc ,
        ' ' ) AS auto_c023,
        COALESCE( EDW1.created_by_name ,
        ' ' ) AS auto_c024,
        EDW1.creation_date_time,
        COALESCE( EDW1.curr_code ,
        ' ' ) AS auto_c026,
        COALESCE( EDW1.cust_po_nbr ,
        ' ' ) AS auto_c027,
        COALESCE( EDW1.cust_po_nbr_rqrd_flag ,
        ' ' ) AS auto_c028,
        COALESCE( EDW1.deleted_flag ,
        ' ' ) AS auto_c029,
        EDW1.end_date,
        EDW1.entitlement_begin_date,
        CASE 
            WHEN EDW1.entitlement_term_date  IS NULL  THEN EDW1.entitlement_end_date  
            WHEN EDW1.entitlement_term_date < EDW1.entitlement_end_date  THEN EDW1.entitlement_term_date 
            ELSE EDW1.entitlement_end_date  
        END AS auto_c032,
        EDW1.entitlement_end_date,
        EDW1.entitlement_term_date,
        CASE 
            WHEN EDW1.tran_code = 'D'  
            OR EDW1.line_tran_code = 'D'   
            OR EDW1.sline_tran_code = 'D'   
            OR EDW1.tran_code  IS NULL   
            OR EDW1.line_tran_code  IS NULL   
            OR EDW1.sline_tran_code  IS NULL   THEN 'D'  
            ELSE 'U'  
        END AS auto_c035,
        COALESCE( EDW1.est_renew_amt_ent ,
        0 ) AS auto_c036,
        COALESCE( EDW1.final_price_modifier_text ,
        ' ' ) AS auto_c037,
        COALESCE( EDW1.gsa_code ,
        ' ' ) AS auto_c038,
        COALESCE( EDW1.inventory_org_id ,
        0 ) AS auto_c039,
        COALESCE( EDW1.invoice_zero_amt_flag ,
        ' ' ) AS auto_c040,
        COALESCE( EDW1.issue_receive_code ,
        ' ' ) AS auto_c041,
        EDW1.last_update_date_time,
        COALESCE( EDW1.last_update_login_name ,
        ' ' ) AS auto_c043,
        COALESCE( EDW1.last_updated_by_name ,
        ' ' ) AS auto_c044,
        COALESCE( EDW1.line_created_by_name ,
        ' ' ) AS auto_c045,
        EDW1.line_creation_date_time,
        COALESCE( EDW1.line_curr_code ,
        ' ' ) AS auto_c047,
        COALESCE( EDW1.line_display_sequence_nbr ,
        0 ) AS auto_c048,
        EDW1.line_end_date,
        COALESCE( EDW1.line_exception_flag ,
        ' ' ) AS auto_c050,
        EDW1.line_last_update_date_time,
        COALESCE( EDW1.line_last_update_login_name ,
        ' ' ) AS auto_c052,
        COALESCE( EDW1.line_last_updated_by_name ,
        ' ' ) AS auto_c053,
        COALESCE( EDW1.line_ngt_renew_price_amt_ent ,
        0 ) AS auto_c054,
        COALESCE( EDW1.line_orig_sys_id ,
        ' ' ) AS auto_c055,
        COALESCE( EDW1.line_orig_sys_source_code ,
        ' ' ) AS auto_c056,
        COALESCE( EDW1.line_price_level_flag ,
        ' ' ) AS auto_c057,
        COALESCE( EDW1.line_renew_curr_code ,
        ' ' ) AS auto_c058,
        EDW1.line_renew_date,
        EDW1.line_start_date,
        COALESCE( EDW1.line_status_code ,
        ' ' ) AS auto_c061,
        COALESCE( EDW1.line_term_reason_code ,
        ' ' ) AS auto_c062,
        COALESCE( EDW1.line_termination_code ,
        ' ' ) AS auto_c063,
        EDW1.line_termination_date,
        COALESCE( EDW1.line_tran_code ,
        ' ' ) AS auto_c065,
        COALESCE( EDW1.line_upg_orig_sys_ref_name ,
        ' ' ) AS auto_c066,
        COALESCE( EDW1.line_wrnty_exp_reason_code ,
        ' ' ) AS auto_c068,
        COALESCE( EDW1.line_fml_org_code ,
        ' ' ) AS auto_c067,
        COALESCE( EDW1.migration_contract_nbr ,
        ' ' ) AS auto_c069,
        COALESCE( EDW1.orig_sys_id ,
        ' ' ) AS auto_c070,
        COALESCE( EDW1.orig_sys_source_code ,
        ' ' ) AS auto_c071,
        COALESCE( EDW1.prepay_rqrd_flag ,
        ' ' ) AS auto_c072,
        COALESCE( EDW1.qa_check_list_id ,
        ' ' ) AS auto_c073,
        COALESCE( EDW1.region_code ,
        ' ' ) AS auto_c074,
        COALESCE( EDW1.region_desc ,
        ' ' ) AS auto_c075,
        COALESCE( EDW1.renew_curr_code ,
        ' ' ) AS auto_c076,
        EDW1.renew_date,
        EDW1.signed_date,
        COALESCE( EDW1.sline_created_by_name ,
        ' ' ) AS auto_c079,
        EDW1.sline_creation_date_time,
        COALESCE( EDW1.sline_curr_code ,
        ' ' ) AS auto_c081,
        COALESCE( EDW1.sline_display_seq_nbr ,
        0 ) AS auto_c082,
        EDW1.sline_end_date,
        COALESCE( EDW1.sline_exception_flag ,
        ' ' ) AS auto_c084,
        EDW1.sline_last_update_date_time,
        COALESCE( EDW1.sline_last_update_login_name ,
        ' ' ) AS auto_c086,
        COALESCE( EDW1.sline_last_updated_by_name ,
        ' ' ) AS auto_c087,
        COALESCE( EDW1.sline_ngt_price_amt_ent ,
        0 ) AS auto_c088,
        COALESCE( EDW1.sline_ngt_renew_price_amt_ent ,
        0 ) AS auto_c089,
        COALESCE( EDW1.sline_orig_sys_id ,
        ' ' ) AS auto_c090,
        COALESCE( EDW1.sline_orig_sys_source_code ,
        ' ' ) AS auto_c091,
        COALESCE( EDW1.sline_parent_contract_line_id ,
        ' ' ) AS auto_c092,
        COALESCE( EDW1.sline_price_level_flag ,
        ' ' ) AS auto_c093,
        COALESCE( EDW1.sline_renew_curr_code ,
        ' ' ) AS auto_c094,
        EDW1.sline_renew_date,
        COALESCE( EDW1.sline_sot_order_nbr ,
        ' ' ) AS auto_c096,
        COALESCE( EDW1.sline_sot_order_sot_line_nbr ,
        ' ' ) AS auto_c097,
        COALESCE( EDW1.sline_sot_line_nbr ,
        ' ' ) AS auto_c098,
        EDW1.sline_start_date,
        COALESCE( EDW1.sline_status_code ,
        ' ' ) AS auto_c0100,
        COALESCE( EDW1.sline_term_reason_code ,
        ' ' ) AS auto_c0101,
        COALESCE( EDW1.sline_termination_code ,
        ' ' ) AS auto_c0102,
        EDW1.sline_termination_date,
        COALESCE( EDW1.sline_tran_code ,
        ' ' ) AS auto_c0104,
        COALESCE( EDW1.sline_unit_price_amt_ent ,
        0 ) AS auto_c0105,
        COALESCE( EDW1.sline_upg_orig_sys_ref_id ,
        ' ' ) AS auto_c0106,
        COALESCE( EDW1.sline_upg_orig_sys_ref_name ,
        ' ' ) AS auto_c0107,
        COALESCE( EDW1.sline_wrnty_exp_reason_code ,
        ' ' ) AS auto_c0108,
        COALESCE( EDW1.solution_portfolio_id ,
        ' ' ) AS auto_c0109,
        EDW1.start_date,
        COALESCE( EDW1.status_code ,
        ' ' ) AS auto_c0111,
        COALESCE( EDW1.subclass_code ,
        ' ' ) AS auto_c0112,
        COALESCE( EDW1.template_flag ,
        ' ' ) AS auto_c0113,
        COALESCE( EDW1.template_used_name ,
        ' ' ) AS auto_c0114,
        COALESCE( EDW1.termination_code ,
        ' ' ) AS auto_c0115,
        EDW1.termination_date,
        COALESCE( EDW1.termination_reason_code ,
        ' ' ) AS auto_c0117,
        COALESCE( EDW1.tran_code ,
        ' ' ) AS auto_c0118,
        CURRENT_TIMESTAMP() AS auto_c0119,
        COALESCE( EDW1.bill_to_site_use_id ,
        0 ) AS auto_c0120,
        COALESCE( EDW1.curr_cnvrsn_rate ,
        0 ) AS auto_c0121,
        EDW1.curr_cnvrsn_rate_date,
        COALESCE( EDW1.curr_cnvrsn_type_code ,
        ' ' ) AS auto_c0123,
        COALESCE( EDW1.document_id ,
        0 ) AS auto_c0124,
        COALESCE( EDW1.invoice_rule_id ,
        0 ) AS auto_c0125,
        COALESCE( EDW1.invoice_term_id ,
        0 ) AS auto_c0126,
        COALESCE( EDW1.line_bill_to_site_use_id ,
        0 ) AS auto_c0127,
        COALESCE( EDW1.line_cust_acct_id ,
        0 ) AS auto_c0128,
        COALESCE( EDW1.line_invoice_rule_id ,
        0 ) AS auto_c0129,
        COALESCE( EDW1.line_renewal_type_code ,
        ' ' ) AS auto_c0130,
        COALESCE( EDW1.line_ship_to_site_use_id ,
        0 ) AS auto_c0131,
        COALESCE( EDW1.price_list_header_id ,
        0 ) AS auto_c0132,
        EDW1.renewal_end_date,
        COALESCE( EDW1.renewal_type_code ,
        ' ' ) AS auto_c0134,
        COALESCE( EDW1.ship_to_site_use_id ,
        0 ) AS auto_c0135,
        COALESCE( EDW1.sline_invoice_rule_id ,
        0 ) AS auto_c0136,
        COALESCE( EDW1.sline_list_price_amt_ent ,
        0 ) AS auto_c0137,
        COALESCE( EDW1.sline_renewal_type_code ,
        ' ' ) AS auto_c0138,
        COALESCE( EDW1.sline_serviced_qty ,
        0 ) AS auto_c0139,
        COALESCE( EDW2.sub_region_code ,
        ' ' ) AS auto_c0140,
        COALESCE( EDW2.sub_region_name ,
        ' ' ) AS auto_c0141,
        COALESCE( EDW1.contract_subtype_code ,
        ' ' ) AS auto_c0142,
        EDW1.new_for_new_duration,
        EDW1.routing_comment_text,
        EDW1.reverse_logistics_text,
        EDW1.source_location_name,
        EDW1.renewal_approval_type,
        EDW1.crb_invoice_nbr_text,
        EDW1.crb_reference_nbr_text,
        EDW1.suspend_credits_flag,
        EDW1.hold_reason_description,
        EDW1.agreement_start_date,
        EDW1.agreement_end_date  ,
        EDW1.mdm_product_identifier,
        EDW1.mdm_solution_identifier
    FROM
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t6    AS EDW1   
    LEFT OUTER  JOIN
        ${EEDDWWDDBB}.geo_rollup_xref    AS EDW2    
            ON EDW1.author_oper_unit_country_code = EDW2.country_code  
            AND upper(trim(EDW2.business_unit_code)) = upper(trim('B4')) 
    LEFT OUTER JOIN
        (SELECT
             contract_subline_id AS auto_c00,
            instance_id AS auto_c01  
        FROM
            ${EEDDWWDDBB}.contract_dn_ld group by contract_subline_id,instance_id ) AS autoAlias_23 
            ON ( upper(trim(contract_subline_id)) = upper(trim(autoAlias_23.AUTO_C00)) 
            AND upper(trim(instance_id)) = upper(trim(autoAlias_23.AUTO_C01)) ) 
    WHERE
        upper(trim(sline_sot_order_nbr)) <> upper(trim(' '))  
        AND autoAlias_23.AUTO_C00 IS  NULL  
        AND autoAlias_23.AUTO_C01 IS  NULL          ) INSERT 
        INTO
            TABLE
            ${EEDDWWDDBB}.contract_dn_ld       SELECT
                * 
            FROM
                qq1;



--handle SET table

INSERT OVERWRITE TABLE ${EEDDWWDDBB}.contract_dn_ld select distinct * from ${EEDDWWDDBB}.contract_dn_ld;

--Original Query:
  --CREATE  INDEX  (     sline_sot_order_nbr  )ON ${EEDDWWDDBB}.contract_dn_ld

--Translated Query: MANUAL
--      CREATE INDEX contract_dn_ld_sline_sot_order_nbr_idx ON TABLE contract_dn_ld (sline_sot_order_nbr) AS 'COMPACT' WITH DEFERRED REBUILD

--    ALTER INDEX contract_dn_ld_sline_sot_order_nbr_idx         ON contract_dn_ld REBUILD;


--Original Query:
  --CREATE  INDEX  (     contract_nbr  )ON ${EEDDWWDDBB}.contract_dn_ld

--Translated Query: MANUAL
--      CREATE INDEX contract_dn_ld_contract_nbr_idx ON TABLE contract_dn_ld (contract_nbr) AS 'COMPACT' WITH DEFERRED REBUILD

--    ALTER INDEX contract_dn_ld_contract_nbr_idx         ON contract_dn_ld REBUILD;


--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_dn_ldCOLUMN sline_sot_order_nbr

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_dn_ld COMPUTE STATISTICS  FOR COLUMNS sline_sot_order_nbr;


--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_dn_ldCOLUMN contract_nbr

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_dn_ld COMPUTE STATISTICS  FOR COLUMNS contract_nbr;


--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_dn_ld COLUMN SLINE_TRAN_CODE

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_dn_ld COMPUTE STATISTICS  FOR COLUMNS SLINE_TRAN_CODE;


--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_dn_ld COLUMN LINE_TRAN_CODE

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_dn_ld COMPUTE STATISTICS  FOR COLUMNS LINE_TRAN_CODE;


--Original Query:
  --DELETE FROM  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t1 ALL

--Translated Query: SUCCESS

--    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t1;


--Original Query:
  --DELETE FROM  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t2 ALL

--Translated Query: SUCCESS

--    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t2;


--Original Query:
  --DELETE FROM  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t3 ALL

--Translated Query: SUCCESS

--    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t3;


--Original Query:
  --DELETE FROM  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t4 ALL

--Translated Query: SUCCESS

--    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t4;


--Original Query:
  --DELETE FROM  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t5 ALL

--Translated Query: SUCCESS

--    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t5;


--Original Query:
  --DELETE FROM  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t6 ALL

--Translated Query: SUCCESS

--    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_contract_dn_t6;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn_ARCS.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${EEDDWWDDBB}.contract_dn_ARCS CDAWHERE  CDA.contract_subline_id = ${EEDDWWDDBB}.contract_dn_ld.contract_subline_idAND    CDA.instance_id         = ${EEDDWWDDBB}.contract_dn_ld.instance_idAND    CDA.contract_nbr        = ${EEDDWWDDBB}.contract_dn_ld.contract_nbrAND    CDA.contract_nbr_modifier = ${EEDDWWDDBB}.contract_dn_ld.contract_nbr_modifierAND    CDA.contract_line_nbr     = ${EEDDWWDDBB}.contract_dn_ld.contract_line_nbrAND    CDA.contract_subline_nbr  = ${EEDDWWDDBB}.contract_dn_ld.contract_subline_nbr

--Translated Query: MANUAL
--INSERT OVERWRITE TABLE ${EEDDWWDDBB}.contract_dn_ARCS SELECT CDA.* FROM ${EEDDWWDDBB}.contract_dn_ARCS AS CDA LEFT JOIN ${EEDDWWDDBB}.contract_dn_ld AS CL ON (trim(CDA.contract_subline_id)) = upper(trim(CL.contract_subline_id)) AND upper(trim(CDA.instance_id)) = upper(trim(CL.instance_id)) AND upper(trim(CDA.contract_nbr)) = upper(trim(CL.contract_nbr)) AND upper(trim(CDA.contract_nbr_modifier)) = upper(trim(CL.contract_nbr_modifier)) AND upper(trim(CDA.contract_line_nbr)) = upper(trim(CL.contract_line_nbr)) AND upper(trim(CDA.contract_subline_nbr)) = upper(trim(CL.contract_subline_nbr)) WHERE  CL.contract_subline_id IS NULL OR CL.instance_id IS NULL OR CL.contract_nbr IS NULL OR CL.contract_nbr_modifier IS NULL OR CL.contract_line_nbr IS NULL OR CL.contract_subline_nbr IS NULL
-- Transformed and Tuned (Mohit, Comments: Replaced the Non-Equi joins with Left join and removed OR clauses with WHERE clauses)
--------------------------------

    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB}.contract_dn_ARCS SELECT
            CDA.* 
        FROM
            ${EEDDWWDDBB}.contract_dn_ARCS CDA 
        LEFT OUTER JOIN
            ${EEDDWWDDBB}.contract_dn_ld DN 
                ON    CDA.contract_subline_id = DN.contract_subline_id 
                AND  CDA.instance_id = DN.instance_id 
                AND    CDA.contract_nbr        = DN.contract_nbr 
                AND    CDA.contract_nbr_modifier = DN.contract_nbr_modifier 
                AND CDA.contract_line_nbr = DN.contract_line_nbr 
                AND    CDA.contract_subline_nbr  = DN.contract_subline_nbr 
        WHERE
            DN.contract_subline_id IS NULL 
            AND DN.instance_id IS NULL 
            AND DN.contract_nbr IS NULL 
            AND DN.contract_nbr_modifier IS NULL 
            AND DN.contract_line_nbr IS NULL 
            AND DN.contract_subline_nbr IS NULL;


--Original Query:
  --DROP INDEX  (     sline_sot_order_nbr  )ON ${EEDDWWDDBB}.contract_dn_ARCS

--Translated Query: MANUAL
--      DROP INDEX IF EXISTS sline_sot_order_nbr ON ${EEDDWWDDBB}.contract_dn_ARCS


--Original Query:
  --DROP INDEX   (     contract_nbr  ) ON ${EEDDWWDDBB}.contract_dn_ARCS

--Translated Query: MANUAL
--      DROP INDEX IF EXISTS contract_nbr ON ${EEDDWWDDBB}.contract_dn_ARCS


--Original Query:
  --LOCKING TABLE ${EEDDWWDDBB}.contract_dn_ld FOR ACCESSINSERT INTO ${EEDDWWDDBB}.contract_dn_ARCS  (       contract_nbr,       contract_nbr_modifier,       contract_line_nbr,       contract_subline_nbr,       instance_id,       contract_header_id,       contract_line_id,       contract_subline_id,        sline_sot_order_nbr  )SELECT        contract_nbr,       contract_nbr_modifier,       contract_line_nbr,       contract_subline_nbr,       instance_id,       contract_header_id,       contract_line_id,       contract_subline_id,        sline_sot_order_nbr FROM ${EEDDWWDDBB}.contract_dn_ld WHERE (contract_nbr ,contract_nbr_modifier ,contract_line_nbr ,contract_subline_nbr ,instance_id ) IN(SELECT contract_nbr ,contract_nbr_modifier ,contract_line_nbr ,MAX(contract_subline_nbr) ,instance_id  FROM   ${EEDDWWDDBB}.contract_dn_ldGROUP BY 1,2,3,5)

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB}.contract_dn_ARCS           SELECT
            contract_nbr,
            contract_nbr_modifier,
            contract_line_nbr,
            contract_subline_nbr,
            instance_id,
            contract_header_id,
            contract_line_id,
            contract_subline_id,
            sline_sot_order_nbr  
        FROM
            ${EEDDWWDDBB}.contract_dn_ld 
        INNER JOIN
            (
                SELECT
                    contract_nbr AS auto_c00,
                    contract_nbr_modifier AS auto_c01,
                    contract_line_nbr AS auto_c02,
                    MAX( contract_subline_nbr ) AS auto_c03,
                    instance_id AS auto_c04  
                FROM
                    ${EEDDWWDDBB}.contract_dn_ld      
                GROUP BY
                    contract_nbr ,
                    contract_nbr_modifier ,
                    contract_line_nbr ,
                    instance_id 
            ) AS autoAlias_21 
                ON (
                    upper(trim(contract_nbr)) = upper(trim(autoAlias_21.AUTO_C00)) 
                    AND upper(trim(contract_nbr_modifier)) = upper(trim(autoAlias_21.AUTO_C01)) 
                    AND upper(trim(contract_line_nbr)) = upper(trim(autoAlias_21.AUTO_C02)) 
                    AND upper(trim(contract_subline_nbr)) = upper(trim(autoAlias_21.AUTO_C03)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_21.AUTO_C04)) 
                );


--Original Query:
  --CREATE  INDEX   (     sline_sot_order_nbr   )ON ${EEDDWWDDBB}.contract_dn_ARCS

--Translated Query: MANUAL
--      CREATE INDEX contract_dn_arcs_sline_sot_order_nbr_idx ON TABLE contract_dn_ARCS (sline_sot_order_nbr) AS 'COMPACT' WITH DEFERRED REBUILD

--    ALTER INDEX contract_dn_arcs_sline_sot_order_nbr_idx        ON contract_dn_ARCS REBUILD;


--Original Query:
  --CREATE  INDEX  (      contract_nbr  ) ON ${EEDDWWDDBB}.contract_dn_ARCS

--Translated Query: MANUAL
--      CREATE INDEX contract_dn_arcs_contract_nbr_idx ON TABLE contract_dn_ARCS (contract_nbr) AS 'COMPACT' WITH DEFERRED REBUILD

--    ALTER INDEX contract_dn_arcs_contract_nbr_idx    ON contract_dn_ARCS REBUILD;


--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_dn_ARCS COLUMN sline_sot_order_nbr

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_dn_ARCS COMPUTE STATISTICS  FOR COLUMNS sline_sot_order_nbr;


--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_dn_ARCSCOLUMN contract_nbr

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_dn_ARCS COMPUTE STATISTICS  FOR COLUMNS contract_nbr;


--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_dn_ARCS COLUMN instance_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_dn_ARCS COMPUTE STATISTICS  FOR COLUMNS instance_id;


--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_dn_ARCS COLUMNcontract_header_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_dn_ARCS COMPUTE STATISTICS  FOR COLUMNS contract_header_id;


--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_dn_ARCS COLUMNcontract_line_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_dn_ARCS COMPUTE STATISTICS  FOR COLUMNS contract_line_id;


--Original Query:
  --COLLECT STATISTICS ON ${EEDDWWDDBB}.contract_dn_ARCSINDEX  (    contract_nbr   ,contract_nbr_modifier   ,contract_line_nbr   ,contract_subline_nbr   ,instance_id  )

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB}.contract_dn_ARCS COMPUTE STATISTICS  FOR COLUMNS contract_nbr,contract_nbr_modifier,contract_line_nbr,contract_subline_nbr,instance_id;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_header_purge_t1.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${TTMMPPDDBB1}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB1};


--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_header_purge_t1 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1;


--Original Query:
  --INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_header_purge_t1(  contract_header_id       ,instance_id)SELECTcontract_header_id,instance_idFROM ${EEDDWWDDBB1}.contract_header_ld WHERE tran_code = 'D' ANDCAST(end_date_time as DATE) < (DATE - PPDDAATTEE)AND CAST(last_update_date_time AS DATE) < (DATE - PPDDAATTEE)

--Translated Query: MANUAL

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_purge_t1           SELECT
            contract_header_id,
            instance_id  
        FROM
            ${EEDDWWDDBB1}.contract_header_ld     
        WHERE
            upper(trim(tran_code)) = upper(trim('D'))  
            AND CAST (end_date_time AS date) < (
                CAST (DATE_SUB( CURRENT_DATE() , CAST (${PPDDAATTEE} as int) )  AS DATE)
            )   
            AND CAST (last_update_date_time AS date) < (
                CAST (DATE_SUB( CURRENT_DATE() , CAST (${PPDDAATTEE} as int) )  AS DATE)
            );

--Translated Query: SUCCESS
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_line select * from ${EEDDWWDDBB1}.contract_line_ld;
--added by Nitin
--Translated Query: SUCCESS
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_header select distinct * from ${EEDDWWDDBB1}.contract_header_ld;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_dn.ren.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${EEDDWWDDBB}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB};


--Original Query:
  --RENAME TABLE contract_dn_ld TO contract_dn

--Translated Query: MANUAL

--    ALTER TABLE ${EEDDWWDDBB}.contract_dn RENAME TO ${EEDDWWDDBB}.contract_dn_hd;

--  ALTER TABLE ${EEDDWWDDBB}.contract_dn_ld RENAME TO ${EEDDWWDDBB}.contract_dn;

--  ALTER TABLE ${EEDDWWDDBB}.contract_dn_hd RENAME TO ${EEDDWWDDBB}.contract_dn_ld;

    INSERT OVERWRITE TABLE ${EEDDWWDDBB}.contract_dn SELECT * from ${EEDDWWDDBB}.contract_dn_ld;

