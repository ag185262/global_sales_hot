----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_revn_dist_01.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1(    instance_id,    revenue_dist_id,    account_class_code, as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_code)SELECT    instance_id,    revenue_dist_id,    account_class_code, as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_codeFROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_mlWHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,    instance_id,    revenue_dist_id )IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),     instance_id,    revenue_dist_id    FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml    WHERE tran_code <> 'D'    GROUP BY 2,3 )AND tran_code <> 'D'

--Translated Query: SUCCESS
--      INSERT INTO TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 SELECT instance_id ,revenue_dist_id ,account_class_code ,as_of_date_time ,batch_nbr ,contract_header_id ,contract_line_id ,created_by_name ,creation_date_time ,distribution_pct ,gl_code_combination_id ,last_update_date_time ,last_update_login_name ,last_updated_by_name ,tran_code FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml INNER JOIN ( SELECT MAX(CONCAT(CAST(as_of_date_time AS CHAR(26)), batch_nbr)) AS auto_c00 ,instance_id AS auto_c01 ,revenue_dist_id AS auto_c02 FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml WHERE upper(trim(tran_code)) <> upper(trim('D')) GROUP BY instance_id ,revenue_dist_id ) AS autoAlias_45 ON ( CONCAT(CAST(as_of_date_time AS CHAR(26)), batch_nbr) = autoAlias_45.AUTO_C00 AND upper(trim(instance_id)) = upper(trim(autoAlias_45.AUTO_C01)) AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_45.AUTO_C02)) ) WHERE upper(trim(tran_code)) <> upper(trim('D'))
--Chngd MOHIT_NATASHA (removed self join and replaced with ROW NUMBER)

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1           SELECT distinct
            -- instance_id,
            -- revenue_dist_id,
            -- account_class_code,
            -- as_of_date_time,
            -- batch_nbr,
            -- contract_header_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- distribution_pct,
            -- gl_code_combination_id,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- tran_code  
        -- FROM
            -- ( SELECT
                -- m1.*,
                -- ROW_NUMBER() OVER(PARTITION 
            -- BY
                -- instance_id ,
                -- revenue_dist_id 
            -- ORDER BY
                -- CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) DESC) AS r00 
            -- FROM
                -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml m1 
            -- WHERE
                -- upper(trim(tran_code)) <> trim('D')) rcrd 
        -- WHERE
            -- r00=1;
INSERT 
            INTO
                TABLE
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1           SELECT 
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    ( SELECT
                        m1.*,
                        ROW_NUMBER() OVER(PARTITION 
                    BY
                        instance_id ,
                        revenue_dist_id 
                    ORDER BY
                        CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                        batch_nbr) DESC) AS r00 
                    FROM
                        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml m1 
                    WHERE
                        upper(trim(tran_code)) <> trim('D')) rcrd 
                WHERE
                    r00=1;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_revn_dist_02.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 FOR ACCESSLOCK TABLE  ${EEDDWWDDBB1}.contract_revn_dist_ld FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2( instance_id,    revenue_dist_id,    account_class_code, alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, current_record_ind, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_code)SELECT    instance_id,    revenue_dist_id,    account_class_code, alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, 'Y',    distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_codeFROM ${EEDDWWDDBB1}.contract_revn_dist_ldWHERE (instance_id,revenue_dist_id)IN (SELECT     instance_id,    revenue_dist_id    FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1    )

--Translated Query: SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2           SELECT distinct
            -- instance_id,
            -- revenue_dist_id,
            -- account_class_code,
            -- alt_as_of_date_time,
            -- alt_batch_nbr,
            -- as_of_date_time,
            -- batch_nbr,
            -- contract_header_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- 'Y',
            -- distribution_pct,
            -- gl_code_combination_id,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- tran_code  
        -- FROM
            -- ${EEDDWWDDBB1}.contract_revn_dist_ld 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- DISTINCT instance_id AS auto_c00,
                    -- revenue_dist_id AS auto_c01  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 
            -- ) AS autoAlias_47 
                -- ON (
                    -- upper(trim(instance_id)) = upper(trim(autoAlias_47.AUTO_C00)) 
                    -- AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_47.AUTO_C01)) 
                -- );
INSERT 
            INTO
                TABLE
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2           SELECT distinct
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    'Y',
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    ${EEDDWWDDBB1}.contract_revn_dist_ld 
                INNER JOIN
                    (
                        SELECT
                            instance_id AS auto_c00,
                            revenue_dist_id AS auto_c01  
                        FROM
                            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 
                    ) AS autoAlias_47 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_47.AUTO_C00)) 
                            AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_47.AUTO_C01)) 
                        );

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t3 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t3;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 FOR ACCESSLOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t3(   instance_id,    revenue_dist_id,    account_class_code, contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_code)SELECT    instance_id,    revenue_dist_id,    account_class_code, contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_codeFROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2WHERE (instance_id,revenue_dist_id)IN (SELECT     instance_id,    revenue_dist_id    FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1    )

--Translated Query: SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t3           SELECT distinct
            -- instance_id,
            -- revenue_dist_id,
            -- account_class_code,
            -- contract_header_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- distribution_pct,
            -- gl_code_combination_id,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- tran_code  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- DISTINCT instance_id AS auto_c00,
                    -- revenue_dist_id AS auto_c01  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 
            -- ) AS autoAlias_49 
                -- ON (
                    -- upper(trim(instance_id)) = upper(trim(autoAlias_49.AUTO_C00)) 
                    -- AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_49.AUTO_C01)) 
                -- );
INSERT 
            INTO
                TABLE
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t3           SELECT 
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 
                INNER JOIN
                    (
                        SELECT
                             instance_id AS auto_c00,
                            revenue_dist_id AS auto_c01  
                        FROM
                            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 
                    ) AS autoAlias_49 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_49.AUTO_C00)) 
                            AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_49.AUTO_C01)) 
                        );
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2020/01/04 22:48:54 
--Script Name: d8_contract_revn_dist_03.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query: 7e58aa2f355917bd712f410b9b1585c8
  --DATABASE ${TTMMPPDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${TTMMPPDDBB1} ;

--Original Query: 70f8bab906ea5fb74c21f985d6c6280b
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t3 (   instance_id,    revenue_dist_id,    account_class_code,     contract_header_id,     contract_line_id,   created_by_name,    creation_date_time,     distribution_pct,   gl_code_combination_id,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   tran_code ) SELECT  instance_id,    revenue_dist_id,    account_class_code,     contract_header_id,     contract_line_id,   created_by_name,    creation_date_time,     distribution_pct,   gl_code_combination_id,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   tran_code  FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 WHERE (instance_id,revenue_dist_id) IN (SELECT      instance_id,     revenue_dist_id     FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2     )  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t3           SELECT DISTINCT 
            -- instance_id,
            -- revenue_dist_id,
            -- account_class_code,
            -- contract_header_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- distribution_pct,
            -- gl_code_combination_id,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- tran_code  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- DISTINCT instance_id AS auto_c00,
                    -- revenue_dist_id AS auto_c01  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 
            -- ) AS autoAlias_785 
                -- ON (
                    -- upper(trim(instance_id)) = upper(trim(autoAlias_785.AUTO_C00)) 
                    -- AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_785.AUTO_C01)) 
                -- );
INSERT 
            INTO
                TABLE
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t3           SELECT  
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 
                INNER JOIN
                    (
                        SELECT
                             instance_id AS auto_c00,
                            revenue_dist_id AS auto_c01  
                        FROM
                            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 
                    ) AS autoAlias_785 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_785.AUTO_C00)) 
                            AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_785.AUTO_C01)) 
                        );
                
--Natasha: To enable the SET property for t3
INSERT OVERWRITE TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t3           SELECT  
        DISTINCT * FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t3;----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_revn_dist_04.upd.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --UPDATE AAPPLLIIDD1EENNVV_contract_revn_dist_t2 SET current_record_ind = 'D' WHERE (instance_id,revenue_dist_id) IN   (SELECT instance_id,revenue_dist_id  FROM AAPPLLIIDD1EENNVV_contract_revn_dist_t3   GROUP BY instance_id,revenue_dist_id HAVING COUNT(*) = 1 )

--Translated Query: SUCCESS

    -- INSERT OVERWRITE
        -- TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 SELECT distinct
            -- instance_id,
            -- revenue_dist_id,
            -- account_class_code,
            -- alt_as_of_date_time,
            -- alt_batch_nbr,
            -- as_of_date_time,
            -- batch_nbr,
            -- contract_header_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- (CASE 
                -- WHEN(RCRD_UPD=1) THEN 'D' 
                -- ELSE current_record_ind 
            -- END) AS current_record_ind,
            -- distribution_pct,
            -- gl_code_combination_id,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- tran_code  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- instance_id AS auto_c00,
                    -- revenue_dist_id AS auto_c01,
                    -- COUNT(*) AS RCRD_UPD  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 
                -- GROUP BY
                    -- 1,
                    -- 2
            -- ) AS autoAlias_7 
                -- ON (
                    -- upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                    -- AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_7.AUTO_C01)) 
                -- );
INSERT OVERWRITE
                TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 SELECT 
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    (CASE 
                        WHEN(RCRD_UPD=1) THEN 'D' 
                        ELSE current_record_ind 
                    END) AS current_record_ind,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 
                LEFT OUTER JOIN
                    (
                        SELECT
                            instance_id AS auto_c00,
                            revenue_dist_id AS auto_c01,
                            COUNT(*) AS RCRD_UPD  
                        FROM
                            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 
                        GROUP BY
                            1,
                            2
                    ) AS autoAlias_7 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                            AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_7.AUTO_C01)) 
                        );
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_revn_dist_05.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${EEDDWWDDBB1}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB1};


--Original Query:
  --DELETE FROM contract_revn_dist_ld WHERE (instance_id,revenue_dist_id)IN (SELECT instance_id,revenue_dist_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1    WHERE (instance_id,revenue_dist_id) NOT IN (SELECT instance_id,revenue_dist_id FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2  WHERE current_record_ind = 'D'  )    )

--Translated Query: SUCCESS
--DROP TABLE IF EXISTS  ${EEDDWWDDBB1}.contract_revn_dist_ldTemp
--CREATE TABLE ${EEDDWWDDBB1}.contract_revn_dist_ldTemp LIKE ${EEDDWWDDBB1}.contract_revn_dist_ld
--INSERT INTO TABLE ${EEDDWWDDBB1}.contract_revn_dist_ldTemp SELECT Q1.* FROM (SELECT * FROM ${EEDDWWDDBB1}.contract_revn_dist_ld ) AS  Q1  LEFT OUTER JOIN (SELECT  * FROM  ${EEDDWWDDBB1}.contract_revn_dist_ld      ) AS Q2 ON COALESCE( Q1.instance_id ,  '1' ) = COALESCE( Q2.instance_id ,  '1' ) AND COALESCE( Q1.revenue_dist_id ,  '1' ) = COALESCE( Q2.revenue_dist_id ,  '1' ) AND COALESCE( Q1.account_class_code ,  '1' ) = COALESCE( Q2.account_class_code ,  '1' ) AND COALESCE( Q1.alt_as_of_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.alt_as_of_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.alt_batch_nbr , 1) = COALESCE( Q2.alt_batch_nbr , 1) AND COALESCE( Q1.as_of_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.as_of_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.batch_nbr , 1) = COALESCE( Q2.batch_nbr , 1) AND COALESCE( Q1.contract_header_id ,  '1' ) = COALESCE( Q2.contract_header_id ,  '1' ) AND COALESCE( Q1.contract_line_id ,  '1' ) = COALESCE( Q2.contract_line_id ,  '1' ) AND COALESCE( Q1.created_by_name ,  '1' ) = COALESCE( Q2.created_by_name ,  '1' ) AND COALESCE( Q1.creation_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.distribution_pct , 1) = COALESCE( Q2.distribution_pct , 1) AND COALESCE( Q1.gl_code_combination_id , 1) = COALESCE( Q2.gl_code_combination_id , 1) AND COALESCE( Q1.last_update_date_time ,  cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,  cast ( CURRENT_DATE() as timestamp) ) AND COALESCE( Q1.last_update_login_name ,  '1' ) = COALESCE( Q2.last_update_login_name ,  '1' ) AND COALESCE( Q1.last_updated_by_name ,  '1' ) = COALESCE( Q2.last_updated_by_name ,  '1' ) AND COALESCE( Q1.tran_code ,  '1' ) = COALESCE( Q2.tran_code ,  '1' ) WHERE Q2.instance_id IS NULL AND Q2.revenue_dist_id IS NULL AND Q2.account_class_code IS NULL AND Q2.alt_as_of_date_time IS NULL AND Q2.alt_batch_nbr IS NULL AND Q2.as_of_date_time IS NULL AND Q2.batch_nbr IS NULL AND Q2.contract_header_id IS NULL AND Q2.contract_line_id IS NULL AND Q2.created_by_name IS NULL AND Q2.creation_date_time IS NULL AND Q2.distribution_pct IS NULL AND Q2.gl_code_combination_id IS NULL AND Q2.last_update_date_time IS NULL AND Q2.last_update_login_name IS NULL AND Q2.last_updated_by_name IS NULL AND Q2.tran_code IS NULL
--DROP TABLE IF EXISTS  ${EEDDWWDDBB1}.contract_revn_dist_ld
--ALTER TABLE ${EEDDWWDDBB1}.contract_revn_dist_ldTemp RENAME TO ${EEDDWWDDBB1}.contract_revn_dist_ld
--Chngd NATASHA (Corrected the query)

    -- INSERT OVERWRITE
         -- TABLE ${EEDDWWDDBB1}.contract_revn_dist_ld 
         -- SELECT distinct Q1.* FROM  ${EEDDWWDDBB1}.contract_revn_dist_ld AS Q1
         -- LEFT OUTER JOIN 
                -- (
                -- SELECT revenue_dist_id, instance_id
                -- FROM
                 -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 AS Q2  
                -- LEFT OUTER JOIN 
                    -- (   SELECT revenue_dist_id AS autoc000 , instance_id as autoc001
                    -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 
                        -- WHERE current_record_ind = 'D' ) AS Q3 
                -- ON Q2.revenue_dist_id = Q3.autoc000
                -- AND Q2.instance_id = Q3.autoc001
                    -- WHERE Q3.autoc000 IS NULL 
                    -- AND Q3.autoc001 IS NULL
                -- ) AS Q23
         -- ON Q1.revenue_dist_id = Q23.revenue_dist_id
         -- AND Q1.instance_id = Q23.instance_id
            -- WHERE Q1.revenue_dist_id IS NULL 
            -- AND Q1.instance_id IS NULL;
INSERT OVERWRITE
                 TABLE ${EEDDWWDDBB1}.contract_revn_dist_ld 
                 SELECT distinct Q1.* FROM  ${EEDDWWDDBB1}.contract_revn_dist_ld AS Q1
                 LEFT OUTER JOIN 
                        (
                        SELECT revenue_dist_id, instance_id
                        FROM
                         ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 AS Q2  
                        LEFT OUTER JOIN 
                            (   SELECT revenue_dist_id AS autoc000 , instance_id as autoc001
                            FROM
                            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 
                                WHERE current_record_ind = 'D' ) AS Q3 
                        ON Q2.revenue_dist_id = Q3.autoc000
                        AND Q2.instance_id = Q3.autoc001
                            WHERE Q3.autoc000 IS NULL 
                            AND Q3.autoc001 IS NULL
                        ) AS Q23
                 ON Q1.revenue_dist_id = Q23.revenue_dist_id
                 AND Q1.instance_id = Q23.instance_id
                    WHERE Q23.revenue_dist_id IS NULL 
                    AND Q23.instance_id IS NULL;

--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 FOR ACCESSINSERT INTO contract_revn_dist_ld(  instance_id,    revenue_dist_id,    account_class_code, alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_code)SELECT    instance_id,    revenue_dist_id,    account_class_code, as_of_date_time,    batch_nbr,  as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_codeFROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1WHERE ( instance_id,revenue_dist_id )NOT IN (SELECT  instance_id,revenue_dist_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2    WHERE current_record_ind = 'D'     )

--Translated Query: SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${EEDDWWDDBB1}.contract_revn_dist_ld           SELECT distinct
            -- instance_id,
            -- revenue_dist_id,
            -- account_class_code,
            -- as_of_date_time,
            -- batch_nbr,
            -- as_of_date_time,
            -- batch_nbr,
            -- contract_header_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- distribution_pct,
            -- gl_code_combination_id,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- tran_code  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- DISTINCT instance_id AS auto_c00,
                    -- revenue_dist_id AS auto_c01  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2     
                -- WHERE
                    -- upper(trim(current_record_ind)) = upper(trim('D')) 
            -- ) AS autoAlias_55 
                -- ON (
                    -- upper(trim(instance_id)) = upper(trim(autoAlias_55.AUTO_C00)) 
                    -- AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_55.AUTO_C01)) 
                -- ) 
        -- WHERE
            -- autoAlias_55.AUTO_C00 IS  NULL  
            -- AND autoAlias_55.AUTO_C01 IS  NULL;
INSERT 
            INTO
                TABLE
                ${EEDDWWDDBB1}.contract_revn_dist_ld           SELECT distinct
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    as_of_date_time,
                    batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT instance_id AS auto_c00,
                            revenue_dist_id AS auto_c01  
                        FROM
                            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_55 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_55.AUTO_C00)) 
                            AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_55.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_55.AUTO_C00 IS  NULL  
                    AND autoAlias_55.AUTO_C01 IS  NULL;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_revn_dist_06.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 ALL

--Translated Query: SUCCESS

    --TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1(    instance_id,    revenue_dist_id,    account_class_code, as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_code)SELECT    instance_id,    revenue_dist_id,    account_class_code, as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_codeFROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_mlWHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,    instance_id,    revenue_dist_id )IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),     instance_id,    revenue_dist_id    FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml    WHERE tran_code = 'D'    GROUP BY 2,3 )AND tran_code = 'D'

--Translated Query: SUCCESS
--      INSERT INTO TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1           SELECT instance_id, revenue_dist_id, account_class_code, as_of_date_time, batch_nbr, contract_header_id, contract_line_id, created_by_name, creation_date_time, distribution_pct, gl_code_combination_id, last_update_date_time, last_update_login_name, last_updated_by_name, tran_code  FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml INNER JOIN (SELECT  MAX( CONCAT(CAST (as_of_date_time AS CHAR(26)) , batch_nbr) ) AS auto_c00, instance_id AS auto_c01, revenue_dist_id AS auto_c02  FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml     WHERE upper(trim(tran_code)) = upper(trim('D'))  GROUP BY instance_id , revenue_dist_id ) AS autoAlias_57 ON ( CONCAT(CAST (as_of_date_time AS CHAR(26)) , batch_nbr) = autoAlias_57.AUTO_C00 AND upper(trim(instance_id)) = upper(trim(autoAlias_57.AUTO_C01)) AND upper(trim(revenue_dist_id)) = upper(trim(autoAlias_57.AUTO_C02)) ) WHERE upper(trim(tran_code)) = upper(trim('D'))

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1           SELECT distinct
            -- instance_id,
            -- revenue_dist_id,
            -- account_class_code,
            -- as_of_date_time,
            -- batch_nbr,
            -- contract_header_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- distribution_pct,
            -- gl_code_combination_id,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- tran_code  
        -- FROM
            -- ( SELECT
                -- m1.*,
                -- ROW_NUMBER() OVER(PARTITION 
            -- BY
                -- instance_id ,
                -- revenue_dist_id 
            -- ORDER BY
                -- CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) DESC) AS r00 
            -- FROM
                -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml m1 
            -- WHERE
                -- upper(trim(tran_code)) = trim('D')) rcrd 
        -- WHERE
            -- r00=1;
INSERT OVERWRITE
                TABLE
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1           SELECT distinct
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code  
                FROM
                    ( SELECT
                        m1.*,
                        ROW_NUMBER() OVER(PARTITION 
                    BY
                        instance_id ,
                        revenue_dist_id 
                    ORDER BY
                        CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                        batch_nbr) DESC) AS r00 
                    FROM
                        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_ml m1 
                    WHERE
                        upper(trim(tran_code)) = trim('D')) rcrd 
                WHERE
                    r00=1;

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2;
--------------------------------
--LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2(  instance_id,    revenue_dist_id,    account_class_code, alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_code)SELECT    l.instance_id,  l.revenue_dist_id,  l.account_class_code,   t.as_of_date_time,  t.batch_nbr,    l.as_of_date_time,  l.batch_nbr,    l.contract_header_id,   l.contract_line_id, l.created_by_name,  l.creation_date_time,   l.distribution_pct, l.gl_code_combination_id,   l.last_update_date_time,    l.last_update_login_name,   l.last_updated_by_name, t.tran_codeFROM ${EEDDWWDDBB1}.contract_revn_dist_ld l,      ${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1 tWHERE  l.instance_id = t.instance_id AND    l.revenue_dist_id = t.revenue_dist_idAND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <    CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr
--Transformed Query : STATUS SUCCESS
--------------------------------
--      INSERT INTO TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2           SELECT l.instance_id, l.revenue_dist_id, l.account_class_code, t.as_of_date_time, t.batch_nbr, l.as_of_date_time, l.batch_nbr, l.contract_header_id, l.contract_line_id, l.created_by_name, l.creation_date_time, null, l.distribution_pct, l.gl_code_combination_id, l.last_update_date_time, l.last_update_login_name, l.last_updated_by_name, t.tran_code  FROM  ${EEDDWWDDBB1}.contract_revn_dist_ld    AS l   ,  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1    AS t   WHERE l.instance_id = t.instance_id  AND l.revenue_dist_id = t.revenue_dist_id   AND CONCAT(CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) < CONCAT(CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2           SELECT distinct
            -- l.instance_id,
            -- l.revenue_dist_id,
            -- l.account_class_code,
            -- t.as_of_date_time,
            -- t.batch_nbr,
            -- l.as_of_date_time,
            -- l.batch_nbr,
            -- l.contract_header_id,
            -- l.contract_line_id,
            -- l.created_by_name,
            -- l.creation_date_time,
            -- null,
            -- l.distribution_pct,
            -- l.gl_code_combination_id,
            -- l.last_update_date_time,
            -- l.last_update_login_name,
            -- l.last_updated_by_name,
            -- t.tran_code  
        -- FROM
            -- ${EEDDWWDDBB1}.contract_revn_dist_ld    AS l 
        -- INNER JOIN
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1    AS t   
                -- ON l.instance_id = t.instance_id  
                -- AND l.revenue_dist_id = t.revenue_dist_id 
        -- WHERE
            -- CONCAT(CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) < CONCAT(CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);
INSERT 
            INTO
                TABLE
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2           SELECT distinct
                    l.instance_id,
                    l.revenue_dist_id,
                    l.account_class_code,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.contract_header_id,
                    l.contract_line_id,
                    l.created_by_name,
                    l.creation_date_time,
                    null,
                    l.distribution_pct,
                    l.gl_code_combination_id,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    t.tran_code  
                FROM
                    ${EEDDWWDDBB1}.contract_revn_dist_ld    AS l 
                INNER JOIN
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t1    AS t   
                        ON l.instance_id = t.instance_id  
                        AND l.revenue_dist_id = t.revenue_dist_id 
                WHERE
                    CONCAT(CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) < CONCAT(CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_revn_dist_07.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
--INSERT INTO contract_revn_dist_ld(    instance_id,    revenue_dist_id,    account_class_code, alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_code)SELECT    instance_id,    revenue_dist_id,    account_class_code, alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  contract_header_id, contract_line_id,   created_by_name,    creation_date_time, distribution_pct,   gl_code_combination_id, last_update_date_time,  last_update_login_name, last_updated_by_name,   tran_code FROM TTMMPPDDBB1.AAPPLLIIDD1EENNVV_contract_revn_dist_t2;

--Translated Query: SUCCESS

    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_revn_dist_ld SELECT distinct
            -- revn_ld.* 
        -- FROM
            -- ${EEDDWWDDBB1}.contract_revn_dist_ld revn_ld 
        -- LEFT JOIN
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 revn_t2 
                -- on revn_ld.instance_id = revn_t2.instance_id 
                -- AND revn_ld.revenue_dist_id = revn_t2.revenue_dist_id 
        -- WHERE
            -- revn_t2.revenue_dist_id IS NULL 
            -- AND revn_t2.instance_id IS NULL;

INSERT OVERWRITE
                TABLE ${EEDDWWDDBB1}.contract_revn_dist_ld SELECT distinct
                    revn_ld.* 
                FROM
                    ${EEDDWWDDBB1}.contract_revn_dist_ld revn_ld 
                LEFT JOIN
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2 revn_t2 
                        on revn_ld.instance_id = revn_t2.instance_id 
                        AND revn_ld.revenue_dist_id = revn_t2.revenue_dist_id 
                WHERE
                    revn_t2.revenue_dist_id IS NULL 
                    AND revn_t2.instance_id IS NULL;
--Original Query:
--INSERT INTO contract_revn_dist_ld(instance_id,revenue_dist_id,account_class_code,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,contract_header_id,contract_line_id,created_by_name,creation_date_time,distribution_pct,gl_code_combination_id,last_update_date_time,last_update_login_name,last_updated_by_name,tran_code)SELECT instance_id,revenue_dist_id,account_class_code,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,contract_header_id,contract_line_id,created_by_name,creation_date_time,distribution_pct,gl_code_combination_id,last_update_date_time,last_update_login_name,last_updated_by_name,tran_code FROM TTMMPPDDBB1.AAPPLLIIDD1EENNVV_contract_revn_dist_t2;

--Translated Query: SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${EEDDWWDDBB1}.contract_revn_dist_ld SELECT distinct
            -- instance_id,
            -- revenue_dist_id,
            -- account_class_code,
            -- alt_as_of_date_time,
            -- alt_batch_nbr,
            -- as_of_date_time,
            -- batch_nbr,
            -- contract_header_id,
            -- contract_line_id,
            -- created_by_name,
            -- creation_date_time,
            -- distribution_pct,
            -- gl_code_combination_id,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- tran_code 
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2;
INSERT 
            INTO
                TABLE
                ${EEDDWWDDBB1}.contract_revn_dist_ld SELECT distinct
                    instance_id,
                    revenue_dist_id,
                    account_class_code,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    contract_header_id,
                    contract_line_id,
                    created_by_name,
                    creation_date_time,
                    distribution_pct,
                    gl_code_combination_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    tran_code 
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_revn_dist_t2;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_revn_dist2.ren.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${EEDDWWDDBB1}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB1};


--Original Query:
  --RENAME TABLE contract_revn_dist_ld TO contract_revn_dist

--Translated Query: MANUAL

--    ALTER TABLE ${EEDDWWDDBB1}.contract_revn_dist RENAME TO ${EEDDWWDDBB1}.contract_revn_dist_hd;

--    ALTER TABLE ${EEDDWWDDBB1}.contract_revn_dist_ld RENAME TO ${EEDDWWDDBB1}.contract_revn_dist;

--    ALTER TABLE ${EEDDWWDDBB1}.contract_revn_dist_hd RENAME TO ${EEDDWWDDBB1}.contract_revn_dist_ld;

INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.contract_revn_dist SELECT * FROM ${EEDDWWDDBB1}.contract_revn_dist_ld;