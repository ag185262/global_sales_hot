----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_header_08.ins.sql
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
--INSERT INTO AAPPLLIIDD1EENNVV_as_of_date ( oper_unit_country_code, instance_id, as_of_date ) SELECT  '99', instance_id, MAX(CAST( as_of_date_time AS DATE) ) FROM AAPPLLIIDD1EENNVV_contract_header_ml GROUP BY 1,2  ; 

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           SELECT
            '99',
            instance_id,
            MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml      
        GROUP BY
            '99' ,
            instance_id;

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           SELECT
            author_oper_unit_country_code,
            instance_id,
            MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4      
        GROUP BY
            author_oper_unit_country_code ,
            instance_id;



--Translated Query: SUCCESS

    WITH qq1 AS (    SELECT distinct
        instance_id,
        batch_nbr  
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml 
    LEFT OUTER JOIN
        (SELECT
            DISTINCT instance_id AS auto_c00,
            batch_nbr AS auto_c01  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig ) AS autoAlias_7 
            ON ( upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
            AND batch_nbr = autoAlias_7.AUTO_C01 ) 
    WHERE
        autoAlias_7.AUTO_C00 IS  NULL  
        AND autoAlias_7.AUTO_C01 IS  NULL         ) INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig       SELECT
                * 
            FROM
                qq1;----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contr_action_time_08.ins.sql
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

    -- use ${TTMMPPDDBB1};


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_action_time_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_as_of_date(oper_unit_country_code,instance_id,as_of_date)SELECT '99',instance_id,MAX(CAST( as_of_date_time AS DATE) )FROM ${AAPPLLIIDD1EENNVV}_contr_action_time_mlGROUP BY 1,2

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           SELECT
            '99',
            instance_id,
            MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_action_time_ml      
        GROUP BY
            '99' ,
            instance_id;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_action_time_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_batch_nbr_trig(instance_id,batch_nbr)SELECTinstance_id,batch_nbrFROM ${AAPPLLIIDD1EENNVV}_contr_action_time_mlWHERE (instance_id, batch_nbr)NOT IN (SELECT instance_id, batch_nbr  FROM ${AAPPLLIIDD1EENNVV}_batch_nbr_trig    )

--Translated Query: SUCCESS

    WITH qq1 AS (    SELECT distinct
        instance_id,
        batch_nbr  
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_action_time_ml 
    LEFT OUTER JOIN
        (SELECT
            DISTINCT instance_id AS auto_c00,
            batch_nbr AS auto_c01  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig ) AS autoAlias_1 
            ON ( upper(trim(instance_id)) = upper(trim(autoAlias_1.AUTO_C00)) 
            AND batch_nbr = autoAlias_1.AUTO_C01 ) 
    WHERE
        autoAlias_1.AUTO_C00 IS  NULL  
        AND autoAlias_1.AUTO_C01 IS  NULL         ) INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig       SELECT
                * 
            FROM
                qq1;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contr_cover_timezone_08.ins.sql
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

    -- use ${TTMMPPDDBB1};


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_cover_timezone_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_as_of_date(oper_unit_country_code,instance_id,as_of_date)SELECT '99',instance_id,MAX(CAST( as_of_date_time AS DATE) )FROM ${AAPPLLIIDD1EENNVV}_contr_cover_timezone_mlGROUP BY 1,2

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           SELECT
            '99',
            instance_id,
            MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_cover_timezone_ml      
        GROUP BY
            '99' ,
            instance_id;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_cover_timezone_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_batch_nbr_trig(instance_id,batch_nbr)SELECTinstance_id,batch_nbrFROM ${AAPPLLIIDD1EENNVV}_contr_cover_timezone_mlWHERE (instance_id, batch_nbr)NOT IN (SELECT instance_id, batch_nbr    FROM ${AAPPLLIIDD1EENNVV}_batch_nbr_trig    )

--Translated Query: SUCCESS

    WITH qq1 AS (    SELECT distinct 
        instance_id,
        batch_nbr  
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_cover_timezone_ml 
    LEFT OUTER JOIN
        (SELECT
            DISTINCT instance_id AS auto_c00,
            batch_nbr AS auto_c01  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig ) AS autoAlias_5 
            ON ( upper(trim(instance_id)) = upper(trim(autoAlias_5.AUTO_C00)) 
            AND batch_nbr = autoAlias_5.AUTO_C01 ) 
    WHERE
        autoAlias_5.AUTO_C00 IS  NULL  
        AND autoAlias_5.AUTO_C01 IS  NULL         ) INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig       SELECT
                * 
            FROM
                qq1;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contr_coverage_time_08.ins.sql
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

    -- use ${TTMMPPDDBB1};


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_coverage_time_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_as_of_date(oper_unit_country_code,instance_id,as_of_date)SELECT '99',instance_id,MAX(CAST( as_of_date_time AS DATE) )FROM ${AAPPLLIIDD1EENNVV}_contr_coverage_time_mlGROUP BY 1,2

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           SELECT
            '99',
            instance_id,
            MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_coverage_time_ml      
        GROUP BY
            '99' ,
            instance_id;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_coverage_time_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_batch_nbr_trig(instance_id,batch_nbr)SELECTinstance_id,batch_nbrFROM ${AAPPLLIIDD1EENNVV}_contr_coverage_time_mlWHERE (instance_id, batch_nbr)NOT IN (SELECT instance_id, batch_nbr  FROM ${AAPPLLIIDD1EENNVV}_batch_nbr_trig    )

--Translated Query: SUCCESS

    WITH qq1 AS (    SELECT distinct
        instance_id,
        batch_nbr  
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_coverage_time_ml 
    LEFT OUTER JOIN
        (SELECT
            DISTINCT instance_id AS auto_c00,
            batch_nbr AS auto_c01  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig ) AS autoAlias_7 
            ON ( upper(trim(instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
            AND batch_nbr = autoAlias_7.AUTO_C01 ) 
    WHERE
        autoAlias_7.AUTO_C00 IS  NULL  
        AND autoAlias_7.AUTO_C01 IS  NULL         ) INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig       SELECT
                * 
            FROM
                qq1;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contr_bill_stream_08.ins.sql
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

    -- use ${TTMMPPDDBB1};


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_as_of_date(     oper_unit_country_code ,instance_id    ,as_of_date)SELECT   '99'   ,instance_id    ,MAX(CAST( as_of_date_time AS DATE))FROM    ${AAPPLLIIDD1EENNVV}_contr_bill_stream_mlGROUP BY 1,2

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           SELECT
            '99',
            instance_id,
            MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml      
        GROUP BY
            '99' ,
            instance_id;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_batch_nbr_trig(     instance_id    ,batch_nbr)SELECT    instance_id    ,batch_nbrFROM  ${AAPPLLIIDD1EENNVV}_contr_bill_stream_mlWHERE  (instance_id, batch_nbr)NOT IN  (SELECT instance_id, batch_nbr   FROM ${AAPPLLIIDD1EENNVV}_batch_nbr_trig    )

--Translated Query: SUCCESS

    WITH qq1 AS (    SELECT distinct
        instance_id,
        batch_nbr  
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml 
    LEFT OUTER JOIN
        (SELECT
            DISTINCT instance_id AS auto_c00,
            batch_nbr AS auto_c01  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig ) AS autoAlias_3 
            ON ( upper(trim(instance_id)) = upper(trim(autoAlias_3.AUTO_C00)) 
            AND batch_nbr = autoAlias_3.AUTO_C01 ) 
    WHERE
        autoAlias_3.AUTO_C00 IS  NULL  
        AND autoAlias_3.AUTO_C01 IS  NULL         ) INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig       SELECT
                * 
            FROM
                qq1;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_header_svc_supp_08.ins.sql
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

    -- use ${TTMMPPDDBB1};


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_as_of_date(oper_unit_country_code,instance_id,as_of_date)SELECT '99',instance_id,MAX(CAST( as_of_date_time AS DATE) )FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_mlGROUP BY 1,2

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           SELECT
            '99',
            instance_id,
            MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml      
        GROUP BY
            '99' ,
            instance_id;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_batch_nbr_trig(instance_id,batch_nbr)SELECTinstance_id,batch_nbrFROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_mlWHERE (instance_id, batch_nbr)NOT IN (SELECT instance_id, batch_nbr  FROM ${AAPPLLIIDD1EENNVV}_batch_nbr_trig    )

--Translated Query: SUCCESS

    WITH qq1 AS (    SELECT distinct 
        instance_id,
        batch_nbr  
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml 
    LEFT OUTER JOIN
        (SELECT
            DISTINCT instance_id AS auto_c00,
            batch_nbr AS auto_c01  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig ) AS autoAlias_41 
            ON ( upper(trim(instance_id)) = upper(trim(autoAlias_41.AUTO_C00)) 
            AND batch_nbr = autoAlias_41.AUTO_C01 ) 
    WHERE
        autoAlias_41.AUTO_C00 IS  NULL  
        AND autoAlias_41.AUTO_C01 IS  NULL         ) INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig       SELECT
                * 
            FROM
                qq1;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_contract_line_svc_supp_08.ins.sql
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

    -- use ${TTMMPPDDBB1};


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_as_of_date(    oper_unit_country_code ,instance_id    ,as_of_date)SELECT   '99'   ,instance_id    ,MAX(CAST( as_of_date_time AS DATE))FROM    ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_mlGROUP BY 1,2

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           SELECT
            '99',
            instance_id,
            MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml      
        GROUP BY
            '99' ,
            instance_id;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_batch_nbr_trig(    instance_id    ,batch_nbr)SELECT    instance_id    ,batch_nbrFROM  ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_mlWHERE (instance_id, batch_nbr)NOT IN  (SELECT instance_id, batch_nbr   FROM ${AAPPLLIIDD1EENNVV}_batch_nbr_trig    )

--Translated Query: SUCCESS

    WITH qq1 AS (    SELECT distinct
        instance_id,
        batch_nbr  
    FROM
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml 
    LEFT OUTER JOIN
        (SELECT
            DISTINCT instance_id AS auto_c00,
            batch_nbr AS auto_c01  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig ) AS autoAlias_43 
            ON ( upper(trim(instance_id)) = upper(trim(autoAlias_43.AUTO_C00)) 
            AND batch_nbr = autoAlias_43.AUTO_C01 ) 
    WHERE
        autoAlias_43.AUTO_C00 IS  NULL  
        AND autoAlias_43.AUTO_C01 IS  NULL         ) INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig       SELECT
                * 
            FROM
                qq1;
--Translated Query: SUCCESS
-- INSERT 
                -- INTO
                    -- TABLE
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_as_of_date           
                    -- SELECT
                        -- '99',
                        -- instance_id,
                        -- MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    -- FROM
                        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml      
                    -- GROUP BY
                        -- '99' ,
                        -- instance_id
                -- UNION ALL 
                    -- SELECT
                        -- '99',
                        -- instance_id,
                        -- MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    -- FROM
                        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_cover_timezone_ml      
                    -- GROUP BY
                        -- '99' ,
                        -- instance_id
                -- UNION ALL
                    -- SELECT
                        -- '99',
                        -- instance_id,
                        -- MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    -- FROM
                        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_coverage_time_ml      
                    -- GROUP BY
                        -- '99' ,
                        -- instance_id
                -- UNION ALL 
                    -- SELECT
                        -- '99',
                        -- instance_id,
                        -- MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    -- FROM
                        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml      
                    -- GROUP BY
                        -- '99' ,
                        -- instance_id
                -- UNION ALL
                    -- SELECT
                        -- '99',
                        -- instance_id,
                        -- MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    -- FROM
                        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_action_time_ml      
                    -- GROUP BY
                        -- '99' ,
                        -- instance_id
                -- UNION ALL
                    -- SELECT
                        -- '99',
                        -- instance_id,
                        -- MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    -- FROM
                        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml      
                    -- GROUP BY
                        -- '99' ,
                        -- instance_id
                -- UNION ALL
                    -- SELECT
                        -- '99',
                        -- instance_id,
                        -- MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    -- FROM
                        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml      
                    -- GROUP BY
                        -- '99' ,
                        -- instance_id
                -- UNION ALL
                    -- SELECT
                        -- author_oper_unit_country_code,
                        -- instance_id,
                        -- MAX( CAST (as_of_date_time AS date) ) AS auto_c02  
                    -- FROM
                        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_t4      
                    -- GROUP BY
                        -- author_oper_unit_country_code ,
                        -- instance_id;
--Translated Query: SUCCESS
-- WITH qq1 AS (    
                -- SELECT distinct
                    -- instance_id,
                    -- batch_nbr  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_header_ml
                -- UNION ALL
                -- SELECT distinct
                    -- instance_id,
                    -- batch_nbr  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_action_time_ml
                -- UNION ALL
                -- SELECT distinct
                    -- instance_id,
                    -- batch_nbr  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml
                -- UNION ALL
                -- SELECT distinct 
                    -- instance_id,
                    -- batch_nbr  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml 
                -- UNION ALL
                -- SELECT distinct 
                    -- instance_id,
                    -- batch_nbr  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_cover_timezone_ml
                -- UNION ALL
                -- SELECT distinct
                    -- instance_id,
                    -- batch_nbr  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_coverage_time_ml 
                -- UNION ALL
                -- SELECT distinct
                    -- instance_id,
                    -- batch_nbr  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_stream_ml 
                        -- ) INSERT 
                    -- INTO
                        -- TABLE
                        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig       
                        -- SELECT
                        -- DISTINCT qq1.* 
                        -- FROM
                            -- qq1
                -- LEFT OUTER JOIN
                    -- (SELECT
                        -- instance_id AS auto_c00,
                        -- batch_nbr AS auto_c01  
                    -- FROM
                        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_batch_nbr_trig ) AS autoAlias_7 
                        -- ON ( upper(trim(qq1.instance_id)) = upper(trim(autoAlias_7.AUTO_C00)) 
                        -- AND qq1.batch_nbr = autoAlias_7.AUTO_C01 ) 
                -- WHERE
                    -- autoAlias_7.AUTO_C00 IS  NULL  
                    -- AND autoAlias_7.AUTO_C01 IS  NULL;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_curr_calc_country_0.ins.sql
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

    -- use ${TTMMPPDDBB};


--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_cfs_curr_calc_country_t0

--Translated Query: SUCCESS

    -- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_cfs_curr_calc_country_t0;


--Original Query:
  --LOCK TABLE  ${EEDDWWDDBB1}.contract_header_ld FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_cfs_curr_calc_country_t0      (      country_code      )SELECT      author_oper_unit_country_codeFROM  ${EEDDWWDDBB1}.contract_header_ldGROUP BY 1

--Translated Query: SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_cfs_curr_calc_country_t0           SELECT 
            -- author_oper_unit_country_code  
        -- FROM
            -- ${EEDDWWDDBB1}.contract_header_ld      
        -- GROUP BY
            -- author_oper_unit_country_code;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_curr_calc_country_1.ins.sql
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

    -- use ${EEDDWWDDBB};


--Original Query:
  --DELETE FROM cfs_curr_calc_country ALL

--Translated Query: SUCCESS

     TRUNCATE TABLE   ${EEDDWWDDBB}.cfs_curr_calc_country;


--Original Query:
  --LOCK TABLE  ${EEDDWWDDBB}.curr_calc FOR ACCESSLOCK TABLE  ${EEDDWWDDBB}.currency_xref FOR ACCESSINSERT INTO ${EEDDWWDDBB}.cfs_curr_calc_country      (      acc_date,      translate_from_curr_code,      translate_to_curr_code,      country_code,      func_currency_code,      us_average_rate,      us_end_of_period_rate,      local_average_rate,      local_end_of_period_rate,      euro_average_rate,      euro_end_of_period_rate      )SELECT      EDW1.acc_date,      EDW1.translate_from_curr_code,      EDW1.translate_to_curr_code,      EDW2.country_code,      <WM_COMMENT_START> func_currency_code <WM_COMMENT_END>      EDW2.currency_code,      EDW1.us_average_rate,      EDW1.us_end_of_period_rate,      EDW1.local_average_rate,      EDW1.local_end_of_period_rate,      EDW1.euro_average_rate,      EDW1.euro_end_of_period_rateFROM  ${EEDDWWDDBB}.curr_calc EDW1,      ${EEDDWWDDBB}.currency_xref EDW2WHERE EDW1.translate_to_curr_code = EDW2.currency_codeAND   EDW2.data_source_code = 'COS'

--Translated Query: SUCCESS
--      INSERT INTO TABLE  ${EEDDWWDDBB}.cfs_curr_calc_country           SELECT EDW1.acc_date, EDW1.translate_from_curr_code, EDW1.translate_to_curr_code, EDW2.country_code, EDW2.currency_code, EDW1.us_average_rate, EDW1.us_end_of_period_rate, EDW1.local_average_rate, EDW1.local_end_of_period_rate, EDW1.euro_average_rate, EDW1.euro_end_of_period_rate  FROM  ${EEDDWWDDBB}.curr_calc    AS EDW1   ,  ${EEDDWWDDBB}.currency_xref    AS EDW2   WHERE upper(trim(EDW1.translate_to_curr_code)) = upper(trim(EDW2.currency_code))  AND upper(trim(EDW2.data_source_code)) = upper(trim('COS'))
--Chngd NATASHA(removed the cross join)

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${EEDDWWDDBB}.cfs_curr_calc_country           SELECT distinct
            -- EDW1.acc_date,
            -- EDW1.translate_from_curr_code,
            -- EDW1.translate_to_curr_code,
            -- EDW2.country_code,
            -- EDW2.currency_code,
            -- EDW1.us_average_rate,
            -- EDW1.us_end_of_period_rate,
            -- EDW1.local_average_rate,
            -- EDW1.local_end_of_period_rate,
            -- EDW1.euro_average_rate,
            -- EDW1.euro_end_of_period_rate  
        -- FROM
            -- ${EEDDWWDDBB}.curr_calc    AS EDW1  
        -- INNER JOIN
            -- ${EEDDWWDDBB}.currency_xref    AS EDW2   
                -- ON trim(EDW1.translate_to_curr_code) = trim(EDW2.currency_code)  
        -- WHERE
            -- trim(EDW2.data_source_code) = trim('COS');

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_curr_calc_country_2.ins.sql
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
  --LOCK TABLE  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_cfs_curr_calc_country_t0 FOR ACCESSLOCK TABLE  ${EEDDWWDDBB}.curr_calc FOR ACCESSINSERT INTO ${EEDDWWDDBB}.cfs_curr_calc_country      (      acc_date,      translate_from_curr_code,      translate_to_curr_code,      country_code,      func_currency_code,      us_average_rate,      us_end_of_period_rate,      local_average_rate,      local_end_of_period_rate,      euro_average_rate,      euro_end_of_period_rate)SELECT      EDW1.acc_date,      EDW1.translate_from_curr_code,      EDW1.translate_to_curr_code,      EDW2.country_code,      <WM_COMMENT_START> func_currency_code <WM_COMMENT_END>      '    ',      EDW1.us_average_rate,      EDW1.us_end_of_period_rate,      EDW1.local_average_rate,      EDW1.local_end_of_period_rate,      EDW1.euro_average_rate,      EDW1.euro_end_of_period_rateFROM  ${EEDDWWDDBB}.curr_calc EDW1,      ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_cfs_curr_calc_country_t0 EDW2WHERE EDW1.translate_from_curr_code = EDW1.translate_to_curr_codeAND   EDW1.translate_to_curr_code <> 'EUR'AND   (      EDW1.translate_from_curr_code,      EDW2.country_code) NOT IN (SELECT      translate_from_curr_code,      country_codeFROM  ${EEDDWWDDBB}.cfs_curr_calc_country)

--Translated Query: SUCCESS

    -- WITH qq1 AS (    SELECT distinct
        -- EDW1.acc_date,
        -- EDW1.translate_from_curr_code,
        -- EDW1.translate_to_curr_code,
        -- EDW2.country_code,
        -- '    ' AS auto_c04,
        -- EDW1.us_average_rate,
        -- EDW1.us_end_of_period_rate,
        -- EDW1.local_average_rate,
        -- EDW1.local_end_of_period_rate,
        -- EDW1.euro_average_rate,
        -- EDW1.euro_end_of_period_rate  
    -- FROM
        -- ${EEDDWWDDBB}.curr_calc    AS EDW1   ,
        -- ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_cfs_curr_calc_country_t0    AS EDW2 
    -- LEFT OUTER JOIN
        -- (SELECT
            -- DISTINCT translate_from_curr_code AS auto_c00,
            -- country_code AS auto_c01  
        -- FROM
            -- ${EEDDWWDDBB}.cfs_curr_calc_country ) AS autoAlias_59 
            -- ON ( EDW1.translate_from_curr_code = autoAlias_59.AUTO_C00 
            -- AND upper(trim(EDW2.country_code)) = upper(trim(autoAlias_59.AUTO_C01)) ) 
    -- WHERE
        -- upper(trim(EDW1.translate_from_curr_code)) = upper(trim(EDW1.translate_to_curr_code))  
        -- AND upper(trim(EDW1.translate_to_curr_code)) <> upper(trim('EUR'))   
        -- AND autoAlias_59.AUTO_C00 IS  NULL  
        -- AND autoAlias_59.AUTO_C01 IS  NULL          ) INSERT 
        -- INTO
            -- TABLE
            -- ${EEDDWWDDBB}.cfs_curr_calc_country       SELECT
                -- * 
            -- FROM
                -- qq1;
WITH qq1 AS (    SELECT distinct
                    EDW1.acc_date,
                    EDW1.translate_from_curr_code,
                    EDW1.translate_to_curr_code,
                    CASE WHEN EDW3.currency_code IS NOT NULL AND trim(EDW3.data_source_code) = 'COS' THEN EDW3.country_code ELSE EDW2.country_code END AS country_code,
                    CASE WHEN EDW3.currency_code IS NOT NULL AND trim(EDW3.data_source_code) = 'COS' THEN EDW3.currency_code ELSE '    ' END  AS currency_code,
                    EDW1.us_average_rate,
                    EDW1.us_end_of_period_rate,
                    EDW1.local_average_rate,
                    EDW1.local_end_of_period_rate,
                    EDW1.euro_average_rate,
                    EDW1.euro_end_of_period_rate  
                FROM
                    ${EEDDWWDDBB}.curr_calc    AS EDW1   ,
                   (SELECT author_oper_unit_country_code AS  country_code
                    FROM ${EEDDWWDDBB1}.contract_header_ld      
                    GROUP BY author_oper_unit_country_code)   AS EDW2 
                LEFT OUTER JOIN ${EEDDWWDDBB}.currency_xref    AS EDW3   
                            ON trim(EDW1.translate_to_curr_code) = trim(EDW3.currency_code)
                WHERE
                    upper(trim(EDW1.translate_from_curr_code)) = upper(trim(EDW1.translate_to_curr_code))  
                    AND upper(trim(EDW1.translate_to_curr_code)) <> 'EUR') INSERT 
                    INTO
                        TABLE
                        ${EEDDWWDDBB}.cfs_curr_calc_country       SELECT
                            * 
                        FROM
                            qq1;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_curr_calc_country_3.ins.sql
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
  --LOCK TABLE ${EEDDWWDDBB}.currency_xref FOR ACCESSLOCK TABLE ${EEDDWWDDBB}.translation_rates FOR ACCESS UPDATE EDW1FROM  ${EEDDWWDDBB}.cfs_curr_calc_country EDW1,      ${EEDDWWDDBB}.translation_rates EDW2,      ${EEDDWWDDBB}.currency_xref EDW3SET   local_average_rate = (EDW2.average_rate * EDW1.us_average_rate),      local_end_of_period_rate = (EDW2.end_of_period_rate *                                  EDW1.us_end_of_period_rate),      func_currency_code = EDW3.currency_codeWHERE EDW3.data_source_code = 'COS'AND   EDW3.country_code  = EDW1.country_codeAND   EDW3.currency_code = EDW2.translate_to_curr_codeAND   EDW3.currency_code <> EDW1.translate_to_curr_codeAND   EDW2.translate_to_curr_code <> 'USD'AND   EDW2.translate_from_curr_code = 'USD'AND   EDW1.translate_from_curr_code = EDW1.translate_to_curr_codeAND   EDW1.local_average_rate = 1AND   EDW1.local_end_of_period_rate = 1

--Translated Query: SUCCESS

    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB}.cfs_curr_calc_country SELECT distinct
            EDW1.acc_date AS acc_date ,
            EDW1.translate_from_curr_code AS translate_from_curr_code ,
            EDW1.translate_to_curr_code AS translate_to_curr_code ,
            EDW1.country_code AS country_code ,
            COALESCE(EDW4.currency_code,
            EDW1.func_currency_code) AS func_currency_code ,
            EDW1.us_average_rate AS us_average_rate ,
            EDW1.us_end_of_period_rate AS us_end_of_period_rate ,
            COALESCE((EDW4.average_rate * EDW1.us_average_rate),
            EDW1.local_average_rate) AS local_average_rate ,
            COALESCE((EDW4.end_of_period_rate * EDW1.us_end_of_period_rate),
            EDW1.local_end_of_period_rate) AS local_end_of_period_rate ,
            EDW1.euro_average_rate AS euro_average_rate ,
            EDW1.euro_end_of_period_rate AS euro_end_of_period_rate 
        FROM
            ${EEDDWWDDBB}.cfs_curr_calc_country AS EDW1 
        LEFT JOIN
            (
                SELECT distinct
                    EDW2.average_rate,
                    EDW2.end_of_period_rate,
                    EDW3.currency_code,
                    EDW3.country_code 
                FROM
                    ${EEDDWWDDBB}.translation_rates AS EDW2 
                INNER JOIN
                    ${EEDDWWDDBB}.currency_xref AS EDW3 
                        ON upper(trim(EDW3.data_source_code)) = upper(trim('COS')) 
                        AND upper(trim(EDW3.currency_code )) = upper(trim(EDW2.translate_to_curr_code)) 
                        AND upper(trim(EDW2.translate_to_curr_code)) <> upper(trim('USD')) 
                        AND upper(trim(EDW2.translate_from_curr_code)) = upper(trim('USD')) 
                ) AS EDW4 
                    ON upper(trim(EDW4.country_code )) = upper(trim(EDW1.country_code )) 
            WHERE
                upper(trim(EDW1.translate_from_curr_code )) = upper(trim(EDW1.translate_to_curr_code )) 
                AND upper(trim(EDW4.currency_code )) <> upper(trim(EDW1.translate_to_curr_code )) 
                AND EDW1.local_average_rate = 1 
                AND EDW1.local_end_of_period_rate = 1;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/19 15:14:36 
--Script Name: d8_curr_calc_country_4.ins.sql
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
  --LOCK TABLE ${EEDDWWDDBB}.currency_xref FOR ACCESSLOCK TABLE ${EEDDWWDDBB}.translation_rates FOR ACCESSUPDATE EDW1FROM  ${EEDDWWDDBB}.cfs_curr_calc_country EDW1,      ${EEDDWWDDBB}.translation_rates EDW2,      ${EEDDWWDDBB}.currency_xref EDW3SET   local_average_rate = EDW2.average_rate,      local_end_of_period_rate = EDW2.end_of_period_rate,      func_currency_code = EDW3.currency_codeWHERE EDW3.data_source_code = 'COS'AND   EDW3.country_code  = EDW1.country_codeAND   EDW3.currency_code = EDW2.translate_to_curr_codeAND   EDW2.translate_to_curr_code = 'USD'AND   EDW2.translate_from_curr_code <> 'USD'AND   EDW1.translate_from_curr_code <> 'USD'AND   EDW1.translate_from_curr_code = EDW1.translate_to_curr_codeAND   EDW2.translate_from_curr_code = EDW1.translate_to_curr_codeAND   EDW1.local_average_rate = 1AND   EDW1.local_end_of_period_rate = 1

--Translated Query: SUCCESS
--REPLACED THE 5 STEP UPDATE APPROACH WITH INSERT OVERWRITE STATEMENT
--      DROP TABLE IF EXISTS ${EEDDWWDDBB}.cfs_curr_calc_countryTemp
--CREATE TABLE ${EEDDWWDDBB}.cfs_curr_calc_countryTemp Like ${EEDDWWDDBB}.cfs_curr_calc_country

    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB}.cfs_curr_calc_country SELECT distinct
            EDW1.acc_date AS acc_date ,
            EDW1.translate_from_curr_code AS translate_from_curr_code ,
            EDW1.translate_to_curr_code AS translate_to_curr_code ,
            EDW1.country_code AS country_code ,
            COALESCE( EDW3.currency_code ,
            EDW1.func_currency_code ) AS func_currency_code ,
            EDW1.us_average_rate AS us_average_rate ,
            EDW1.us_end_of_period_rate AS us_end_of_period_rate ,
            COALESCE( EDW2.average_rate ,
            EDW1.local_average_rate ) AS local_average_rate ,
            COALESCE( EDW2.end_of_period_rate ,
            EDW1.local_end_of_period_rate ) AS local_end_of_period_rate ,
            EDW1.euro_average_rate AS euro_average_rate ,
            EDW1.euro_end_of_period_rate AS euro_end_of_period_rate  
        FROM
            ${EEDDWWDDBB}.translation_rates AS EDW2  ,
            ${EEDDWWDDBB}.currency_xref AS EDW3  
        RIGHT OUTER JOIN
            ${EEDDWWDDBB}.cfs_curr_calc_country AS EDW1 
                ON upper(trim(EDW3.data_source_code)) = upper(trim('COS'))  
                AND upper(trim(EDW3.country_code)) = upper(trim(EDW1.country_code))   
                AND upper(trim(EDW3.currency_code)) = upper(trim(EDW2.translate_to_curr_code))   
                AND upper(trim(EDW2.translate_to_curr_code)) = upper(trim('USD'))   
                AND upper(trim(EDW2.translate_from_curr_code)) <> upper(trim('USD'))   
                AND upper(trim(EDW1.translate_from_curr_code)) <> upper(trim('USD'))   
                AND upper(trim(EDW1.translate_from_curr_code)) = upper(trim(EDW1.translate_to_curr_code))   
                AND upper(trim(EDW2.translate_from_curr_code)) = upper(trim(EDW1.translate_to_curr_code))   
                AND EDW1.local_average_rate = 1   
                AND EDW1.local_end_of_period_rate = 1;
--DROP table IF EXISTS ${EEDDWWDDBB}.cfs_curr_calc_country
--Alter table ${EEDDWWDDBB}.cfs_curr_calc_countryTemp rename to ${EEDDWWDDBB}.cfs_curr_calc_country

