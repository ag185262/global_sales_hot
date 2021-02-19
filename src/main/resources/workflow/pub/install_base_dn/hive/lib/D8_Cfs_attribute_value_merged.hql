----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_cfs_attribute_value_ld.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${EEDDWWDDBB1}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB1};

--Original Query:
  ----DELETE FROM cfs_attribute_value_ld ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${EEDDWWDDBB1}.cfs_attribute_value_ld;

--Original Query:
  ----INSERT INTO cfs_attribute_value_ldSELECT * FROM cfs_attribute_value

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.cfs_attribute_value_ld           SELECT
            cfs_attribute_value.*  
        FROM
            ${EEDDWWDDBB1}.cfs_attribute_value;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_cfs_attribute_value_t1.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB1}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB1};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_cfs_attribute_value_t1 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_attribute_value_t1;

--Original Query:
  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_cfs_attribute_value_t1 (       attribute_value_id      ,instance_id      ,active_end_date_time      ,active_start_date_time      ,as_of_date_time      ,attribute_id      ,attribute_value      ,batch_nbr      ,created_by_name      ,creation_date_time      ,item_instance_id      ,last_update_date_time      ,last_update_login_name      ,last_updated_by_name      ,tran_code      )SELECT         a.attribute_value_id      ,a.instance_id      ,a.active_end_date_time      ,a.active_start_date_time      ,a.as_of_date_time      ,a.attribute_id      ,a.attribute_value      ,a.batch_nbr      ,a.created_by_name      ,a.creation_date_time      ,a.item_instance_id      ,a.last_update_date_time      ,a.last_update_login_name      ,a.last_updated_by_name      ,a.tran_code      FROM ${AAPPLLIIDD1EENNVV}_cfs_attribute_value_ml aWHERE (a.batch_nbr || CAST (a.as_of_date_time AS CHAR (26))                         ,a.attribute_value_id                         ,a.instance_id                         )IN (SELECT MAX (batch_nbr || CAST (as_of_date_time AS CHAR (26)))                                  ,attribute_value_id                                  ,instance_id    FROM ${AAPPLLIIDD1EENNVV}_cfs_attribute_value_ml     GROUP BY 2, 3)

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_attribute_value_t1           SELECT
            a.attribute_value_id,
            a.instance_id,
            a.active_end_date_time,
            a.active_start_date_time,
            a.as_of_date_time,
            a.attribute_id,
            a.attribute_value,
            a.batch_nbr,
            a.created_by_name,
            a.creation_date_time,
            a.item_instance_id,
            a.last_update_date_time,
            a.last_update_login_name,
            a.last_updated_by_name,
            a.tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_attribute_value_ml    AS a 
        INNER JOIN
            (
                SELECT
                    MAX( CONCAT(batch_nbr ,
                    CAST (as_of_date_time AS CHAR(26))) ) AS auto_c00,
                    attribute_value_id AS auto_c01,
                    instance_id AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_attribute_value_ml      
                GROUP BY
                    attribute_value_id ,
                    instance_id 
            ) AS autoAlias_1 
                ON (
                    CONCAT(a.batch_nbr ,
                CAST (a.as_of_date_time AS CHAR(26))) = autoAlias_1.AUTO_C00 
                AND a.attribute_value_id = autoAlias_1.AUTO_C01 
                AND a.instance_id = autoAlias_1.AUTO_C02 );
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_cfs_attribute_value_ld_1.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB1}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB1};

--Original Query:
  ----DELETE FROM ${EEDDWWDDBB1}.cfs_attribute_value_ld aWHERE  a.attribute_value_id  =  ${AAPPLLIIDD1EENNVV}_cfs_attribute_value_t1.attribute_value_idAND    a.instance_id         =  ${AAPPLLIIDD1EENNVV}_cfs_attribute_value_t1.instance_id

--Translated Query: MANUAL

    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.cfs_attribute_value_ld SELECT
            a.* 
        FROM
            ${EEDDWWDDBB1}.cfs_attribute_value_ld    AS a  
        LEFT OUTER  JOIN
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_attribute_value_t1 b 
                on    a.attribute_value_id = b.attribute_value_id  
                AND a.instance_id = b.instance_id    
        where
            b.attribute_value_id IS   NULL  
            and  b.instance_id IS  NULL;

--Original Query:
  ----INSERT INTO ${EEDDWWDDBB1}.cfs_attribute_value_ld (       attribute_value_id      ,instance_id      ,active_end_date_time      ,active_start_date_time      ,as_of_date_time      ,attribute_id      ,attribute_value      ,batch_nbr      ,created_by_name      ,creation_date_time      ,item_instance_id      ,last_update_date_time      ,last_update_login_name      ,last_updated_by_name      ,tran_code      )SELECT         attribute_value_id      ,instance_id      ,active_end_date_time      ,active_start_date_time      ,as_of_date_time      ,attribute_id      ,attribute_value      ,batch_nbr      ,created_by_name      ,creation_date_time      ,item_instance_id      ,last_update_date_time      ,last_update_login_name      ,last_updated_by_name      ,tran_codeFROM   ${AAPPLLIIDD1EENNVV}_cfs_attribute_value_t1WHERE  tran_code <> 'D'

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.cfs_attribute_value_ld           SELECT
            attribute_value_id,
            instance_id,
            active_end_date_time,
            active_start_date_time,
            as_of_date_time,
            attribute_id,
            attribute_value,
            batch_nbr,
            created_by_name,
            creation_date_time,
            item_instance_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_attribute_value_t1     
        WHERE
            upper(trim(tran_code)) <> upper(trim('D'));
