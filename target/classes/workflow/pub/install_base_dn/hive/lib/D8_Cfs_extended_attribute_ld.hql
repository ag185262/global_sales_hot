----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_cfs_extended_attribute_ld.ins.sql
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
  ----DELETE FROM cfs_extended_attribute_ld ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${EEDDWWDDBB1}.cfs_extended_attribute_ld;

--Original Query:
  ----INSERT INTO cfs_extended_attribute_ldSELECT * FROM cfs_extended_attribute

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.cfs_extended_attribute_ld           SELECT
          A.*  
        FROM
            ${EEDDWWDDBB1}.cfs_extended_attribute A;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_cfs_extended_attribute_t1.ins.sql
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
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_t1 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_t1;

--Original Query:
  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_t1 (       attribute_id                 ,instance_id                  ,active_end_date_time         ,active_start_date_time       ,as_of_date_time              ,attribute_category_code      ,attribute_code_name          ,attribute_desc               ,attribute_level_name         ,attribute_name               ,batch_nbr                    ,created_by_name              ,creation_date_time           ,inventory_org_id             ,inventory_item_id            ,item_category_id             ,last_update_date_time        ,last_update_login_name       ,last_updated_by_name         ,tran_code              )SELECT         a.attribute_id		              ,a.instance_id			      ,a.active_end_date_time		      ,a.active_start_date_time	      ,a.as_of_date_time 		      ,a.attribute_category_code	      ,a.attribute_code_name		      ,a.attribute_desc		      ,a.attribute_level_name		      ,a.attribute_name		      ,a.batch_nbr      ,a.created_by_name		      ,a.creation_date_time		      ,a.inventory_org_id		      ,a.inventory_item_id		      ,a.item_category_id		      ,a.last_update_date_time		      ,a.last_update_login_name	      ,a.last_updated_by_name		      ,a.tran_code      FROM ${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_ml aWHERE (a.batch_nbr || CAST (a.as_of_date_time AS CHAR (26))                         ,a.attribute_id                         ,a.instance_id                         )IN (SELECT MAX (batch_nbr || CAST (as_of_date_time AS CHAR (26)))                                  ,attribute_id                                  ,instance_id    FROM ${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_ml    GROUP BY 2,3)

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_t1           SELECT
            a.attribute_id,
            a.instance_id,
            a.active_end_date_time,
            a.active_start_date_time,
            a.as_of_date_time,
            a.attribute_category_code,
            a.attribute_code_name,
            a.attribute_desc,
            a.attribute_level_name,
            a.attribute_name,
            a.batch_nbr,
            a.created_by_name,
            a.creation_date_time,
            a.inventory_org_id,
            a.inventory_item_id,
            a.item_category_id,
            a.last_update_date_time,
            a.last_update_login_name,
            a.last_updated_by_name,
            a.tran_code  
        FROM (
                   SELECT
                        m1.*,
                        ROW_NUMBER() OVER(PARTITION 
                    BY
                        attribute_id ,
                        instance_id 
                    ORDER BY
                        CAST (as_of_date_time AS CHAR(26)) DESC) AS r00 
                    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_ml  m1    
            ) AS a 
               WHERE
                    r00=1;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_cfs_extended_attribute_ld_1.ins.sql
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
  ----DELETE FROM ${EEDDWWDDBB1}.cfs_extended_attribute_ld aWHERE  a.attribute_id   =  ${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_t1.attribute_idAND    a.instance_id    =  ${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_t1.instance_id

--Translated Query: MANUAL

    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.cfs_extended_attribute_ld SELECT
            a.* 
        FROM
            ${EEDDWWDDBB1}.cfs_extended_attribute_ld    AS a  
        left outer join
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_t1 b 
                on   (
                    a.attribute_id = b.attribute_id  
                    AND a.instance_id = b.instance_id  
                )  
        where
            b.attribute_id IS NULL   
            and  b.instance_id IS NULL;

--Original Query:
  ----INSERT INTO ${EEDDWWDDBB1}.cfs_extended_attribute_ld (        attribute_id                  ,instance_id                   ,active_end_date_time          ,active_start_date_time        ,as_of_date_time               ,attribute_category_code       ,attribute_code_name           ,attribute_desc                ,attribute_level_name          ,attribute_name                ,batch_nbr                     ,created_by_name               ,creation_date_time            ,inventory_org_id              ,inventory_item_id             ,item_category_id              ,last_update_date_time         ,last_update_login_name        ,last_updated_by_name          ,tran_code     )SELECT          attribute_id                  ,instance_id                   ,active_end_date_time          ,active_start_date_time        ,as_of_date_time               ,attribute_category_code       ,attribute_code_name           ,attribute_desc                ,attribute_level_name          ,attribute_name                ,batch_nbr                     ,created_by_name               ,creation_date_time            ,inventory_org_id              ,inventory_item_id             ,item_category_id              ,last_update_date_time         ,last_update_login_name        ,last_updated_by_name          ,tran_code FROM   ${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_t1WHERE  tran_code <> 'D'

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.cfs_extended_attribute_ld           SELECT
            attribute_id,
            instance_id,
            active_end_date_time,
            active_start_date_time,
            as_of_date_time,
            attribute_category_code,
            attribute_code_name,
            attribute_desc,
            attribute_level_name,
            attribute_name,
            batch_nbr,
            created_by_name,
            creation_date_time,
            inventory_org_id,
            inventory_item_id,
            item_category_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_cfs_extended_attribute_t1     
        WHERE
            upper(trim(tran_code)) <> trim('D');
