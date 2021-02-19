----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_ld.ins.sql
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

    use ${EEDDWWDDBB};

--Original Query:
  ----DELETE FROM install_base_dn_ld ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${EEDDWWDDBB}.install_base_item_hist_ld;

--Original Query:
  ----INSERT INTO install_base_item_hist_ldSELECT * FROM install_base_item_hist

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB}.install_base_item_hist_ld           SELECT
            X.*  
        FROM
            ${EEDDWWDDBB}.install_base_item_hist X;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_hist_01.ins.sql
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
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_hist_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1(instance_history_id,instance_id,as_of_date_time,batch_nbr,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>****Added As Per CQ4952****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date)SELECT instance_history_id,instance_id,as_of_date_time,batch_nbr,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>****Added As Per CQ4952****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,CASE WHEN m.new_product_id IS NOT NULLTHEN SUBSTRING(CAST (m.creation_date_time AS VARCHAR(26))from 1 for 10) END,CASE WHEN m.new_product_id IS NOT NULL THEN SUBSTRING(CAST(m.creation_date_time AS VARCHAR(26))from 12 for 5) END,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_dateFROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_ml m WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,  instance_history_id,    instance_id )IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),     instance_history_id,    instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_ml    WHERE tran_code <> 'D'    GROUP BY 2,3)AND tran_code <> 'D'

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1           SELECT
            instance_history_id,
            instance_id,
            as_of_date_time,
            batch_nbr,
            created_by_name,
            creation_date_time,
            item_instance_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            migrated_flag,
            tran_code,
            transaction_id,
            new_product_id,
            old_product_id,
            CASE 
                WHEN m.new_product_id  IS NOT NULL  THEN substring ( CAST (m.creation_date_time AS VARCHAR(26)) ,
                1  ,
                10  )   
            END AS auto_c019,
            CASE 
                WHEN m.new_product_id  IS NOT NULL  THEN substring ( CAST (m.creation_date_time AS VARCHAR(26)) ,
                12  ,
                5  )   
            END AS auto_c020,
            new_prev_cust_nbr,
            old_prev_cust_nbr,
            new_prev_site_nbr,
            old_prev_site_nbr,
            new_service_tier_name,
            old_service_tier_name,
            esd_flag,
            media_type,
            license_model,
            license_start_date,
            license_end_date  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_ml    AS m 
        INNER JOIN
            (
                SELECT
                    MAX( CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                    batch_nbr) ) AS auto_c00,
                    instance_history_id AS auto_c01,
                    instance_id AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_ml     
                WHERE
                    upper(trim(tran_code)) <> 'D'
                GROUP BY
                    instance_history_id ,
                    instance_id 
            ) AS autoAlias_70 
                ON (
                    CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                batch_nbr) = autoAlias_70.AUTO_C00 
                AND instance_history_id = autoAlias_70.AUTO_C01 
                AND instance_id = autoAlias_70.AUTO_C02);
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_hist_02.ins.sql
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
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 FOR ACCESSLOCK TABLE  ${EEDDWWDDBB1}.install_base_item_hist_ld FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2(instance_history_id,instance_id,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,created_by_name,creation_date_time,current_record_ind,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>*****Added As Per CQ4952*****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT instance_history_id,instance_id,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,created_by_name,creation_date_time,'Y',item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>*****Added As Per CQ4952*****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM ${EEDDWWDDBB1}.install_base_item_hist_ldWHERE (instance_history_id, instance_id)IN (SELECT     instance_history_id,     instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1    )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2           SELECT
            instance_history_id,
            instance_id,
            alt_as_of_date_time,
            alt_batch_nbr,
            as_of_date_time,
            batch_nbr,
            created_by_name,
            creation_date_time,
            'Y',
            item_instance_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            migrated_flag,
            tran_code,
            transaction_id,
            new_product_id,
            old_product_id,
            product_id_swap_date,
            product_id_swap_time,
            new_prev_cust_nbr,
            old_prev_cust_nbr,
            new_prev_site_nbr,
            old_prev_site_nbr,
            new_service_tier_name,
            old_service_tier_name,
            esd_flag,
            media_type,
            license_model,
            license_start_date,
            license_end_date  
        FROM
            ${EEDDWWDDBB1}.install_base_item_hist_ld 
        INNER JOIN
            (
                SELECT
                    instance_history_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 
            ) AS autoAlias_72 
                ON (
                    instance_history_id = autoAlias_72.AUTO_C00 
                    AND instance_id = autoAlias_72.AUTO_C01 
                );

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 FOR ACCESSLOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3(instance_history_id,instance_id,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>****Added As Per CQ4952****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECTinstance_history_id,instance_id,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>****Added As Per CQ4952****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2WHERE (instance_history_id, instance_id)IN (SELECT     instance_history_id,     instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1    )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3           SELECT
            instance_history_id,
            instance_id,
            created_by_name,
            creation_date_time,
            item_instance_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            migrated_flag,
            tran_code,
            transaction_id,
            new_product_id,
            old_product_id,
            product_id_swap_date,
            product_id_swap_time,
            new_prev_cust_nbr,
            old_prev_cust_nbr,
            new_prev_site_nbr,
            old_prev_site_nbr,
            new_service_tier_name,
            old_service_tier_name,
            esd_flag,
            media_type,
            license_model,
            license_start_date,
            license_end_date  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_hist_03.ins.sql
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
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 FOR ACCESSLOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3(instance_history_id,instance_id,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>*****Added As Per CQ4952*****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECTinstance_history_id,instance_id,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>*****Added As Per CQ4952*****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1WHERE (instance_history_id, instance_id)IN (SELECT     instance_history_id,     instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2    )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3           SELECT
            instance_history_id,
            instance_id,
            created_by_name,
            creation_date_time,
            item_instance_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            migrated_flag,
            tran_code,
            transaction_id,
            new_product_id,
            old_product_id,
            product_id_swap_date,
            product_id_swap_time,
            new_prev_cust_nbr,
            old_prev_cust_nbr,
            new_prev_site_nbr,
            old_prev_site_nbr,
            new_service_tier_name,
            old_service_tier_name,
            esd_flag,
            media_type,
            license_model,
            license_start_date,
            license_end_date  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 
        INNER JOIN
            (
                SELECT
                     instance_history_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2 
            ) AS autoAlias_76 
                ON (
                    instance_history_id = autoAlias_76.AUTO_C00 
                    AND instance_id = autoAlias_76.AUTO_C01 
                );
--Translated Query: MANUAL
INSERT overwrite TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3  SELECT DISTINCT * FROM  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_hist_04.upd.sql
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
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3 FOR ACCESSUPDATE ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2 SET current_record_ind = 'D'WHERE (instance_history_id, instance_id)  IN    (SELECT instance_history_id, instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3    GROUP BY instance_history_id, instance_id    HAVING COUNT(*) = 1    )

--Translated Query: SUCCESS
--Modified Further (Comments: Replaced 5 step approach with insert overwrite and removed distinct with groupBy

--    DROP TABLE IF EXISTS ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2Temp;
--    CREATE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2Temp Like ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2;
    INSERT 
    overwrite
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2
        SELECT
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.instance_history_id AS instance_history_id ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.alt_as_of_date_time AS alt_as_of_date_time ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.alt_batch_nbr AS alt_batch_nbr ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.as_of_date_time AS as_of_date_time ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.batch_nbr AS batch_nbr ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.created_by_name AS created_by_name ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.creation_date_time AS creation_date_time ,
            CASE 
                WHEN autoAlias_78.auto_c00 IS not null THEN 'D' 
                ELSE AAPPLLIIDD1EENNVV_install_base_item_hist_t2.current_record_ind 
            END AS current_record_ind ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.item_instance_id AS item_instance_id ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.last_update_date_time AS last_update_date_time ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.last_update_login_name AS last_update_login_name ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.last_updated_by_name AS last_updated_by_name ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.migrated_flag AS migrated_flag ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.tran_code AS tran_code ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.transaction_id AS transaction_id ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.new_product_id AS new_product_id ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.old_product_id AS old_product_id ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.product_id_swap_date AS product_id_swap_date ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.product_id_swap_time AS product_id_swap_time ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.new_prev_cust_nbr AS new_prev_cust_nbr ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.old_prev_cust_nbr AS old_prev_cust_nbr ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.new_prev_site_nbr AS new_prev_site_nbr ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.old_prev_site_nbr AS old_prev_site_nbr ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.new_service_tier_name AS new_service_tier_name ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.old_service_tier_name AS old_service_tier_name ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.esd_flag AS esd_flag ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.media_type AS media_type ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.license_model AS license_model ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.license_start_date AS license_start_date ,
            AAPPLLIIDD1EENNVV_install_base_item_hist_t2.license_end_date AS license_end_date 
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2 AS AAPPLLIIDD1EENNVV_install_base_item_hist_t2 
        Left Outer Join
            (
                SELECT
                    instance_history_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t3      
                GROUP BY
                    instance_history_id,
                    instance_id 
                HAVING
                    COUNT(*) = 1
            )autoAlias_78 
                ON (
                    autoAlias_78.auto_c00 = AAPPLLIIDD1EENNVV_install_base_item_hist_t2.instance_history_id 
                    AND autoAlias_78.auto_c01 = AAPPLLIIDD1EENNVV_install_base_item_hist_t2.instance_id
                );
--    DROP table IF EXISTS ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2;
--    Alter table ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2Temp rename to ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_hist_05.ins.sql
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
  ----DELETE FROM install_base_item_hist_ld WHERE (instance_history_id, instance_id )IN (SELECT instance_history_id, instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1    WHERE (instance_history_id, instance_id) NOT IN  (SELECT instance_history_id, instance_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2  WHERE current_record_ind = 'D'  )    )

--Translated Query: STATUS MANUAL
    WITH CTE AS 
    (SELECT   T1.instance_id ,T1.item_instance_id FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 T1
        LEFT JOIN
        (SELECT  instance_id,item_instance_id,current_record_ind FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2) T2
        ON T1.instance_id=T2.instance_id
        AND T1.item_instance_id=T2.item_instance_id
        AND T2.current_record_ind = 'D'
        WHERE T2.instance_id IS NULL
        AND T2.item_instance_id IS NULL
    )
        
    INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.install_base_item_hist_ld
    SELECT A.* from  ${EEDDWWDDBB1}.install_base_item_hist_ld A
    LEFT OUTER JOIN
    CTE B
    ON  A.instance_id=B.instance_id
    AND A.item_instance_id=B.item_instance_id
    WHERE B.instance_id IS  NULL
    AND B.item_instance_id IS  NULL;
  
    
--Original Query:
  ----LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 FOR ACCESSINSERT INTO install_base_item_hist_ld(instance_history_id,instance_id,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>*****Added As Per CQ4952*****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT instance_history_id,instance_id,as_of_date_time,batch_nbr,as_of_date_time,batch_nbr,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>*****Added As Per CQ4952*****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1WHERE ( instance_history_id, instance_id )NOT IN (SELECT  instance_history_id, instance_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2    WHERE current_record_ind = 'D'     )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_item_hist_ld           SELECT
            instance_history_id,
            instance_id,
            as_of_date_time,
            batch_nbr,
            as_of_date_time,
            batch_nbr,
            created_by_name,
            creation_date_time,
            item_instance_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            migrated_flag,
            tran_code,
            transaction_id,
            new_product_id,
            old_product_id,
            product_id_swap_date,
            product_id_swap_time,
            new_prev_cust_nbr,
            old_prev_cust_nbr,
            new_prev_site_nbr,
            old_prev_site_nbr,
            new_service_tier_name,
            old_service_tier_name,
            esd_flag,
            media_type,
            license_model,
            license_start_date,
            license_end_date  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 
        LEFT OUTER JOIN
            (
                SELECT
                     instance_history_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2     
                WHERE
                    upper(trim(current_record_ind)) = 'D'
            ) AS autoAlias_82 
                ON (
                    instance_history_id = autoAlias_82.AUTO_C00 
                    AND instance_id = autoAlias_82.AUTO_C01 
                ) 
        WHERE
            autoAlias_82.AUTO_C00 IS  NULL  
            AND autoAlias_82.AUTO_C01 IS  NULL;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_hist_06.ins.sql
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
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_hist_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1(instance_history_id,instance_id,as_of_date_time,batch_nbr,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>****Added As Per CQ4952****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT instance_history_id,instance_id,as_of_date_time,batch_nbr,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>****Added As Per CQ4952****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,CASE WHEN EDW1.new_product_id IS NOT NULL THEN SUBSTRING(CAST(EDW1.creation_date_time AS VARCHAR(26))FROM 1 FOR 10) END,CASE WHEN EDW1.new_product_id IS NOT NULL THEN SUBSTRING(CAST(EDW1.creation_date_time AS VARCHAR(26))FROM 12 FOR 5) END,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_ml EDW1 WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,     instance_history_id,    instance_id)IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),  instance_history_id,    instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_ml    WHERE tran_code = 'D'    GROUP BY 2,3 )AND tran_code = 'D'

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1           SELECT
            instance_history_id,
            instance_id,
            as_of_date_time,
            batch_nbr,
            created_by_name,
            creation_date_time,
            item_instance_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            migrated_flag,
            tran_code,
            transaction_id,
            new_product_id,
            old_product_id,
            CASE 
                WHEN EDW1.new_product_id  IS NOT NULL  THEN substring ( CAST (EDW1.creation_date_time AS VARCHAR(26)) ,
                1  ,
                10  )   
            END AS auto_c019,
            CASE 
                WHEN EDW1.new_product_id  IS NOT NULL  THEN substring ( CAST (EDW1.creation_date_time AS VARCHAR(26)) ,
                12  ,
                5  )   
            END AS auto_c020,
            new_prev_cust_nbr,
            old_prev_cust_nbr,
            new_prev_site_nbr,
            old_prev_site_nbr,
            new_service_tier_name,
            old_service_tier_name,
            esd_flag,
            media_type,
            license_model,
            license_start_date,
            license_end_date  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_ml    AS EDW1 
        INNER JOIN
            (
                SELECT
                    MAX( CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                    batch_nbr) ) AS auto_c00,
                    instance_history_id AS auto_c01,
                    instance_id AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_ml     
                WHERE
                    upper(trim(tran_code)) = upper(trim('D'))  
                GROUP BY
                    instance_history_id ,
                    instance_id 
            ) AS autoAlias_84 
                ON (
                    CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                batch_nbr) = autoAlias_84.AUTO_C00 
                AND instance_history_id = autoAlias_84.AUTO_C01 
                AND upper(trim(instance_id)) = upper(trim(autoAlias_84.AUTO_C02)) ) 
            WHERE
                upper(trim(tran_code)) = 'D';

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2(instance_history_id,instance_id,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,created_by_name,creation_date_time,item_instance_id,last_update_date_time,last_update_login_name,last_updated_by_name,migrated_flag,tran_code,transaction_id,<WM_COMMENT_START>****Added As Per CQ4952****<WM_COMMENT_END>new_product_id,old_product_id,new_prev_cust_nbr,old_prev_cust_nbr,new_prev_site_nbr,old_prev_site_nbr,product_id_swap_date,product_id_swap_time,new_service_tier_name,old_service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECTl.instance_history_id,l.instance_id,t.as_of_date_time,t.batch_nbr,l.as_of_date_time,l.batch_nbr,l.created_by_name,l.creation_date_time,l.item_instance_id,l.last_update_date_time,l.last_update_login_name,l.last_updated_by_name,l.migrated_flag,t.tran_code,l.transaction_id,<WM_COMMENT_START>****Added As Per CQ4952****<WM_COMMENT_END>l.new_product_id,l.old_product_id,l.new_prev_cust_nbr,l.old_prev_cust_nbr,l.new_prev_site_nbr,l.old_prev_site_nbr,l.product_id_swap_date,l.product_id_swap_time,l.new_service_tier_name,l.old_service_tier_name,l.esd_flag,                   l.media_type,                 l.license_model,              l.license_start_date,         l.license_end_date           FROM ${EEDDWWDDBB1}.install_base_item_hist_ld l,      ${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1 tWHERE  l.instance_history_id = t.instance_history_id AND    l.instance_id = t.instance_idAND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <    CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2           SELECT
            l.instance_history_id,
            l.instance_id,
            t.as_of_date_time,
            t.batch_nbr,
            l.as_of_date_time,
            l.batch_nbr,
            l.created_by_name,
            l.creation_date_time,
            null,
            l.item_instance_id,
            l.last_update_date_time,
            l.last_update_login_name,
            l.last_updated_by_name,
            l.migrated_flag,
            t.tran_code,
            l.transaction_id,
            l.new_product_id,
            l.old_product_id,
            l.product_id_swap_date,
            l.product_id_swap_time,
            l.new_prev_cust_nbr,
            l.old_prev_cust_nbr,
            l.new_prev_site_nbr,
            l.old_prev_site_nbr,
            l.new_service_tier_name,
            l.old_service_tier_name,
            l.esd_flag,
            l.media_type,
            l.license_model,
            l.license_start_date,
            l.license_end_date  
        FROM
            ${EEDDWWDDBB1}.install_base_item_hist_ld    AS l   ,
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t1    AS t   
        WHERE
            l.instance_history_id = t.instance_history_id  
            AND l.instance_id = t.instance_id   
            AND CONCAT(CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) < CONCAT(CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_hist_07.ins.sql
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
  ----DELETE FROM install_base_item_hist_ld WHERE (instance_history_id, instance_id)IN (SELECT instance_history_id, instance_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2    )

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_item_hist_ld           SELECT
            instance_history_id,
            instance_id,
            alt_as_of_date_time,
            alt_batch_nbr,
            as_of_date_time,
            batch_nbr,
            created_by_name,
            creation_date_time,
            item_instance_id,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            migrated_flag,
            tran_code,
            transaction_id,
            new_product_id,
            old_product_id,
            product_id_swap_date,
            product_id_swap_time,
            new_prev_cust_nbr,
            old_prev_cust_nbr,
            new_prev_site_nbr,
            old_prev_site_nbr,
            new_service_tier_name,
            old_service_tier_name,
            esd_flag,
            media_type,
            license_model,
            license_start_date,
            license_end_date  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_hist_t2;

--Original Query:
  ----COLLECT STATISTICS ON ${EEDDWWDDBB1}.install_base_item_hist_ld COLUMN (INSTANCE_HISTORY_ID ,INSTANCE_ID ,ITEM_INSTANCE_ID )

--Translated Query: SUCCESS

ANALYZE TABLE ${EEDDWWDDBB1}.install_base_item_hist_ld COMPUTE STATISTICS  FOR COLUMNS INSTANCE_HISTORY_ID,INSTANCE_ID,ITEM_INSTANCE_ID;

--Original Query:
  ----COLLECT STATISTICS ON ${EEDDWWDDBB1}.install_base_item_hist_ld COLUMN (item_instance_id,instance_id)

--Translated Query: SUCCESS

    ANALYZE TABLE ${EEDDWWDDBB1}.install_base_item_hist_ld COMPUTE STATISTICS  FOR COLUMNS item_instance_id,instance_id;