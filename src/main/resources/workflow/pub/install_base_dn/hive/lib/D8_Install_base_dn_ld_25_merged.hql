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

    TRUNCATE TABLE   ${EEDDWWDDBB}.install_base_dn_ld;

--Original Query:
  ----INSERT INTO install_base_dn_ldSELECT * FROM install_base_dn

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB}.install_base_dn_ld           SELECT
            X.*  
        FROM
            ${EEDDWWDDBB}.install_base_dn X;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_01.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t01 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t01;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.install_base_item_ld FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t01(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,external_reference_nbr     ,install_date     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_id     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,return_by_date     ,prev_site_nbr     ,service_tier_name,   esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      instance_id     ,item_instance_id     ,CAST(active_end_date_time AS DATE)     ,CAST(active_start_date_time AS DATE)     ,CAST(actual_return_date_time AS DATE)     ,external_reference_nbr     ,CAST(install_date_time AS DATE)     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_id     <WM_COMMENT_START> item_instance_tran_code <WM_COMMENT_END>     ,tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,CAST(return_by_date_time AS DATE)     ,prev_site_nbr     ,service_tier_name,   esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM  ${EEDDWWDDBB2}.install_base_item_ld

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t01           SELECT
            instance_id,
            item_instance_id,
            CAST (active_end_date_time AS date) AS auto_c02,
            CAST (active_start_date_time AS date) AS auto_c03,
            CAST (actual_return_date_time AS date) AS auto_c04,
            external_reference_nbr,
            CAST (install_date_time AS date) AS auto_c06,
            install_location_id,
            install_location_type_code,
            inventory_item_id,
            inventory_master_org_id,
            item_instance_status_id,
            tran_code,
            last_order_line_id,
            last_valid_invtry_org_id,
            location_id,
            location_type_code,
            CAST (return_by_date_time AS date) AS auto_c017,
            prev_site_nbr,
            service_tier_name,
            esd_flag,
            media_type,
            license_model,
            license_start_date,
            license_end_date,
        cit_vendor_code,        
        mdm_product_identifier, 
        mdm_solution_identifier
        FROM
            ${EEDDWWDDBB2}.install_base_item_ld;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_02.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t02 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t02;

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t01    COLUMN item_instance_status_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t01 COMPUTE STATISTICS  FOR COLUMNS item_instance_status_id;

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t01        COLUMN instance_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t01 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.install_base_item_sts_ld FOR ACCESSLOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t01 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t02(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,external_reference_nbr     ,install_date     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,return_by_date     ,service_order_allowed_flag     ,status_tran_code     ,terminated_flag     ,prev_site_nbr     ,service_tier_name,    esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW1.instance_id     ,EDW1.item_instance_id     ,EDW1.active_end_date     ,EDW1.active_start_date     ,EDW1.actual_return_date     ,EDW1.external_reference_nbr     ,EDW1.install_date     ,EDW1.install_location_id     ,EDW1.install_location_type_code     ,EDW1.inventory_item_id     ,EDW1.inventory_master_org_id     ,IBIS.item_instance_status_desc     ,EDW1.item_instance_status_id     ,IBIS.item_instance_status_name     ,EDW1.item_instance_tran_code     ,EDW1.last_order_line_id     ,EDW1.last_valid_invtry_org_id     ,EDW1.location_id     ,EDW1.location_type_code     ,EDW1.return_by_date     ,IBIS.service_order_allowed_flag     <WM_COMMENT_START> status_tran_code <WM_COMMENT_END>     ,IBIS.tran_code     ,IBIS.terminated_flag     ,EDW1.prev_site_nbr     ,EDW1.service_tier_name,    EDW1.esd_flag,                        EDW1.media_type,                      EDW1.license_model,                   EDW1.license_start_date,              EDW1.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t01 EDW1LEFT OUTER JOIN${EEDDWWDDBB2}.install_base_item_sts_ld IBISON  EDW1.instance_id = IBIS.instance_idAND EDW1.item_instance_status_id = IBIS.item_instance_status_id

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t02           SELECT
            EDW1.instance_id,
            EDW1.item_instance_id,
            EDW1.active_end_date,
            EDW1.active_start_date,
            EDW1.actual_return_date,
            EDW1.external_reference_nbr,
            EDW1.install_date,
            EDW1.install_location_id,
            EDW1.install_location_type_code,
            EDW1.inventory_item_id,
            EDW1.inventory_master_org_id,
            IBIS.item_instance_status_desc,
            EDW1.item_instance_status_id,
            IBIS.item_instance_status_name,
            EDW1.item_instance_tran_code,
            EDW1.last_order_line_id,
            EDW1.last_valid_invtry_org_id,
            EDW1.location_id,
            EDW1.location_type_code,
            EDW1.return_by_date,
            IBIS.service_order_allowed_flag,
            IBIS.tran_code,
            IBIS.terminated_flag,
            EDW1.prev_site_nbr,
            EDW1.service_tier_name,
            EDW1.esd_flag,
            EDW1.media_type,
            EDW1.license_model,
            EDW1.license_start_date,
            EDW1.license_end_date,
            EDW1.cit_vendor_code,        
        EDW1.mdm_product_identifier, 
        EDW1.mdm_solution_identifier  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t01    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.install_base_item_sts_ld    AS IBIS    
                ON EDW1.instance_id = IBIS.instance_id  
                AND EDW1.item_instance_status_id = IBIS.item_instance_status_id;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_03.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t03 ALL

--Translated Query: SUCCESS

TRUNCATE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t03;
TRUNCATE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t02_orc;
INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t02_orc select * from ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t02;


drop table if exists ${TTMMPPDDBB}.install_base_vrsn_lbl_ldttmp;
Create table ${TTMMPPDDBB}.install_base_vrsn_lbl_ldttmp like  ${EEDDWWDDBB2}.install_base_vrsn_lbl_ld  stored as orc;
ALTER TABLE ${TTMMPPDDBB}.install_base_vrsn_lbl_ldttmp CLUSTERED BY (item_instance_id,instance_id) into 160 buckets ;
INSERT INTO ${TTMMPPDDBB}.install_base_vrsn_lbl_ldttmp 
Select  instance_id              
,version_label_id         
,active_end_date_time     
,active_start_date_time   
,alt_as_of_date_time      
,alt_batch_nbr            
,as_of_date_time          
,batch_nbr                
,created_by_name          
,creation_date_time       
,item_instance_id         
,last_update_date_time    
,last_update_login_name   
,last_updated_by_name     
,migrated_flag            
,tran_code                
,version_label_date_time  
,version_label_desc       
,version_label_name       

 from
(select  instance_id              
,version_label_id         
,active_end_date_time     
,active_start_date_time   
,alt_as_of_date_time      
,alt_batch_nbr            
,as_of_date_time          
,batch_nbr                
,created_by_name          
,creation_date_time       
,item_instance_id         
,last_update_date_time    
,last_update_login_name   
,last_updated_by_name     
,migrated_flag            
,tran_code                
,version_label_date_time  
,version_label_desc       
,version_label_name       
, rank() over (partition by instance_id, item_instance_id order by version_label_id DESC) AS RNK FROM ${EEDDWWDDBB2}.install_base_vrsn_lbl_ld)X where X.RNK=1; 

analyze table ${TTMMPPDDBB}.install_base_vrsn_lbl_ldttmp compute statistics;
analyze table ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t02_orc compute statistics;


INSERT INTO  ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t03
SELECT     EDW2.instance_id,EDW2.item_instance_id,EDW2.active_end_date,EDW2.active_start_date,EDW2.actual_return_date,EDW2.external_reference_nbr,EDW2.install_date,EDW2.install_location_id,EDW2.install_location_type_code,EDW2.inventory_item_id,EDW2.inventory_master_org_id,EDW2.item_instance_status_desc,EDW2.item_instance_status_id,EDW2.item_instance_status_name,EDW2.item_instance_tran_code,EDW2.last_order_line_id,EDW2.last_valid_invtry_org_id,EDW2.location_id,EDW2.location_type_code,EDW2.return_by_date,EDW2.service_order_allowed_flag,EDW2.status_tran_code,EDW2.terminated_flag,CAST(IBVL.version_label_date_time AS DATE),IBVL.version_label_desc,IBVL.version_label_name,IBVL.tran_code,EDW2.prev_site_nbr,EDW2.service_tier_name,    EDW2.esd_flag,EDW2.media_type,  EDW2.license_model,EDW2.license_start_date,EDW2.license_end_date, EDW2.cit_vendor_code, EDW2.mdm_product_identifier,EDW2.mdm_solution_identifier
FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t02_orc EDW2 LEFT OUTER JOIN ${TTMMPPDDBB}.install_base_vrsn_lbl_ldttmp IBVL
ON EDW2.instance_id = IBVL.instance_id AND EDW2.item_instance_id = IBVL.item_instance_id ;

--INSERT OVERWRITE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t03 SELECT DISTINCT * FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t03;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_04.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t04 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t04;

--Original Query:
  ----COLLECT STATS ${AAPPLLIIDD1EENNVV}_install_base_dn_t03 COLUMN instance_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t03 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  ----COLLECT STATS ${AAPPLLIIDD1EENNVV}_install_base_dn_t03 COLUMN install_location_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t03 COMPUTE STATISTICS  FOR COLUMNS install_location_id;

--Original Query:
  ----COLLECT STATS ${AAPPLLIIDD1EENNVV}_install_base_dn_t03 COLUMN ( instance_id , install_location_id )

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t03 COMPUTE STATISTICS  FOR COLUMNS instance_id,install_location_id;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.cust_account_site FOR ACCESSLOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t03 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t04(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,return_by_date     ,service_order_allowed_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vrsn_lbl_tran_code     ,prev_site_nbr     ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW3.instance_id     ,EDW3.item_instance_id     ,EDW3.active_end_date     ,EDW3.active_start_date     ,EDW3.actual_return_date     ,EDW3.external_reference_nbr     ,EDW3.install_date     <WM_COMMENT_START> install_loc_oper_unit_id <WM_COMMENT_END>     ,CAS.operating_unit_id     ,EDW3.install_location_id     ,EDW3.install_location_type_code     ,EDW3.inventory_item_id     ,EDW3.inventory_master_org_id     ,EDW3.item_instance_status_desc     ,EDW3.item_instance_status_id     ,EDW3.item_instance_status_name     ,EDW3.item_instance_tran_code     ,EDW3.last_order_line_id     ,EDW3.last_valid_invtry_org_id     ,EDW3.location_id     ,EDW3.location_type_code     ,EDW3.return_by_date     ,EDW3.service_order_allowed_flag     ,EDW3.status_tran_code     ,EDW3.terminated_flag     ,EDW3.version_label_date     ,EDW3.version_label_desc     ,EDW3.version_label_name     ,EDW3.vrsn_lbl_tran_code     ,EDW3.prev_site_nbr     ,EDW3.service_tier_name,  EDW3.esd_flag,                        EDW3.media_type,                      EDW3.license_model,                   EDW3.license_start_date,              EDW3.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t03 EDW3LEFT OUTER JOIN${EEDDWWDDBB1}.cust_account_site CASON  EDW3.instance_id = CAS.instance_idAND EDW3.install_location_id = CAS.party_site_idAND EDW3.install_location_type_code = 'HZ_PARTY_SITES'WHERE EDW3.install_location_id > ''

--Translated Query: MANUAL

    with qq1 as  (select * from ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t03  where trim(upper(install_location_type_code)) = 'HZ_PARTY_SITES' )
            INSERT  
            INTO
                TABLE
                ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t04           SELECT
                    EDW3.instance_id,
                    EDW3.item_instance_id,
                    EDW3.active_end_date,
                    EDW3.active_start_date,
                    EDW3.actual_return_date,
                    EDW3.external_reference_nbr,
                    EDW3.install_date,
                    (case when EDW3.install_location_id is not null then CAS.operating_unit_id else NULL end),
                    EDW3.install_location_id,
                    EDW3.install_location_type_code,
                    EDW3.inventory_item_id,
                    EDW3.inventory_master_org_id,
                    EDW3.item_instance_status_desc,
                    EDW3.item_instance_status_id,
                    EDW3.item_instance_status_name,
                    EDW3.item_instance_tran_code,
                    EDW3.last_order_line_id,
                    EDW3.last_valid_invtry_org_id,
                    EDW3.location_id,
                    EDW3.location_type_code,
                    EDW3.return_by_date,
                    EDW3.service_order_allowed_flag,
                    EDW3.status_tran_code,
                    EDW3.terminated_flag,
                    EDW3.version_label_date,
                    EDW3.version_label_desc,
                    EDW3.version_label_name,
                    EDW3.vrsn_lbl_tran_code,
                    EDW3.prev_site_nbr,
                    EDW3.service_tier_name,
                    EDW3.esd_flag,
                    EDW3.media_type,
                    EDW3.license_model,
                    EDW3.license_start_date,
                    EDW3.license_end_date,
                EDW3.cit_vendor_code,        
                EDW3.mdm_product_identifier, 
                EDW3.mdm_solution_identifier
                FROM
                    qq1 AS EDW3  
                LEFT OUTER  JOIN
                    ${EEDDWWDDBB1}.cust_account_site    AS CAS  
                        ON EDW3.instance_id = CAS.instance_id  
                        AND EDW3.install_location_id = CAS.party_site_id;

--Original Query:
  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t04(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,return_by_date     ,service_order_allowed_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vrsn_lbl_tran_code     ,prev_site_nbr     ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW3.instance_id     ,EDW3.item_instance_id     ,EDW3.active_end_date     ,EDW3.active_start_date     ,EDW3.actual_return_date     ,EDW3.external_reference_nbr     ,EDW3.install_date     <WM_COMMENT_START> install_loc_oper_unit_id <WM_COMMENT_END>     ,NULL                                    ,EDW3.install_location_id     ,EDW3.install_location_type_code     ,EDW3.inventory_item_id     ,EDW3.inventory_master_org_id     ,EDW3.item_instance_status_desc     ,EDW3.item_instance_status_id     ,EDW3.item_instance_status_name     ,EDW3.item_instance_tran_code     ,EDW3.last_order_line_id     ,EDW3.last_valid_invtry_org_id     ,EDW3.location_id     ,EDW3.location_type_code     ,EDW3.return_by_date     ,EDW3.service_order_allowed_flag     ,EDW3.status_tran_code     ,EDW3.terminated_flag     ,EDW3.version_label_date     ,EDW3.version_label_desc     ,EDW3.version_label_name     ,EDW3.vrsn_lbl_tran_code     ,EDW3.prev_site_nbr     ,EDW3.service_tier_name,    EDW3.esd_flag,                        EDW3.media_type,                      EDW3.license_model,                   EDW3.license_start_date,              EDW3.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t03 EDW3WHERE EDW3.install_location_id = ''

--Translated Query: SUCCESS

---    INSERT 
---    INTO
---        TABLE
---        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t04           SELECT
---            EDW3.instance_id,
---            EDW3.item_instance_id,
---            EDW3.active_end_date,
---            EDW3.active_start_date,
---            EDW3.actual_return_date,
---            EDW3.external_reference_nbr,
---            EDW3.install_date,
---            CAST( NULL AS STRING ),
---            EDW3.install_location_id,
---            EDW3.install_location_type_code,
---            EDW3.inventory_item_id,
---            EDW3.inventory_master_org_id,
---            EDW3.item_instance_status_desc,
---            EDW3.item_instance_status_id,
---            EDW3.item_instance_status_name,
---            EDW3.item_instance_tran_code,
---            EDW3.last_order_line_id,
---            EDW3.last_valid_invtry_org_id,
---            EDW3.location_id,
---            EDW3.location_type_code,
---            EDW3.return_by_date,
---            EDW3.service_order_allowed_flag,
---            EDW3.status_tran_code,
---            EDW3.terminated_flag,
---            EDW3.version_label_date,
---            EDW3.version_label_desc,
---            EDW3.version_label_name,
---            EDW3.vrsn_lbl_tran_code,
---            EDW3.prev_site_nbr,
---            EDW3.service_tier_name,
---            EDW3.esd_flag,
---            EDW3.media_type,
---            EDW3.license_model,
---            EDW3.license_start_date,
---            EDW3.license_end_date,
---            EDW3.cit_vendor_code,
---            EDW3.mdm_product_identifier,
---            EDW3.mdm_solution_identifier
---        FROM
---            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t03    AS EDW3   
---        WHERE
---            EDW3.install_location_id is null;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t01 ALL

--Translated Query: SUCCESS W8c

    ----TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t01;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_05.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t05 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t05;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.cust_account_site FOR ACCESSLOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t04 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t05(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,country_code     ,crnt_loc_cs_fml_org_code     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,return_by_date     ,service_order_allowed_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vrsn_lbl_tran_code     ,prev_site_nbr     ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW4.instance_id     ,EDW4.item_instance_id     ,EDW4.active_end_date     ,EDW4.active_start_date     ,EDW4.actual_return_date     <WM_COMMENT_START> country_code <WM_COMMENT_END>     ,CAS.oper_unit_country_code     <WM_COMMENT_START> crnt_loc_cs_fml_org_code <WM_COMMENT_END>     ,CAS.cs_fml_organization_code     ,EDW4.external_reference_nbr     ,EDW4.install_date     ,EDW4.install_loc_oper_unit_id     ,EDW4.install_location_id     ,EDW4.install_location_type_code     ,EDW4.inventory_item_id     ,EDW4.inventory_master_org_id     ,EDW4.item_instance_status_desc     ,EDW4.item_instance_status_id     ,EDW4.item_instance_status_name     ,EDW4.item_instance_tran_code     ,EDW4.last_order_line_id     ,EDW4.last_valid_invtry_org_id     ,EDW4.location_id     ,EDW4.location_type_code     ,EDW4.return_by_date     ,EDW4.service_order_allowed_flag     ,EDW4.status_tran_code     ,EDW4.terminated_flag     ,EDW4.version_label_date     ,EDW4.version_label_desc     ,EDW4.version_label_name     ,EDW4.vrsn_lbl_tran_code     ,EDW4.prev_site_nbr     ,EDW4.service_tier_name,   EDW4.esd_flag,                        EDW4.media_type,                      EDW4.license_model,                   EDW4.license_start_date,              EDW4.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t04 EDW4LEFT OUTER JOIN${EEDDWWDDBB1}.cust_account_site CASON  EDW4.instance_id = CAS.instance_idAND EDW4.location_id = CAS.party_site_idAND EDW4.location_type_code = 'HZ_PARTY_SITES'

--Translated Query: MANUAL

            INSERT  
            INTO
                TABLE
                ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t05           SELECT
                    EDW4.instance_id,
                    EDW4.item_instance_id,
                    EDW4.active_end_date,
                    EDW4.active_start_date,
                    EDW4.actual_return_date,
                    CAS.oper_unit_country_code,
                    CAS.cs_fml_organization_code,
                    EDW4.external_reference_nbr,
                    EDW4.install_date,
                    EDW4.install_loc_oper_unit_id,
                    EDW4.install_location_id,
                    EDW4.install_location_type_code,
                    EDW4.inventory_item_id,
                    EDW4.inventory_master_org_id,
                    EDW4.item_instance_status_desc,
                    EDW4.item_instance_status_id,
                    EDW4.item_instance_status_name,
                    EDW4.item_instance_tran_code,
                    EDW4.last_order_line_id,
                    EDW4.last_valid_invtry_org_id,
                    EDW4.location_id,
                    EDW4.location_type_code,
                    EDW4.return_by_date,
                    EDW4.service_order_allowed_flag,
                    EDW4.status_tran_code,
                    EDW4.terminated_flag,
                    EDW4.version_label_date,
                    EDW4.version_label_desc,
                    EDW4.version_label_name,
                    EDW4.vrsn_lbl_tran_code,
                    EDW4.prev_site_nbr,
                    EDW4.service_tier_name,
                    EDW4.esd_flag,
                    EDW4.media_type,
                    EDW4.license_model,
                    EDW4.license_start_date,
                    EDW4.license_end_date,
                EDW4.cit_vendor_code,
                EDW4.mdm_product_identifier,
                EDW4.mdm_solution_identifier
 
                FROM
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t04 AS EDW4  
                LEFT OUTER  JOIN
                    ${EEDDWWDDBB1}.cust_account_site    AS CAS  
                        ON EDW4.instance_id = CAS.instance_id  
                        AND EDW4.location_id = CAS.party_site_id and location_type_code = 'HZ_PARTY_SITES' ;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t02 ALL

--Translated Query: SUCCESS W8c

   --- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t02;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_06.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t06 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t06;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.sales_order_sys_parm FOR ACCESS      LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t05 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t06(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,country_code     ,crnt_loc_cs_fml_org_code     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,return_by_date     ,service_order_allowed_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     ,prev_site_nbr     ,service_tier_name,    esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW5.instance_id     ,EDW5.item_instance_id     ,EDW5.active_end_date     ,EDW5.active_start_date     ,EDW5.actual_return_date     ,EDW5.country_code     ,EDW5.crnt_loc_cs_fml_org_code     ,EDW5.external_reference_nbr     ,EDW5.install_date     ,EDW5.install_loc_oper_unit_id     ,EDW5.install_location_id     ,EDW5.install_location_type_code     ,EDW5.inventory_item_id     ,EDW5.inventory_master_org_id     ,EDW5.item_instance_status_desc     ,EDW5.item_instance_status_id     ,EDW5.item_instance_status_name     ,EDW5.item_instance_tran_code     ,EDW5.last_order_line_id     ,EDW5.last_valid_invtry_org_id     ,EDW5.location_id     ,EDW5.location_type_code     ,EDW5.return_by_date     ,EDW5.service_order_allowed_flag     ,EDW5.status_tran_code     ,EDW5.terminated_flag     ,EDW5.version_label_date     ,EDW5.version_label_desc     ,EDW5.version_label_name     <WM_COMMENT_START> vldtn_inventory_org_id <WM_COMMENT_END>     ,SOSP.inventory_org_id     ,EDW5.vrsn_lbl_tran_code     ,EDW5.prev_site_nbr     ,EDW5.service_tier_name,   EDW5.esd_flag,                        EDW5.media_type,                      EDW5.license_model,                   EDW5.license_start_date,              EDW5.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t05 EDW5LEFT OUTER JOIN${EEDDWWDDBB1}.sales_order_sys_parm SOSPON  EDW5.instance_id = SOSP.instance_idAND EDW5.install_loc_oper_unit_id = SOSP.operating_unit_id

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t06           SELECT
            EDW5.instance_id,
            EDW5.item_instance_id,
            EDW5.active_end_date,
            EDW5.active_start_date,
            EDW5.actual_return_date,
            EDW5.country_code,
            EDW5.crnt_loc_cs_fml_org_code,
            EDW5.external_reference_nbr,
            EDW5.install_date,
            EDW5.install_loc_oper_unit_id,
            EDW5.install_location_id,
            EDW5.install_location_type_code,
            EDW5.inventory_item_id,
            EDW5.inventory_master_org_id,
            EDW5.item_instance_status_desc,
            EDW5.item_instance_status_id,
            EDW5.item_instance_status_name,
            EDW5.item_instance_tran_code,
            EDW5.last_order_line_id,
            EDW5.last_valid_invtry_org_id,
            EDW5.location_id,
            EDW5.location_type_code,
            EDW5.return_by_date,
            EDW5.service_order_allowed_flag,
            EDW5.status_tran_code,
            EDW5.terminated_flag,
            EDW5.version_label_date,
            EDW5.version_label_desc,
            EDW5.version_label_name,
            SOSP.inventory_org_id,
            EDW5.vrsn_lbl_tran_code,
            EDW5.prev_site_nbr,
            EDW5.service_tier_name,
            EDW5.esd_flag,
            EDW5.media_type,
            EDW5.license_model,
            EDW5.license_start_date,
            EDW5.license_end_date,
            EDW5.cit_vendor_code,
            EDW5.mdm_product_identifier,
            EDW5.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t05    AS EDW5   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB1}.sales_order_sys_parm    AS SOSP    
                ON EDW5.instance_id = SOSP.instance_id  
                AND EDW5.install_loc_oper_unit_id = SOSP.operating_unit_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t03 ALL

--Translated Query: SUCCESS W8c

    ----TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t03;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_07.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t07 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07;

--Original Query:
  ----COLLECT STAT ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t06  COLUMN last_order_line_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t06 COMPUTE STATISTICS  FOR COLUMNS last_order_line_id;

--Original Query:
  ----COLLECT STAT ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t06        COLUMN instance_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t06 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  ----COLLECT STAT ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t06

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t06 COMPUTE STATISTICS;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.sales_order_line FOR ACCESS          LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t06 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t07(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,country_code     ,crnt_loc_cs_fml_org_code     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,order_header_id     ,order_line_nbr     ,return_by_date     ,service_order_allowed_flag     ,ship_to_site_use_id     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     ,prev_site_nbr     ,service_tier_name,   esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW6.instance_id     ,EDW6.item_instance_id     ,EDW6.active_end_date     ,EDW6.active_start_date     ,EDW6.actual_return_date     ,CAST(SOL.actual_ship_date_time AS DATE)     <WM_COMMENT_START> bill_to_site_use_id <WM_COMMENT_END>     ,SOL.invoice_to_site_use_id     ,EDW6.country_code     ,EDW6.crnt_loc_cs_fml_org_code     ,EDW6.external_reference_nbr     ,EDW6.install_date     ,EDW6.install_loc_oper_unit_id     ,EDW6.install_location_id     ,EDW6.install_location_type_code     ,EDW6.inventory_item_id     ,EDW6.inventory_master_org_id     ,EDW6.item_instance_status_desc     ,EDW6.item_instance_status_id     ,EDW6.item_instance_status_name     ,EDW6.item_instance_tran_code     ,EDW6.last_order_line_id     ,EDW6.last_valid_invtry_org_id     ,EDW6.location_id     ,EDW6.location_type_code     ,SOL.order_header_id     ,SOL.order_line_nbr     ,EDW6.return_by_date     ,EDW6.service_order_allowed_flag     ,SOL.ship_to_site_use_id     ,EDW6.status_tran_code     ,EDW6.terminated_flag     ,EDW6.version_label_date     ,EDW6.version_label_desc     ,EDW6.version_label_name     ,EDW6.vldtn_inventory_org_id     ,EDW6.vrsn_lbl_tran_code     ,EDW6.prev_site_nbr     ,EDW6.service_tier_name,     EDW6.esd_flag,                        EDW6.media_type,                      EDW6.license_model,                   EDW6.license_start_date,              EDW6.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t06 EDW6,${EEDDWWDDBB1}.sales_order_line SOLWHERE  EDW6.instance_id = SOL.instance_idAND    EDW6.last_order_line_id = SOL.order_line_idAND EDW6.last_order_line_id > ''

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07           SELECT
            EDW6.instance_id,
            EDW6.item_instance_id,
            EDW6.active_end_date,
            EDW6.active_start_date,
            EDW6.actual_return_date,
            (case when EDW6.instance_id = SOL.instance_id and  EDW6.last_order_line_id = SOL.order_line_id then CAST (SOL.actual_ship_date_time AS date) else NULL end) AS auto_c05,
            (case when EDW6.instance_id = SOL.instance_id and  EDW6.last_order_line_id = SOL.order_line_id then SOL.invoice_to_site_use_id else null end),
            EDW6.country_code,
            EDW6.crnt_loc_cs_fml_org_code,
            EDW6.external_reference_nbr,
            EDW6.install_date,
            EDW6.install_loc_oper_unit_id,
            EDW6.install_location_id,
            EDW6.install_location_type_code,
            EDW6.inventory_item_id,
            EDW6.inventory_master_org_id,
            EDW6.item_instance_status_desc,
            EDW6.item_instance_status_id,
            EDW6.item_instance_status_name,
            EDW6.item_instance_tran_code,
            EDW6.last_order_line_id,
            EDW6.last_valid_invtry_org_id,
            EDW6.location_id,
            EDW6.location_type_code,
            (case when EDW6.instance_id = SOL.instance_id and  EDW6.last_order_line_id = SOL.order_line_id then SOL.order_header_id else null end),
            (case when EDW6.instance_id = SOL.instance_id and  EDW6.last_order_line_id = SOL.order_line_id then SOL.order_line_nbr else null end),
            EDW6.return_by_date,
            EDW6.service_order_allowed_flag,
            (case when EDW6.instance_id = SOL.instance_id and  EDW6.last_order_line_id = SOL.order_line_id then SOL.ship_to_site_use_id else null end),
            EDW6.status_tran_code,
            EDW6.terminated_flag,
            EDW6.version_label_date,
            EDW6.version_label_desc,
            EDW6.version_label_name,
            EDW6.vldtn_inventory_org_id,
            EDW6.vrsn_lbl_tran_code,
            EDW6.prev_site_nbr,
            EDW6.service_tier_name,
            EDW6.esd_flag,
            EDW6.media_type,
            EDW6.license_model,
            EDW6.license_start_date,
            EDW6.license_end_date,
        EDW6.cit_vendor_code,        
        EDW6.mdm_product_identifier, 
        EDW6.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t06    AS EDW6   
        LEFT OUTER JOIN ${EEDDWWDDBB1}.sales_order_line    AS SOL   
        on
            EDW6.instance_id = SOL.instance_id  
            AND EDW6.last_order_line_id = SOL.order_line_id   ;

--Original Query:
  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t07(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,country_code     ,crnt_loc_cs_fml_org_code     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,order_header_id     ,order_line_nbr     ,return_by_date     ,service_order_allowed_flag     ,ship_to_site_use_id     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     ,prev_site_nbr     ,service_tier_name,    esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW6.instance_id     ,EDW6.item_instance_id     ,EDW6.active_end_date     ,EDW6.active_start_date     ,EDW6.actual_return_date     ,NULL     <WM_COMMENT_START> bill_to_site_use_id <WM_COMMENT_END>     ,NULL     ,EDW6.country_code     ,EDW6.crnt_loc_cs_fml_org_code     ,EDW6.external_reference_nbr     ,EDW6.install_date     ,EDW6.install_loc_oper_unit_id     ,EDW6.install_location_id     ,EDW6.install_location_type_code     ,EDW6.inventory_item_id     ,EDW6.inventory_master_org_id     ,EDW6.item_instance_status_desc     ,EDW6.item_instance_status_id     ,EDW6.item_instance_status_name     ,EDW6.item_instance_tran_code     ,EDW6.last_order_line_id     ,EDW6.last_valid_invtry_org_id     ,EDW6.location_id     ,EDW6.location_type_code     ,NULL     ,NULL     ,EDW6.return_by_date     ,EDW6.service_order_allowed_flag     ,NULL     ,EDW6.status_tran_code     ,EDW6.terminated_flag     ,EDW6.version_label_date     ,EDW6.version_label_desc     ,EDW6.version_label_name     ,EDW6.vldtn_inventory_org_id     ,EDW6.vrsn_lbl_tran_code     ,EDW6.prev_site_nbr     ,EDW6.service_tier_name,  EDW6.esd_flag,                        EDW6.media_type,                      EDW6.license_model,                   EDW6.license_start_date,              EDW6.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t06 EDW6WHERE  EDW6.last_order_line_id =''

--Translated Query: SUCCESS

--    INSERT 
--    INTO
--        TABLE
--        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07           SELECT
--            EDW6.instance_id,
--            EDW6.item_instance_id,
--            EDW6.active_end_date,
--            EDW6.active_start_date,
--            EDW6.actual_return_date,
--            CAST( NULL AS STRING ),
--            CAST( NULL AS STRING ),
--            EDW6.country_code,
--            EDW6.crnt_loc_cs_fml_org_code,
--            EDW6.external_reference_nbr,
--            EDW6.install_date,
--            EDW6.install_loc_oper_unit_id,
--            EDW6.install_location_id,
--            EDW6.install_location_type_code,
--            EDW6.inventory_item_id,
--            EDW6.inventory_master_org_id,
--            EDW6.item_instance_status_desc,
--            EDW6.item_instance_status_id,
--            EDW6.item_instance_status_name,
--            EDW6.item_instance_tran_code,
--            EDW6.last_order_line_id,
--            EDW6.last_valid_invtry_org_id,
--            EDW6.location_id,
--            EDW6.location_type_code,
--            CAST( NULL AS STRING ),
--            CAST( NULL AS STRING ),
--            EDW6.return_by_date,
--            EDW6.service_order_allowed_flag,
--            CAST( NULL AS STRING ),
--            EDW6.status_tran_code,
--            EDW6.terminated_flag,
--            EDW6.version_label_date,
--            EDW6.version_label_desc,
--            EDW6.version_label_name,
--            EDW6.vldtn_inventory_org_id,
--            EDW6.vrsn_lbl_tran_code,
--            EDW6.prev_site_nbr,
--            EDW6.service_tier_name,
--            EDW6.esd_flag,
--            EDW6.media_type,
--            EDW6.license_model,
--            EDW6.license_start_date,
--            EDW6.license_end_date,
--            EDW6.cit_vendor_code,
--            EDW6.mdm_product_identifier,
--            EDW6.mdm_solution_identifier
--        FROM
--            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t06    AS EDW6   
--        WHERE
--            EDW6.last_order_line_id is null;
--
----Original Query:
--  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t07(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,country_code     ,crnt_loc_cs_fml_org_code     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,order_header_id     ,order_line_nbr     ,return_by_date     ,service_order_allowed_flag     ,ship_to_site_use_id     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     ,prev_site_nbr     ,service_tier_name,  esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date                )          SELECT           EDW6.instance_id          ,EDW6.item_instance_id          ,EDW6.active_end_date          ,EDW6.active_start_date          ,EDW6.actual_return_date           <WM_COMMENT_START> actual_ship_date_time <WM_COMMENT_END>          ,NULL          <WM_COMMENT_START> bill_to_site_use_id <WM_COMMENT_END>          ,NULL          ,EDW6.country_code          ,EDW6.crnt_loc_cs_fml_org_code          ,EDW6.external_reference_nbr          ,EDW6.install_date          ,EDW6.install_loc_oper_unit_id          ,EDW6.install_location_id          ,EDW6.install_location_type_code          ,EDW6.inventory_item_id          ,EDW6.inventory_master_org_id          ,EDW6.item_instance_status_desc          ,EDW6.item_instance_status_id          ,EDW6.item_instance_status_name          ,EDW6.item_instance_tran_code          ,EDW6.last_order_line_id          ,EDW6.last_valid_invtry_org_id          ,EDW6.location_id          ,EDW6.location_type_code           <WM_COMMENT_START> order_header_id <WM_COMMENT_END>          ,NULL           <WM_COMMENT_START> order_line_nbr <WM_COMMENT_END>          ,NULL          ,EDW6.return_by_date          ,EDW6.service_order_allowed_flag           <WM_COMMENT_START> ship_to_site_use_id <WM_COMMENT_END>          ,NULL          ,EDW6.status_tran_code          ,EDW6.terminated_flag          ,EDW6.version_label_date          ,EDW6.version_label_desc          ,EDW6.version_label_name          ,EDW6.vldtn_inventory_org_id          ,EDW6.vrsn_lbl_tran_code          ,EDW6.prev_site_nbr          ,EDW6.service_tier_name,        EDW6.esd_flag,                             EDW6.media_type,                           EDW6.license_model,                        EDW6.license_start_date,                   EDW6.license_end_date                FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t06 EDW6     WHERE (instance_id,            item_instance_id) NOT IN           (SELECT instance_id,                   item_instance_id            FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t07)
--
----Translated Query: SUCCESS
--
--    WITH qq1 AS (    SELECT
--        EDW6.instance_id,
--        EDW6.item_instance_id,
--        EDW6.active_end_date,
--        EDW6.active_start_date,
--        EDW6.actual_return_date,
--        CAST( NULL AS STRING ),
--        CAST( NULL AS STRING ),
--        EDW6.country_code,
--        EDW6.crnt_loc_cs_fml_org_code,
--        EDW6.external_reference_nbr,
--        EDW6.install_date,
--        EDW6.install_loc_oper_unit_id,
--        EDW6.install_location_id,
--        EDW6.install_location_type_code,
--        EDW6.inventory_item_id,
--        EDW6.inventory_master_org_id,
--        EDW6.item_instance_status_desc,
--        EDW6.item_instance_status_id,
--        EDW6.item_instance_status_name,
--        EDW6.item_instance_tran_code,
--        EDW6.last_order_line_id,
--        EDW6.last_valid_invtry_org_id,
--        EDW6.location_id,
--        EDW6.location_type_code,
--        CAST( NULL AS STRING ),
--        CAST( NULL AS STRING ),
--        EDW6.return_by_date,
--        EDW6.service_order_allowed_flag,
--        CAST( NULL AS STRING ),
--        EDW6.status_tran_code,
--        EDW6.terminated_flag,
--        EDW6.version_label_date,
--        EDW6.version_label_desc,
--        EDW6.version_label_name,
--        EDW6.vldtn_inventory_org_id,
--        EDW6.vrsn_lbl_tran_code,
--        EDW6.prev_site_nbr,
--        EDW6.service_tier_name,
--        EDW6.esd_flag,
--        EDW6.media_type,
--        EDW6.license_model,
--        EDW6.license_start_date,
--        EDW6.license_end_date,
--        EDW6.cit_vendor_code,
--        EDW6.mdm_product_identifier,
--        EDW6.mdm_solution_identifier
--    FROM
--        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t06    AS EDW6 
--    LEFT OUTER JOIN
--        (SELECT
--            DISTINCT instance_id AS auto_c00,
--            item_instance_id AS auto_c01  
--        FROM
--            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07 ) AS autoAlias_40 
--            ON ( upper(trim(instance_id)) = upper(trim(autoAlias_40.AUTO_C00)) 
--            AND item_instance_id = autoAlias_40.AUTO_C01 ) 
--    WHERE
--        autoAlias_40.AUTO_C00 IS  NULL  
--        AND autoAlias_40.AUTO_C01 IS  NULL         ) INSERT 
--        INTO
--            TABLE
--            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07       SELECT
--                * 
--            FROM
--                qq1;
--
----Original Query:
--  ----COLLECT STATISTICS COLUMN (INSTANCE_ID ,ORDER_HEADER_ID) ON ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07
--
----Translated Query: SUCCESS
--
--    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07 COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,ORDER_HEADER_ID;
--
----Original Query:
--  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t04 ALL
--
----Translated Query: SUCCESS W8c
--
------TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t04;
--
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_08.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t08 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t08;

--Original Query:
  ----COLLECT STAT ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t07  COLUMN instance_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  ----COLLECT STAT ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t07  COLUMN order_header_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07 COMPUTE STATISTICS  FOR COLUMNS order_header_id;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.sales_order FOR ACCESS                LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t07 FOR ACCESS INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t08(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,country_code     ,crnt_loc_cs_fml_org_code     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,order_header_id     ,order_line_nbr     ,return_by_date     ,service_order_allowed_flag     ,ship_to_site_use_id     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     ,prev_site_nbr     ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW7.instance_id     ,EDW7.item_instance_id     ,EDW7.active_end_date     ,EDW7.active_start_date     ,EDW7.actual_return_date     ,EDW7.actual_ship_date     ,EDW7.bill_to_site_use_id     ,EDW7.country_code     ,EDW7.crnt_loc_cs_fml_org_code     ,EDW7.external_reference_nbr     ,EDW7.install_date     ,EDW7.install_loc_oper_unit_id     ,EDW7.install_location_id     ,EDW7.install_location_type_code     ,EDW7.inventory_item_id     ,EDW7.inventory_master_org_id     ,EDW7.item_instance_status_desc     ,EDW7.item_instance_status_id     ,EDW7.item_instance_status_name     ,EDW7.item_instance_tran_code     ,EDW7.last_order_line_id     ,EDW7.last_valid_invtry_org_id     ,EDW7.location_id     ,EDW7.location_type_code     ,EDW7.order_header_id     ,EDW7.order_line_nbr     ,EDW7.return_by_date     ,EDW7.service_order_allowed_flag     ,EDW7.ship_to_site_use_id     ,EDW7.status_tran_code     ,EDW7.terminated_flag     ,EDW7.version_label_date     ,EDW7.version_label_desc     ,EDW7.version_label_name     ,EDW7.vldtn_inventory_org_id     ,EDW7.vrsn_lbl_tran_code     ,EDW7.prev_site_nbr     ,EDW7.service_tier_name,  EDW7.esd_flag,                        EDW7.media_type,                      EDW7.license_model,                   EDW7.license_start_date,              EDW7.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t07 EDW7WHERE EDW7.order_header_id IS NULL

--Translated Query: SUCCESS

----INSERT 
----INTO
----    TABLE
----    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t08           SELECT
----        EDW7.instance_id,
----        EDW7.item_instance_id,
----        EDW7.active_end_date,
----        EDW7.active_start_date,
----        EDW7.actual_return_date,
----        EDW7.actual_ship_date,
----        EDW7.bill_to_site_use_id,
----        EDW7.country_code,
----        EDW7.crnt_loc_cs_fml_org_code,
----        null,
----        null,
----        EDW7.external_reference_nbr,
----        EDW7.install_date,
----        EDW7.install_loc_oper_unit_id,
----        EDW7.install_location_id,
----        EDW7.install_location_type_code,
----        EDW7.inventory_item_id,
----        EDW7.inventory_master_org_id,
----        EDW7.item_instance_status_desc,
----        EDW7.item_instance_status_id,
----        EDW7.item_instance_status_name,
----        EDW7.item_instance_tran_code,
----        EDW7.last_order_line_id,
----        EDW7.last_valid_invtry_org_id,
----        EDW7.location_id,
----        EDW7.location_type_code,
----        null,
----        EDW7.order_header_id,
----        EDW7.order_line_nbr,
----        null,
----        EDW7.return_by_date,
----        EDW7.service_order_allowed_flag,
----        EDW7.ship_to_site_use_id,
----        EDW7.status_tran_code,
----        EDW7.terminated_flag,
----        EDW7.version_label_date,
----        EDW7.version_label_desc,
----        EDW7.version_label_name,
----        EDW7.vldtn_inventory_org_id,
----        EDW7.vrsn_lbl_tran_code,
----        EDW7.prev_site_nbr,
----        EDW7.service_tier_name,
----        EDW7.esd_flag,
----        EDW7.media_type,
----        EDW7.license_model,
----        EDW7.license_start_date,
----        EDW7.license_end_date,
----    EDW7.cit_vendor_code,        
----    EDW7.mdm_product_identifier, 
----    EDW7.mdm_solution_identifier
----    FROM
----        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07    AS EDW7   
----    WHERE
----        EDW7.order_header_id  IS NULL;

--Original Query:
  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t08(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,return_by_date     ,service_order_allowed_flag     ,ship_to_site_use_id     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     ,prev_site_nbr     ,service_tier_name,  esd_flag,                        media_type,                      license_model,                   license_start_date,              license_end_date           )SELECT      EDW7.instance_id     ,EDW7.item_instance_id     ,EDW7.active_end_date     ,EDW7.active_start_date     ,EDW7.actual_return_date     ,EDW7.actual_ship_date     ,EDW7.bill_to_site_use_id     ,EDW7.country_code     ,EDW7.crnt_loc_cs_fml_org_code     ,SO.customer_po_date     ,SO.customer_po_nbr     ,EDW7.external_reference_nbr     ,EDW7.install_date     ,EDW7.install_loc_oper_unit_id     ,EDW7.install_location_id     ,EDW7.install_location_type_code     ,EDW7.inventory_item_id     ,EDW7.inventory_master_org_id     ,EDW7.item_instance_status_desc     ,EDW7.item_instance_status_id     ,EDW7.item_instance_status_name     ,EDW7.item_instance_tran_code     ,EDW7.last_order_line_id     ,EDW7.last_valid_invtry_org_id     ,EDW7.location_id     ,EDW7.location_type_code     ,CAST(SO.order_date_time AS DATE)     ,EDW7.order_header_id     ,EDW7.order_line_nbr     ,SO.order_nbr     ,EDW7.return_by_date     ,EDW7.service_order_allowed_flag     ,EDW7.ship_to_site_use_id     ,EDW7.status_tran_code     ,EDW7.terminated_flag     ,EDW7.version_label_date     ,EDW7.version_label_desc     ,EDW7.version_label_name     ,EDW7.vldtn_inventory_org_id     ,EDW7.vrsn_lbl_tran_code     ,EDW7.prev_site_nbr     ,EDW7.service_tier_name,     EDW7.esd_flag,                        EDW7.media_type,                      EDW7.license_model,                   EDW7.license_start_date,              EDW7.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t07 EDW7LEFT OUTER JOIN${EEDDWWDDBB1}.sales_order SOON  EDW7.instance_id = SO.instance_idAND EDW7.order_header_id = SO.order_header_idAND EDW7.order_header_id IS NOT NULL

--Translated Query: SUCCESS
   INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t08           SELECT
            EDW7.instance_id,
            EDW7.item_instance_id,
            EDW7.active_end_date,
            EDW7.active_start_date,
            EDW7.actual_return_date,
            EDW7.actual_ship_date,
            EDW7.bill_to_site_use_id,
            EDW7.country_code,
            EDW7.crnt_loc_cs_fml_org_code,
            ( case when EDW7.instance_id = SO.instance_id and  EDW7.order_header_id = SO.order_header_id  then SO.customer_po_date else null end),
            (case when EDW7.instance_id = SO.instance_id and  EDW7.order_header_id = SO.order_header_id  then  SO.customer_po_nbr else null end),
            EDW7.external_reference_nbr,
            EDW7.install_date,
            EDW7.install_loc_oper_unit_id,
            EDW7.install_location_id,
            EDW7.install_location_type_code,
            EDW7.inventory_item_id,
            EDW7.inventory_master_org_id,
            EDW7.item_instance_status_desc,
            EDW7.item_instance_status_id,
            EDW7.item_instance_status_name,
            EDW7.item_instance_tran_code,
            EDW7.last_order_line_id,
            EDW7.last_valid_invtry_org_id,
            EDW7.location_id,
            EDW7.location_type_code,
            (case when EDW7.instance_id = SO.instance_id and  EDW7.order_header_id = SO.order_header_id  then  CAST (SO.order_date_time AS date) else null end),
            EDW7.order_header_id,
            EDW7.order_line_nbr,
            (case when EDW7.instance_id = SO.instance_id and  EDW7.order_header_id = SO.order_header_id  then  SO.order_nbr else null end),
            EDW7.return_by_date,
            EDW7.service_order_allowed_flag,
            EDW7.ship_to_site_use_id,
            EDW7.status_tran_code,
            EDW7.terminated_flag,
            EDW7.version_label_date,
            EDW7.version_label_desc,
            EDW7.version_label_name,
            EDW7.vldtn_inventory_org_id,
            EDW7.vrsn_lbl_tran_code,
            EDW7.prev_site_nbr,
            EDW7.service_tier_name,
            EDW7.esd_flag,
            EDW7.media_type,
            EDW7.license_model,
            EDW7.license_start_date,
            EDW7.license_end_date,
        EDW7.cit_vendor_code,        
        EDW7.mdm_product_identifier, 
            EDW7.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07    AS EDW7   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB1}.sales_order    AS SO    
                ON EDW7.instance_id = SO.instance_id  
                AND EDW7.order_header_id = SO.order_header_id   ;
--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t05 ALL

--Translated Query: SUCCESS w8c

    -----TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t05;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t05 ALL

--Translated Query: SUCCESS W8c

   ----- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t05;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_09.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t09 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t09;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.invtry_item FOR ACCESS               LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t08 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t09(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_status_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,offering_acctg_type_code     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr     ,service_tier_name,    esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW8.instance_id     ,EDW8.item_instance_id     ,EDW8.active_end_date     ,EDW8.active_start_date     ,EDW8.actual_return_date     ,EDW8.actual_ship_date     ,EDW8.bill_to_site_use_id     ,II.bom_enabled_flag     ,EDW8.country_code     ,EDW8.crnt_loc_cs_fml_org_code     ,EDW8.customer_po_date     ,EDW8.customer_po_nbr     ,EDW8.external_reference_nbr     ,EDW8.install_date     ,EDW8.install_loc_oper_unit_id     ,EDW8.install_location_id     ,EDW8.install_location_type_code     ,EDW8.inventory_item_id     ,EDW8.inventory_master_org_id     ,II.invtry_item_desc     ,II.invtry_item_flag     ,II.invtry_item_status_code     <WM_COMMENT_START> invtry_pass_nbr <WM_COMMENT_END>     ,1     <WM_COMMENT_START> invtry_tran_code <WM_COMMENT_END>     ,II.tran_code     <WM_COMMENT_START> invtry_uom_code <WM_COMMENT_END>     ,II.unit_of_measure_code     ,EDW8.item_instance_status_desc     ,EDW8.item_instance_status_id     ,EDW8.item_instance_status_name     ,EDW8.item_instance_tran_code     ,II.item_type_name     ,EDW8.last_order_line_id     ,EDW8.last_valid_invtry_org_id     ,EDW8.location_id     ,EDW8.location_type_code     ,II.offering_acctg_type_code     ,EDW8.order_date     ,EDW8.order_header_id     ,EDW8.order_line_nbr     ,EDW8.order_nbr     <WM_COMMENT_START> product_class <WM_COMMENT_END>     ,SUBSTR(II.offering_id, 1, 4)     <WM_COMMENT_START> product_class_model <WM_COMMENT_END>     ,SUBSTR(II.offering_id, 1, 8)     <WM_COMMENT_START> product_id <WM_COMMENT_END>     ,II.offering_id     <WM_COMMENT_START> product_id_formatted <WM_COMMENT_END>     ,II.offering_id_hyphenated     <WM_COMMENT_START> product_model <WM_COMMENT_END>     ,SUBSTR(II.offering_id, 5, 4)     <WM_COMMENT_START> product_submodel <WM_COMMENT_END>     ,SUBSTR(II.offering_id, 9, 4)     ,EDW8.return_by_date     ,II.serial_nbr_control_code     ,EDW8.service_order_allowed_flag     ,EDW8.ship_to_site_use_id     ,II.shippable_item_flag     ,EDW8.status_tran_code     ,EDW8.terminated_flag     ,EDW8.version_label_date     ,EDW8.version_label_desc     ,EDW8.version_label_name     ,EDW8.vldtn_inventory_org_id     ,EDW8.vrsn_lbl_tran_code     ,COALESCE(II.invtry_item_desc_unc,' ')     ,EDW8.prev_site_nbr     ,EDW8.service_tier_name,   EDW8.esd_flag,                        EDW8.media_type,                      EDW8.license_model,                   EDW8.license_start_date,              EDW8.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t08 EDW8LEFT OUTER JOIN${EEDDWWDDBB1}.invtry_item IION  EDW8.instance_id = II.instance_idAND EDW8.inventory_item_id = II.inventory_item_idAND EDW8.last_valid_invtry_org_id = II.inventory_org_id

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t09           SELECT
            EDW8.instance_id,
            EDW8.item_instance_id,
            EDW8.active_end_date,
            EDW8.active_start_date,
            EDW8.actual_return_date,
            EDW8.actual_ship_date,
            EDW8.bill_to_site_use_id,
            II.bom_enabled_flag,
            EDW8.country_code,
            EDW8.crnt_loc_cs_fml_org_code,
            EDW8.customer_po_date,
            EDW8.customer_po_nbr,
            EDW8.external_reference_nbr,
            EDW8.install_date,
            EDW8.install_loc_oper_unit_id,
            EDW8.install_location_id,
            EDW8.install_location_type_code,
            EDW8.inventory_item_id,
            EDW8.inventory_master_org_id,
            II.invtry_item_desc,
            II.invtry_item_flag,
            II.invtry_item_status_code,
            1 AS auto_c022,
            II.tran_code,
            II.unit_of_measure_code,
            EDW8.item_instance_status_desc,
            EDW8.item_instance_status_id,
            EDW8.item_instance_status_name,
            EDW8.item_instance_tran_code,
            II.item_type_name,
            EDW8.last_order_line_id,
            EDW8.last_valid_invtry_org_id,
            EDW8.location_id,
            EDW8.location_type_code,
            II.offering_acctg_type_code,
            EDW8.order_date,
            EDW8.order_header_id,
            EDW8.order_line_nbr,
            EDW8.order_nbr,
            SUBSTR( II.offering_id ,
            1 ,
            4 ) AS auto_c039,
            SUBSTR( II.offering_id ,
            1 ,
            8 ) AS auto_c040,
            II.offering_id,
            II.offering_id_hyphenated,
            SUBSTR( II.offering_id ,
            5 ,
            4 ) AS auto_c043,
            SUBSTR( II.offering_id ,
            9 ,
            4 ) AS auto_c044,
            EDW8.return_by_date,
            II.serial_nbr_control_code,
            EDW8.service_order_allowed_flag,
            EDW8.ship_to_site_use_id,
            II.shippable_item_flag,
            EDW8.status_tran_code,
            EDW8.terminated_flag,
            EDW8.version_label_date,
            EDW8.version_label_desc,
            EDW8.version_label_name,
            EDW8.vldtn_inventory_org_id,
            EDW8.vrsn_lbl_tran_code,
            COALESCE( II.invtry_item_desc_unc ,
            ' ' ) AS auto_c057,
            EDW8.prev_site_nbr,
            EDW8.service_tier_name,
            EDW8.esd_flag,
            EDW8.media_type,
            EDW8.license_model,
            EDW8.license_start_date,
            EDW8.license_end_date,
        EDW8.cit_vendor_code,        
        EDW8.mdm_product_identifier, 
        EDW8.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t08    AS EDW8   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB1}.invtry_item    AS II    
                ON EDW8.instance_id = II.instance_id  
                AND EDW8.inventory_item_id = II.inventory_item_id   
                AND EDW8.last_valid_invtry_org_id = II.inventory_org_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t06 ALL

--Translated Query: SUCCESS w8c

    ---TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t06;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_10.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t10 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t10;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.invtry_item FOR ACCESS               LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t09 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t10(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,offering_acctg_type_code     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc      ,prev_site_nbr     ,service_tier_name,  esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW9.instance_id     ,EDW9.item_instance_id     ,EDW9.active_end_date     ,EDW9.active_start_date     ,EDW9.actual_return_date     ,EDW9.actual_ship_date     ,EDW9.bill_to_site_use_id     ,EDW9.bom_enabled_flag     ,EDW9.country_code     ,EDW9.crnt_loc_cs_fml_org_code     ,EDW9.customer_po_date     ,EDW9.customer_po_nbr     ,EDW9.external_reference_nbr     ,EDW9.install_date     ,EDW9.install_loc_oper_unit_id     ,EDW9.install_location_id     ,EDW9.install_location_type_code     ,EDW9.inventory_item_id     ,EDW9.inventory_master_org_id     ,EDW9.invtry_item_desc     ,EDW9.invtry_item_flag     ,II.invtry_item_sponsor_loc_id     ,EDW9.invtry_item_status_code     <WM_COMMENT_START> invtry_pass_nbr <WM_COMMENT_END>     ,2     ,EDW9.invtry_tran_code     ,EDW9.invtry_uom_code     ,EDW9.item_instance_status_desc     ,EDW9.item_instance_status_id     ,EDW9.item_instance_status_name     ,EDW9.item_instance_tran_code     ,EDW9.item_type_name     ,EDW9.last_order_line_id     ,EDW9.last_valid_invtry_org_id     ,EDW9.location_id     ,EDW9.location_type_code     ,EDW9.offering_acctg_type_code     ,EDW9.order_date     ,EDW9.order_header_id     ,EDW9.order_line_nbr     ,EDW9.order_nbr     ,EDW9.product_class     ,EDW9.product_class_model     ,EDW9.product_id     ,EDW9.product_id_formatted     ,EDW9.product_model     ,II.product_source_org_id     ,EDW9.product_submodel     ,EDW9.return_by_date     ,EDW9.serial_nbr_control_code     ,EDW9.service_order_allowed_flag     ,II.serviceable_product_flag     ,EDW9.ship_to_site_use_id     ,EDW9.shippable_item_flag     ,EDW9.status_tran_code     ,EDW9.terminated_flag     ,EDW9.version_label_date     ,EDW9.version_label_desc     ,EDW9.version_label_name     ,EDW9.vldtn_inventory_org_id     ,EDW9.vrsn_lbl_tran_code     ,EDW9.invtry_item_desc_unc     ,EDW9.prev_site_nbr     ,EDW9.service_tier_name,   EDW9.esd_flag,                        EDW9.media_type,                      EDW9.license_model,                   EDW9.license_start_date,              EDW9.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t09 EDW9LEFT OUTER JOIN${EEDDWWDDBB1}.invtry_item IION  EDW9.instance_id = II.instance_idAND EDW9.inventory_item_id = II.inventory_item_idAND EDW9.vldtn_inventory_org_id = II.inventory_org_id

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t10           SELECT
            EDW9.instance_id,
            EDW9.item_instance_id,
            EDW9.active_end_date,
            EDW9.active_start_date,
            EDW9.actual_return_date,
            EDW9.actual_ship_date,
            EDW9.bill_to_site_use_id,
            EDW9.bom_enabled_flag,
            EDW9.country_code,
            EDW9.crnt_loc_cs_fml_org_code,
            EDW9.customer_po_date,
            EDW9.customer_po_nbr,
            EDW9.external_reference_nbr,
            EDW9.install_date,
            EDW9.install_loc_oper_unit_id,
            EDW9.install_location_id,
            EDW9.install_location_type_code,
            EDW9.inventory_item_id,
            EDW9.inventory_master_org_id,
            EDW9.invtry_item_desc,
            EDW9.invtry_item_flag,
            II.invtry_item_sponsor_loc_id,
            EDW9.invtry_item_status_code,
            2 AS auto_c023,
            EDW9.invtry_tran_code,
            EDW9.invtry_uom_code,
            EDW9.item_instance_status_desc,
            EDW9.item_instance_status_id,
            EDW9.item_instance_status_name,
            EDW9.item_instance_tran_code,
            EDW9.item_type_name,
            EDW9.last_order_line_id,
            EDW9.last_valid_invtry_org_id,
            EDW9.location_id,
            EDW9.location_type_code,
            EDW9.offering_acctg_type_code,
            EDW9.order_date,
            EDW9.order_header_id,
            EDW9.order_line_nbr,
            EDW9.order_nbr,
            EDW9.product_class,
            EDW9.product_class_model,
            EDW9.product_id,
            EDW9.product_id_formatted,
            EDW9.product_model,
            II.product_source_org_id,
            EDW9.product_submodel,
            EDW9.return_by_date,
            EDW9.serial_nbr_control_code,
            EDW9.service_order_allowed_flag,
            II.serviceable_product_flag,
            EDW9.ship_to_site_use_id,
            EDW9.shippable_item_flag,
            EDW9.status_tran_code,
            EDW9.terminated_flag,
            EDW9.version_label_date,
            EDW9.version_label_desc,
            EDW9.version_label_name,
            EDW9.vldtn_inventory_org_id,
            EDW9.vrsn_lbl_tran_code,
            EDW9.invtry_item_desc_unc,
            EDW9.prev_site_nbr,
            EDW9.service_tier_name,
            EDW9.esd_flag,
            EDW9.media_type,
            EDW9.license_model,
            EDW9.license_start_date,
            EDW9.license_end_date,
        EDW9.cit_vendor_code,        
        EDW9.mdm_product_identifier, 
        EDW9.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t09    AS EDW9   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB1}.invtry_item    AS II    
                ON EDW9.instance_id = II.instance_id  
                AND EDW9.inventory_item_id = II.inventory_item_id   
                AND EDW9.vldtn_inventory_org_id = II.inventory_org_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t07 ALL

--Translated Query: SUCCESS w8c

   ---- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t07;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_11.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t11 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t11;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.invtry_item FOR ACCESS               LOCK TABLE ${EEDDWWDDBB2}.invtry_organization FOR ACCESS       LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t10 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t11(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr     ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW10.instance_id     ,EDW10.item_instance_id     ,EDW10.active_end_date     ,EDW10.active_start_date     ,EDW10.actual_return_date     ,EDW10.actual_ship_date     ,EDW10.bill_to_site_use_id     ,EDW10.bom_enabled_flag     ,EDW10.country_code     ,EDW10.crnt_loc_cs_fml_org_code     ,EDW10.customer_po_date     ,EDW10.customer_po_nbr     ,EDW10.external_reference_nbr     ,EDW10.install_date     ,EDW10.install_loc_oper_unit_id     ,EDW10.install_location_id     ,EDW10.install_location_type_code     ,EDW10.inventory_item_id     ,EDW10.inventory_master_org_id     ,EDW10.invtry_item_desc     ,EDW10.invtry_item_flag     ,EDW10.invtry_item_sponsor_loc_id     ,EDW10.invtry_item_status_code     ,EDW10.invtry_pass_nbr     ,EDW10.invtry_tran_code     ,EDW10.invtry_uom_code     ,EDW10.item_instance_status_desc     ,EDW10.item_instance_status_id     ,EDW10.item_instance_status_name     ,EDW10.item_instance_tran_code     ,EDW10.item_type_name     ,EDW10.last_order_line_id     ,EDW10.last_valid_invtry_org_id     ,EDW10.location_id     ,EDW10.location_type_code     ,II.nl_trackable_flag     ,EDW10.offering_acctg_type_code     ,EDW10.order_date     ,EDW10.order_header_id     ,EDW10.order_line_nbr     ,EDW10.order_nbr     ,EDW10.product_class     ,EDW10.product_class_model     ,EDW10.product_id     ,EDW10.product_id_formatted     ,EDW10.product_model     ,EDW10.product_source_org_id     ,EDW10.product_submodel     ,EDW10.return_by_date     ,EDW10.serial_nbr_control_code     ,EDW10.service_order_allowed_flag     ,EDW10.serviceable_product_flag     ,EDW10.ship_to_site_use_id     ,EDW10.shippable_item_flag     ,EDW10.status_tran_code     ,EDW10.terminated_flag     ,EDW10.version_label_date     ,EDW10.version_label_desc     ,EDW10.version_label_name     ,EDW10.vldtn_inventory_org_id     ,EDW10.vrsn_lbl_tran_code     ,EDW10.invtry_item_desc_unc     ,EDW10.prev_site_nbr     ,EDW10.service_tier_name,     EDW10.esd_flag,                        EDW10.media_type,                      EDW10.license_model,                   EDW10.license_start_date,              EDW10.license_end_date           FROM  (${AAPPLLIIDD1EENNVV}_install_base_dn_t10 EDW10LEFT OUTER JOIN  (${EEDDWWDDBB1}.invtry_item II   INNER JOIN  ${EEDDWWDDBB2}.invtry_organization IVO  ON II.instance_id = IVO.instance_id     AND II.inventory_org_id = IVO.inventory_org_id  AND IVO.invtry_org_name = 'Master Organization')ON  EDW10.instance_id = II.instance_idAND EDW10.inventory_item_id = II.inventory_item_id)

--Translated Query: MANUAL

with qq1 as (select II.nl_trackable_flag,II.instance_id,II.inventory_item_id from ${EEDDWWDDBB1}.invtry_item    AS II 
INNER JOIN 
(select * from ${EEDDWWDDBB2}.invtry_organization  where  upper(trim(invtry_org_name)) = 'MASTER ORGANIZATION')   AS IVO                 
                    ON upper(trim(II.instance_id)) = upper(trim(IVO.instance_id))                  
                    AND II.inventory_org_id = IVO.inventory_org_id                   
                    ) 
    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t11           SELECT
            EDW10.instance_id,
            EDW10.item_instance_id,
            EDW10.active_end_date,
            EDW10.active_start_date,
            EDW10.actual_return_date,
            EDW10.actual_ship_date,
            EDW10.bill_to_site_use_id,
            EDW10.bom_enabled_flag,
            EDW10.country_code,
            EDW10.crnt_loc_cs_fml_org_code,
            EDW10.customer_po_date,
            EDW10.customer_po_nbr,
            EDW10.external_reference_nbr,
            EDW10.install_date,
            EDW10.install_loc_oper_unit_id,
            EDW10.install_location_id,
            EDW10.install_location_type_code,
            EDW10.inventory_item_id,
            EDW10.inventory_master_org_id,
            EDW10.invtry_item_desc,
            EDW10.invtry_item_flag,
            EDW10.invtry_item_sponsor_loc_id,
            EDW10.invtry_item_status_code,
            EDW10.invtry_pass_nbr,
            EDW10.invtry_tran_code,
            EDW10.invtry_uom_code,
            EDW10.item_instance_status_desc,
            EDW10.item_instance_status_id,
            EDW10.item_instance_status_name,
            EDW10.item_instance_tran_code,
            EDW10.item_type_name,
            EDW10.last_order_line_id,
            EDW10.last_valid_invtry_org_id,
            EDW10.location_id,
            EDW10.location_type_code,
            A.nl_trackable_flag,
            EDW10.offering_acctg_type_code,
            EDW10.order_date,
            EDW10.order_header_id,
            EDW10.order_line_nbr,
            EDW10.order_nbr,
            EDW10.product_class,
            EDW10.product_class_model,
            EDW10.product_id,
            EDW10.product_id_formatted,
            EDW10.product_model,
            EDW10.product_source_org_id,
            EDW10.product_submodel,
            EDW10.return_by_date,
            EDW10.serial_nbr_control_code,
            EDW10.service_order_allowed_flag,
            EDW10.serviceable_product_flag,
            EDW10.ship_to_site_use_id,
            EDW10.shippable_item_flag,
            EDW10.status_tran_code,
            EDW10.terminated_flag,
            EDW10.version_label_date,
            EDW10.version_label_desc,
            EDW10.version_label_name,
            EDW10.vldtn_inventory_org_id,
            EDW10.vrsn_lbl_tran_code,
            EDW10.invtry_item_desc_unc,
            EDW10.prev_site_nbr,
            EDW10.service_tier_name,
            EDW10.esd_flag,
            EDW10.media_type,
            EDW10.license_model,
            EDW10.license_start_date,
            EDW10.license_end_date,
            EDW10.cit_vendor_code,        
            EDW10.mdm_product_identifier, 
            EDW10.mdm_solution_identifier
        FROM
 ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t10    AS EDW10
 LEFt OUTER JOIN
 qq1 as A
                 ON EDW10.instance_id = A.instance_id  
                AND EDW10.inventory_item_id = A.inventory_item_id   ;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t08 ALL

--Translated Query: SUCCESS W8c

   ---- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t08;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_12.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t12 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t12;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.invtry_organization FOR ACCESS        LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_dn_t11 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t12(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr     ,service_tier_name,  esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW11.instance_id     ,EDW11.item_instance_id     ,EDW11.active_end_date     ,EDW11.active_start_date     ,EDW11.actual_return_date     ,EDW11.actual_ship_date     ,EDW11.bill_to_site_use_id     ,EDW11.bom_enabled_flag     ,EDW11.country_code     ,EDW11.crnt_loc_cs_fml_org_code     ,EDW11.customer_po_date     ,EDW11.customer_po_nbr     ,EDW11.external_reference_nbr     ,EDW11.install_date     ,EDW11.install_loc_oper_unit_id     ,EDW11.install_location_id     ,EDW11.install_location_type_code     ,EDW11.inventory_item_id     ,EDW11.inventory_master_org_id     ,IVO.inventory_org_id     ,EDW11.invtry_item_desc     ,EDW11.invtry_item_flag     ,EDW11.invtry_item_sponsor_loc_id     ,EDW11.invtry_item_status_code     <WM_COMMENT_START> invtry_org_tran_code <WM_COMMENT_END>     ,IVO.tran_code     ,EDW11.invtry_pass_nbr     ,EDW11.invtry_tran_code     ,EDW11.invtry_uom_code     ,EDW11.item_instance_status_desc     ,EDW11.item_instance_status_id     ,EDW11.item_instance_status_name     ,EDW11.item_instance_tran_code     ,EDW11.item_type_name     ,EDW11.last_order_line_id     ,EDW11.last_valid_invtry_org_id     ,EDW11.location_id     ,EDW11.location_type_code     ,EDW11.nl_trackable_flag     ,EDW11.offering_acctg_type_code     ,EDW11.order_date     ,EDW11.order_header_id     ,EDW11.order_line_nbr     ,EDW11.order_nbr     ,EDW11.product_class     ,EDW11.product_class_model     ,EDW11.product_id     ,EDW11.product_id_formatted     ,EDW11.product_model     ,EDW11.product_source_org_id     ,EDW11.product_submodel     ,EDW11.return_by_date     ,EDW11.serial_nbr_control_code     ,EDW11.service_order_allowed_flag     ,EDW11.serviceable_product_flag     ,EDW11.ship_to_site_use_id     ,EDW11.shippable_item_flag     ,EDW11.status_tran_code     ,EDW11.terminated_flag     ,EDW11.version_label_date     ,EDW11.version_label_desc     ,EDW11.version_label_name     ,EDW11.vldtn_inventory_org_id     ,EDW11.vrsn_lbl_tran_code     ,EDW11.invtry_item_desc_unc     ,EDW11.prev_site_nbr     ,EDW11.service_tier_name,    EDW11.esd_flag,                        EDW11.media_type,                      EDW11.license_model,                   EDW11.license_start_date,              EDW11.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t11 EDW11LEFT OUTER JOIN ${EEDDWWDDBB2}.invtry_organization IVOON  EDW11.instance_id = IVO.instance_idAND IVO.invtry_org_name = 'MASTER Organization'

--Translated Query: MANUAL
with qq1 as (select * from ${EEDDWWDDBB2}.invtry_organization where upper(trim(invtry_org_name)) = 'MASTER ORGANIZATION' )
    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t12           SELECT
            EDW11.instance_id,
            EDW11.item_instance_id,
            EDW11.active_end_date,
            EDW11.active_start_date,
            EDW11.actual_return_date,
            EDW11.actual_ship_date,
            EDW11.bill_to_site_use_id,
            EDW11.bom_enabled_flag,
            EDW11.country_code,
            EDW11.crnt_loc_cs_fml_org_code,
            EDW11.customer_po_date,
            EDW11.customer_po_nbr,
            EDW11.external_reference_nbr,
            EDW11.install_date,
            EDW11.install_loc_oper_unit_id,
            EDW11.install_location_id,
            EDW11.install_location_type_code,
            EDW11.inventory_item_id,
            EDW11.inventory_master_org_id,
            IVO.inventory_org_id,
            EDW11.invtry_item_desc,
            EDW11.invtry_item_flag,
            EDW11.invtry_item_sponsor_loc_id,
            EDW11.invtry_item_status_code,
            IVO.tran_code,
            EDW11.invtry_pass_nbr,
            EDW11.invtry_tran_code,
            EDW11.invtry_uom_code,
            EDW11.item_instance_status_desc,
            EDW11.item_instance_status_id,
            EDW11.item_instance_status_name,
            EDW11.item_instance_tran_code,
            EDW11.item_type_name,
            EDW11.last_order_line_id,
            EDW11.last_valid_invtry_org_id,
            EDW11.location_id,
            EDW11.location_type_code,
            EDW11.nl_trackable_flag,
            EDW11.offering_acctg_type_code,
            EDW11.order_date,
            EDW11.order_header_id,
            EDW11.order_line_nbr,
            EDW11.order_nbr,
            EDW11.product_class,
            EDW11.product_class_model,
            EDW11.product_id,
            EDW11.product_id_formatted,
            EDW11.product_model,
            EDW11.product_source_org_id,
            EDW11.product_submodel,
            EDW11.return_by_date,
            EDW11.serial_nbr_control_code,
            EDW11.service_order_allowed_flag,
            EDW11.serviceable_product_flag,
            EDW11.ship_to_site_use_id,
            EDW11.shippable_item_flag,
            EDW11.status_tran_code,
            EDW11.terminated_flag,
            EDW11.version_label_date,
            EDW11.version_label_desc,
            EDW11.version_label_name,
            EDW11.vldtn_inventory_org_id,
            EDW11.vrsn_lbl_tran_code,
            EDW11.invtry_item_desc_unc,
            EDW11.prev_site_nbr,
            EDW11.service_tier_name,
            EDW11.esd_flag,
            EDW11.media_type,
            EDW11.license_model,
            EDW11.license_start_date,
            EDW11.license_end_date,
            EDW11.cit_vendor_code,        
            EDW11.mdm_product_identifier, 
            EDW11.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t11    AS EDW11   
        LEFT OUTER  JOIN
            qq1    AS IVO  
              ON EDW11.instance_id = IVO.instance_id ;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t09 ALL

--Translated Query: SUCCESS w8c

   ---- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t09;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_13.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t13 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t13;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.invtry_organization FOR ACCESS       LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t12 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t13(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,operating_unit_id     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc      ,prev_site_nbr      ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW12.instance_id     ,EDW12.item_instance_id     ,EDW12.active_end_date     ,EDW12.active_start_date     ,EDW12.actual_return_date     ,EDW12.actual_ship_date     ,EDW12.bill_to_site_use_id     ,EDW12.bom_enabled_flag     ,EDW12.country_code     ,EDW12.crnt_loc_cs_fml_org_code     ,EDW12.customer_po_date     ,EDW12.customer_po_nbr     ,EDW12.external_reference_nbr     ,EDW12.install_date     ,EDW12.install_loc_oper_unit_id     ,EDW12.install_location_id     ,EDW12.install_location_type_code     ,EDW12.inventory_item_id     ,EDW12.inventory_master_org_id     ,EDW12.invtry_item_desc     ,EDW12.invtry_item_flag     ,EDW12.invtry_item_sponsor_loc_id     ,EDW12.invtry_item_status_code     ,EDW12.invtry_org_tran_code     ,EDW12.invtry_pass_nbr     ,EDW12.invtry_tran_code     ,EDW12.invtry_uom_code     ,EDW12.item_instance_status_desc     ,EDW12.item_instance_status_id     ,EDW12.item_instance_status_name     ,EDW12.item_instance_tran_code     ,EDW12.item_type_name     ,EDW12.last_order_line_id     ,EDW12.last_valid_invtry_org_id     ,EDW12.location_id     ,EDW12.location_type_code     ,EDW12.nl_trackable_flag     ,EDW12.offering_acctg_type_code     ,IVO.operating_unit_id     ,EDW12.order_date     ,EDW12.order_header_id     ,EDW12.order_line_nbr     ,EDW12.order_nbr     ,EDW12.product_class     ,EDW12.product_class_model     ,EDW12.product_id     ,EDW12.product_id_formatted     ,EDW12.product_model     ,EDW12.product_source_org_id     ,EDW12.product_submodel     ,EDW12.return_by_date     ,EDW12.serial_nbr_control_code     ,EDW12.service_order_allowed_flag     ,EDW12.serviceable_product_flag     ,EDW12.ship_to_site_use_id     ,EDW12.shippable_item_flag     ,EDW12.status_tran_code     ,EDW12.terminated_flag     ,EDW12.version_label_date     ,EDW12.version_label_desc     ,EDW12.version_label_name     ,EDW12.vldtn_inventory_org_id     ,EDW12.vrsn_lbl_tran_code     ,EDW12.invtry_item_desc_unc     ,EDW12.prev_site_nbr         ,EDW12.service_tier_name,   EDW12.esd_flag,                        EDW12.media_type,                      EDW12.license_model,                   EDW12.license_start_date,              EDW12.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t12 EDW12LEFT OUTER JOIN ${EEDDWWDDBB2}.invtry_organization IVOON  EDW12.instance_id = IVO.instance_idAND EDW12.last_valid_invtry_org_id  = IVO.inventory_org_id

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t13           SELECT
            EDW12.instance_id,
            EDW12.item_instance_id,
            EDW12.active_end_date,
            EDW12.active_start_date,
            EDW12.actual_return_date,
            EDW12.actual_ship_date,
            EDW12.bill_to_site_use_id,
            EDW12.bom_enabled_flag,
            EDW12.country_code,
            EDW12.crnt_loc_cs_fml_org_code,
            EDW12.customer_po_date,
            EDW12.customer_po_nbr,
            EDW12.external_reference_nbr,
            EDW12.install_date,
            EDW12.install_loc_oper_unit_id,
            EDW12.install_location_id,
            EDW12.install_location_type_code,
            EDW12.inventory_item_id,
            EDW12.inventory_master_org_id,
            EDW12.invtry_item_desc,
            EDW12.invtry_item_flag,
            EDW12.invtry_item_sponsor_loc_id,
            EDW12.invtry_item_status_code,
            EDW12.invtry_org_tran_code,
            EDW12.invtry_pass_nbr,
            EDW12.invtry_tran_code,
            EDW12.invtry_uom_code,
            EDW12.item_instance_status_desc,
            EDW12.item_instance_status_id,
            EDW12.item_instance_status_name,
            EDW12.item_instance_tran_code,
            EDW12.item_type_name,
            EDW12.last_order_line_id,
            EDW12.last_valid_invtry_org_id,
            EDW12.location_id,
            EDW12.location_type_code,
            EDW12.nl_trackable_flag,
            EDW12.offering_acctg_type_code,
            IVO.operating_unit_id,
            EDW12.order_date,
            EDW12.order_header_id,
            EDW12.order_line_nbr,
            EDW12.order_nbr,
            EDW12.product_class,
            EDW12.product_class_model,
            EDW12.product_id,
            EDW12.product_id_formatted,
            EDW12.product_model,
            EDW12.product_source_org_id,
            EDW12.product_submodel,
            EDW12.return_by_date,
            EDW12.serial_nbr_control_code,
            EDW12.service_order_allowed_flag,
            EDW12.serviceable_product_flag,
            EDW12.ship_to_site_use_id,
            EDW12.shippable_item_flag,
            EDW12.status_tran_code,
            EDW12.terminated_flag,
            EDW12.version_label_date,
            EDW12.version_label_desc,
            EDW12.version_label_name,
            EDW12.vldtn_inventory_org_id,
            EDW12.vrsn_lbl_tran_code,
            EDW12.invtry_item_desc_unc,
            EDW12.prev_site_nbr,
            EDW12.service_tier_name,
            EDW12.esd_flag,
            EDW12.media_type,
            EDW12.license_model,
            EDW12.license_start_date,
            EDW12.license_end_date,
        EDW12.cit_vendor_code,        
        EDW12.mdm_product_identifier, 
        EDW12.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t12    AS EDW12   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.invtry_organization    AS IVO    
                ON EDW12.instance_id = IVO.instance_id  
                AND EDW12.last_valid_invtry_org_id = IVO.inventory_org_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t10 ALL

--Translated Query: SUCCESS w8c

   --- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t10;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_14.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t14 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t14;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.erp_operating_unit FOR ACCESS        LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t13 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t14(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,external_reference_nbr     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr      ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW13.instance_id     ,EDW13.item_instance_id     ,EDW13.active_end_date     ,EDW13.active_start_date     ,EDW13.actual_return_date     ,EDW13.actual_ship_date     ,EDW13.bill_to_site_use_id     ,EDW13.bom_enabled_flag     ,EDW13.country_code     ,EDW13.crnt_loc_cs_fml_org_code     ,EDW13.customer_po_date     ,EDW13.customer_po_nbr     ,EDW13.external_reference_nbr     ,EDW13.install_date     ,EDW13.install_loc_oper_unit_id     ,EDW13.install_location_id     ,EDW13.install_location_type_code     ,EDW13.inventory_item_id     ,EDW13.inventory_master_org_id     ,EDW13.invtry_item_desc     ,EDW13.invtry_item_flag     ,EDW13.invtry_item_sponsor_loc_id     ,EDW13.invtry_item_status_code     ,EDW13.invtry_org_tran_code     ,EDW13.invtry_pass_nbr     ,EDW13.invtry_tran_code     ,EDW13.invtry_uom_code     ,EDW13.item_instance_status_desc     ,EDW13.item_instance_status_id     ,EDW13.item_instance_status_name     ,EDW13.item_instance_tran_code     ,EDW13.item_type_name     ,EDW13.last_order_line_id     ,EDW13.last_valid_invtry_org_id     ,EDW13.location_id     ,EDW13.location_type_code     ,EDW13.nl_trackable_flag     ,EDW13.offering_acctg_type_code     ,EOU.oper_unit_country_code     ,EDW13.operating_unit_id     ,EOU.operating_unit_name     ,EDW13.order_date     ,EDW13.order_header_id     ,EDW13.order_line_nbr     ,EDW13.order_nbr     ,EDW13.product_class     ,EDW13.product_class_model     ,EDW13.product_id     ,EDW13.product_id_formatted     ,EDW13.product_model     ,EDW13.product_source_org_id     ,EDW13.product_submodel     ,EDW13.return_by_date     ,EDW13.serial_nbr_control_code     ,EDW13.service_order_allowed_flag     ,EDW13.serviceable_product_flag     ,EDW13.ship_to_site_use_id     ,EDW13.shippable_item_flag     ,EDW13.status_tran_code     ,EDW13.terminated_flag     ,EDW13.version_label_date     ,EDW13.version_label_desc     ,EDW13.version_label_name     ,EDW13.vldtn_inventory_org_id     ,EDW13.vrsn_lbl_tran_code     ,EDW13.invtry_item_desc_unc     ,EDW13.prev_site_nbr      ,EDW13.service_tier_name,   EDW13.esd_flag,                        EDW13.media_type,                      EDW13.license_model,                   EDW13.license_start_date,              EDW13.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t13 EDW13LEFT OUTER JOIN ${EEDDWWDDBB2}.erp_operating_unit EOUON  EDW13.instance_id = EOU.instance_idAND EDW13.operating_unit_id  = EOU.operating_unit_id

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t14           SELECT
            EDW13.instance_id,
            EDW13.item_instance_id,
            EDW13.active_end_date,
            EDW13.active_start_date,
            EDW13.actual_return_date,
            EDW13.actual_ship_date,
            EDW13.bill_to_site_use_id,
            EDW13.bom_enabled_flag,
            EDW13.country_code,
            EDW13.crnt_loc_cs_fml_org_code,
            EDW13.customer_po_date,
            EDW13.customer_po_nbr,
            EDW13.external_reference_nbr,
            EDW13.install_date,
            EDW13.install_loc_oper_unit_id,
            EDW13.install_location_id,
            EDW13.install_location_type_code,
            EDW13.inventory_item_id,
            EDW13.inventory_master_org_id,
            EDW13.invtry_item_desc,
            EDW13.invtry_item_flag,
            EDW13.invtry_item_sponsor_loc_id,
            EDW13.invtry_item_status_code,
            EDW13.invtry_org_tran_code,
            EDW13.invtry_pass_nbr,
            EDW13.invtry_tran_code,
            EDW13.invtry_uom_code,
            EDW13.item_instance_status_desc,
            EDW13.item_instance_status_id,
            EDW13.item_instance_status_name,
            EDW13.item_instance_tran_code,
            EDW13.item_type_name,
            EDW13.last_order_line_id,
            EDW13.last_valid_invtry_org_id,
            EDW13.location_id,
            EDW13.location_type_code,
            EDW13.nl_trackable_flag,
            EDW13.offering_acctg_type_code,
            EOU.oper_unit_country_code,
            EDW13.operating_unit_id,
            EOU.operating_unit_name,
            EDW13.order_date,
            EDW13.order_header_id,
            EDW13.order_line_nbr,
            EDW13.order_nbr,
            EDW13.product_class,
            EDW13.product_class_model,
            EDW13.product_id,
            EDW13.product_id_formatted,
            EDW13.product_model,
            EDW13.product_source_org_id,
            EDW13.product_submodel,
            EDW13.return_by_date,
            EDW13.serial_nbr_control_code,
            EDW13.service_order_allowed_flag,
            EDW13.serviceable_product_flag,
            EDW13.ship_to_site_use_id,
            EDW13.shippable_item_flag,
            EDW13.status_tran_code,
            EDW13.terminated_flag,
            EDW13.version_label_date,
            EDW13.version_label_desc,
            EDW13.version_label_name,
            EDW13.vldtn_inventory_org_id,
            EDW13.vrsn_lbl_tran_code,
            EDW13.invtry_item_desc_unc,
            EDW13.prev_site_nbr,
            EDW13.service_tier_name,
            EDW13.esd_flag,
            EDW13.media_type,
            EDW13.license_model,
            EDW13.license_start_date,
            EDW13.license_end_date,
        EDW13.cit_vendor_code,        
        EDW13.mdm_product_identifier, 
        EDW13.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t13    AS EDW13   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.erp_operating_unit    AS EOU    
                ON EDW13.instance_id = EOU.instance_id  
                AND EDW13.operating_unit_id = EOU.operating_unit_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t11 ALL

--Translated Query: SUCCESS w8c

   ----- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t11;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_15a.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t15a ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15a;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.invtry_mtl_parameter FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t15a(      instance_id     ,inventory_org_id     ,gsdb_org_code)SELECT      instance_id     ,inventory_org_id     ,gsdb_org_codeFROM  ${EEDDWWDDBB2}.invtry_mtl_parameterWHERE gsdb_org_code IS NOT NULLGROUP BY 1,2,3

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15a           SELECT
            instance_id,
            inventory_org_id,
            gsdb_org_code  
        FROM
            ${EEDDWWDDBB2}.invtry_mtl_parameter     
        WHERE
            gsdb_org_code  IS NOT NULL  
        GROUP BY
            instance_id ,
            inventory_org_id ,
            gsdb_org_code;

--Original Query:
  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t15a(      instance_id     ,inventory_org_id     ,gsdb_org_code)SELECT      IMP1.instance_id     ,IMP1.inventory_org_id     ,IMP2.gsdb_org_codeFROM ${EEDDWWDDBB2}.invtry_mtl_parameter IMP1,     ${EEDDWWDDBB2}.invtry_mtl_parameter IMP2WHERE IMP1.instance_id = IMP2.instance_idAND   IMP1.inventory_org_id = CAST(IMP2.related_gsl_sdc_org_id AS DECIMAL(18,0))AND   IMP1.gsdb_org_code IS NULL                        GROUP BY 1,2,3

--Translated Query: MANUAL

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15a           SELECT
            IMP1.instance_id,
            IMP1.inventory_org_id,
            IMP2.gsdb_org_code  
        FROM
            (select * from ${EEDDWWDDBB2}.invtry_mtl_parameter where gsdb_org_code  IS NULL)    AS IMP1   ,
            ${EEDDWWDDBB2}.invtry_mtl_parameter    AS IMP2   
        WHERE
            upper(trim(IMP1.instance_id)) = upper(trim(IMP2.instance_id))  
            AND IMP1.inventory_org_id = cast(IMP2.related_gsl_sdc_org_id as decimal(18,0))   
            
        GROUP BY
            IMP1.instance_id ,
            IMP1.inventory_org_id ,
            IMP2.gsdb_org_code;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_15b.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t15b ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15b;

--Original Query:
  ----LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t14 FOR ACCESS LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t15a FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t15b(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr       ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW14.instance_id     ,EDW14.item_instance_id     ,EDW14.active_end_date     ,EDW14.active_start_date     ,EDW14.actual_return_date     ,EDW14.actual_ship_date     ,EDW14.bill_to_site_use_id     ,EDW14.bom_enabled_flag     ,EDW14.country_code     ,EDW14.crnt_loc_cs_fml_org_code     ,EDW14.customer_po_date     ,EDW14.customer_po_nbr     ,EDW14.external_reference_nbr     ,EDW15A.gsdb_org_code     ,EDW14.install_date     ,EDW14.install_loc_oper_unit_id     ,EDW14.install_location_id     ,EDW14.install_location_type_code     ,EDW14.inventory_item_id     ,EDW14.inventory_master_org_id     ,EDW15a.inventory_org_id     ,EDW14.invtry_item_desc     ,EDW14.invtry_item_flag     ,EDW14.invtry_item_sponsor_loc_id     ,EDW14.invtry_item_status_code     ,EDW14.invtry_org_tran_code     ,EDW14.invtry_pass_nbr     ,EDW14.invtry_tran_code     ,EDW14.invtry_uom_code     ,EDW14.item_instance_status_desc     ,EDW14.item_instance_status_id     ,EDW14.item_instance_status_name     ,EDW14.item_instance_tran_code     ,EDW14.item_type_name     ,EDW14.last_order_line_id     ,EDW14.last_valid_invtry_org_id     ,EDW14.location_id     ,EDW14.location_type_code     ,EDW14.nl_trackable_flag     ,EDW14.offering_acctg_type_code     ,EDW14.oper_unit_country_code     ,EDW14.operating_unit_id     ,EDW14.operating_unit_name     ,EDW14.order_date     ,EDW14.order_header_id     ,EDW14.order_line_nbr     ,EDW14.order_nbr     ,EDW14.product_class     ,EDW14.product_class_model     ,EDW14.product_id     ,EDW14.product_id_formatted     ,EDW14.product_model     ,EDW14.product_source_org_id     ,EDW14.product_submodel     ,EDW14.return_by_date     ,EDW14.serial_nbr_control_code     ,EDW14.service_order_allowed_flag     ,EDW14.serviceable_product_flag     ,EDW14.ship_to_site_use_id     ,EDW14.shippable_item_flag     ,EDW14.status_tran_code     ,EDW14.terminated_flag     ,EDW14.version_label_date     ,EDW14.version_label_desc     ,EDW14.version_label_name     ,EDW14.vldtn_inventory_org_id     ,EDW14.vrsn_lbl_tran_code     ,EDW14.invtry_item_desc_unc     ,EDW14.prev_site_nbr       ,EDW14.service_tier_name,   EDW14.esd_flag,                        EDW14.media_type,                      EDW14.license_model,                   EDW14.license_start_date,              EDW14.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t14 EDW14LEFT OUTER JOIN${AAPPLLIIDD1EENNVV}_install_base_dn_t15a EDW15AON  EDW14.instance_id = EDW15A.instance_idAND EDW14.last_valid_invtry_org_id = EDW15A.inventory_org_id

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15b           SELECT
            EDW14.instance_id,
            EDW14.item_instance_id,
            EDW14.active_end_date,
            EDW14.active_start_date,
            EDW14.actual_return_date,
            EDW14.actual_ship_date,
            EDW14.bill_to_site_use_id,
            EDW14.bom_enabled_flag,
            EDW14.country_code,
            EDW14.crnt_loc_cs_fml_org_code,
            EDW14.customer_po_date,
            EDW14.customer_po_nbr,
            EDW14.external_reference_nbr,
            EDW15A.gsdb_org_code,
            EDW14.install_date,
            EDW14.install_loc_oper_unit_id,
            EDW14.install_location_id,
            EDW14.install_location_type_code,
            EDW14.inventory_item_id,
            EDW14.inventory_master_org_id,
            EDW15a.inventory_org_id,
            EDW14.invtry_item_desc,
            EDW14.invtry_item_flag,
            EDW14.invtry_item_sponsor_loc_id,
            EDW14.invtry_item_status_code,
            EDW14.invtry_org_tran_code,
            EDW14.invtry_pass_nbr,
            EDW14.invtry_tran_code,
            EDW14.invtry_uom_code,
            EDW14.item_instance_status_desc,
            EDW14.item_instance_status_id,
            EDW14.item_instance_status_name,
            EDW14.item_instance_tran_code,
            EDW14.item_type_name,
            EDW14.last_order_line_id,
            EDW14.last_valid_invtry_org_id,
            EDW14.location_id,
            EDW14.location_type_code,
            EDW14.nl_trackable_flag,
            EDW14.offering_acctg_type_code,
            EDW14.oper_unit_country_code,
            EDW14.operating_unit_id,
            EDW14.operating_unit_name,
            EDW14.order_date,
            EDW14.order_header_id,
            EDW14.order_line_nbr,
            EDW14.order_nbr,
            EDW14.product_class,
            EDW14.product_class_model,
            EDW14.product_id,
            EDW14.product_id_formatted,
            EDW14.product_model,
            EDW14.product_source_org_id,
            EDW14.product_submodel,
            EDW14.return_by_date,
            EDW14.serial_nbr_control_code,
            EDW14.service_order_allowed_flag,
            EDW14.serviceable_product_flag,
            EDW14.ship_to_site_use_id,
            EDW14.shippable_item_flag,
            EDW14.status_tran_code,
            EDW14.terminated_flag,
            EDW14.version_label_date,
            EDW14.version_label_desc,
            EDW14.version_label_name,
            EDW14.vldtn_inventory_org_id,
            EDW14.vrsn_lbl_tran_code,
            EDW14.invtry_item_desc_unc,
            EDW14.prev_site_nbr,
            EDW14.service_tier_name,
            EDW14.esd_flag,
            EDW14.media_type,
            EDW14.license_model,
            EDW14.license_start_date,
            EDW14.license_end_date,
        EDW14.cit_vendor_code,        
        EDW14.mdm_product_identifier, 
        EDW14.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t14    AS EDW14   
        LEFT OUTER  JOIN
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15a    AS EDW15A    
                ON EDW14.instance_id = EDW15A.instance_id  
                AND EDW14.last_valid_invtry_org_id = EDW15A.inventory_org_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t12 ALL

--Translated Query: SUCCESS w8c

    -----TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t12;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_16.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t16 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t16;

--Original Query:
  ----COLLECT STATS ${AAPPLLIIDD1EENNVV}_install_base_dn_t15b COLUMN product_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15b COMPUTE STATISTICS  FOR COLUMNS product_id;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.product FOR ACCESS                    LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t15b FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t16(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_product_id     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,sal_code     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr        ,service_tier_name,      esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW15B.instance_id     ,EDW15B.item_instance_id     ,EDW15B.active_end_date     ,EDW15B.active_start_date     ,EDW15B.actual_return_date     ,EDW15B.actual_ship_date     ,EDW15B.bill_to_site_use_id     ,EDW15B.bom_enabled_flag     ,EDW15B.country_code     ,EDW15B.crnt_loc_cs_fml_org_code     ,EDW15B.customer_po_date     ,EDW15B.customer_po_nbr     ,PROD.service_product_id     ,EDW15B.external_reference_nbr     ,EDW15B.gsdb_org_code     ,EDW15B.install_date     ,EDW15B.install_loc_oper_unit_id     ,EDW15B.install_location_id     ,EDW15B.install_location_type_code     ,EDW15B.inventory_item_id     ,EDW15B.inventory_master_org_id     ,EDW15B.inventory_org_id     ,EDW15B.invtry_item_desc     ,EDW15B.invtry_item_flag     ,EDW15B.invtry_item_sponsor_loc_id     ,EDW15B.invtry_item_status_code     ,EDW15B.invtry_org_tran_code     ,EDW15B.invtry_pass_nbr     ,EDW15B.invtry_tran_code     ,EDW15B.invtry_uom_code     ,EDW15B.item_instance_status_desc     ,EDW15B.item_instance_status_id     ,EDW15B.item_instance_status_name     ,EDW15B.item_instance_tran_code     ,EDW15B.item_type_name     ,EDW15B.last_order_line_id     ,EDW15B.last_valid_invtry_org_id     ,EDW15B.location_id     ,EDW15B.location_type_code     ,EDW15B.nl_trackable_flag     ,EDW15B.offering_acctg_type_code     ,EDW15B.oper_unit_country_code     ,EDW15B.operating_unit_id     ,EDW15B.operating_unit_name     ,EDW15B.order_date     ,EDW15B.order_header_id     ,EDW15B.order_line_nbr     ,EDW15B.order_nbr     ,EDW15B.product_class     ,EDW15B.product_class_model     ,EDW15B.product_id     ,EDW15B.product_id_formatted     ,EDW15B.product_model     ,EDW15B.product_source_org_id     ,EDW15B.product_submodel     ,EDW15B.return_by_date     ,PROD.sal_code     ,EDW15B.serial_nbr_control_code     ,EDW15B.service_order_allowed_flag     ,EDW15B.serviceable_product_flag     ,EDW15B.ship_to_site_use_id     ,EDW15B.shippable_item_flag     ,EDW15B.status_tran_code     ,EDW15B.terminated_flag     ,EDW15B.version_label_date     ,EDW15B.version_label_desc     ,EDW15B.version_label_name     ,EDW15B.vldtn_inventory_org_id     ,EDW15B.vrsn_lbl_tran_code     ,EDW15B.invtry_item_desc_unc     ,EDW15B.prev_site_nbr       ,EDW15B.service_tier_name,  EDW15B.esd_flag,                        EDW15B.media_type,                      EDW15B.license_model,                   EDW15B.license_start_date,              EDW15B.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t15b EDW15BLEFT OUTER JOIN ${EEDDWWDDBB2}.product PRODON EDW15B.product_id = PROD.product_idWHERE EDW15B.product_id IS NOT NULL

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t16           SELECT
            EDW15B.instance_id,
            EDW15B.item_instance_id,
            EDW15B.active_end_date,
            EDW15B.active_start_date,
            EDW15B.actual_return_date,
            EDW15B.actual_ship_date,
            EDW15B.bill_to_site_use_id,
            EDW15B.bom_enabled_flag,
            EDW15B.country_code,
            EDW15B.crnt_loc_cs_fml_org_code,
            EDW15B.customer_po_date,
            EDW15B.customer_po_nbr,
            ( case when EDW15B.product_id = PROD.product_id then PROD.service_product_id else null end),
            EDW15B.external_reference_nbr,
            EDW15B.gsdb_org_code,
            EDW15B.install_date,
            EDW15B.install_loc_oper_unit_id,
            EDW15B.install_location_id,
            EDW15B.install_location_type_code,
            EDW15B.inventory_item_id,
            EDW15B.inventory_master_org_id,
            EDW15B.inventory_org_id,
            EDW15B.invtry_item_desc,
            EDW15B.invtry_item_flag,
            EDW15B.invtry_item_sponsor_loc_id,
            EDW15B.invtry_item_status_code,
            EDW15B.invtry_org_tran_code,
            EDW15B.invtry_pass_nbr,
            EDW15B.invtry_tran_code,
            EDW15B.invtry_uom_code,
            EDW15B.item_instance_status_desc,
            EDW15B.item_instance_status_id,
            EDW15B.item_instance_status_name,
            EDW15B.item_instance_tran_code,
            EDW15B.item_type_name,
            EDW15B.last_order_line_id,
            EDW15B.last_valid_invtry_org_id,
            EDW15B.location_id,
            EDW15B.location_type_code,
            EDW15B.nl_trackable_flag,
            EDW15B.offering_acctg_type_code,
            EDW15B.oper_unit_country_code,
            EDW15B.operating_unit_id,
            EDW15B.operating_unit_name,
            EDW15B.order_date,
            EDW15B.order_header_id,
            EDW15B.order_line_nbr,
            EDW15B.order_nbr,
            EDW15B.product_class,
            EDW15B.product_class_model,
            EDW15B.product_id,
            EDW15B.product_id_formatted,
            EDW15B.product_model,
            EDW15B.product_source_org_id,
            EDW15B.product_submodel,
            EDW15B.return_by_date,
            ( case when EDW15B.product_id = PROD.product_id then PROD.sal_code else null end),
            EDW15B.serial_nbr_control_code,
            EDW15B.service_order_allowed_flag,
            EDW15B.serviceable_product_flag,
            EDW15B.ship_to_site_use_id,
            EDW15B.shippable_item_flag,
            EDW15B.status_tran_code,
            EDW15B.terminated_flag,
            EDW15B.version_label_date,
            EDW15B.version_label_desc,
            EDW15B.version_label_name,
            EDW15B.vldtn_inventory_org_id,
            EDW15B.vrsn_lbl_tran_code,
            EDW15B.invtry_item_desc_unc,
            EDW15B.prev_site_nbr,
            EDW15B.service_tier_name,
            EDW15B.esd_flag,
            EDW15B.media_type,
            EDW15B.license_model,
            EDW15B.license_start_date,
            EDW15B.license_end_date,
        EDW15B.cit_vendor_code,        
        EDW15B.mdm_product_identifier, 
        EDW15B.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15b    AS EDW15B   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.product    AS PROD    
                ON EDW15B.product_id = PROD.product_id ;

--Original Query:
  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t16(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_product_id     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,sal_code     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr       ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW15B.instance_id     ,EDW15B.item_instance_id     ,EDW15B.active_end_date     ,EDW15B.active_start_date     ,EDW15B.actual_return_date     ,EDW15B.actual_ship_date     ,EDW15B.bill_to_site_use_id     ,EDW15B.bom_enabled_flag     ,EDW15B.country_code     ,EDW15B.crnt_loc_cs_fml_org_code     ,EDW15B.customer_po_date     ,EDW15B.customer_po_nbr     ,NULL     ,EDW15B.external_reference_nbr     ,EDW15B.gsdb_org_code     ,EDW15B.install_date     ,EDW15B.install_loc_oper_unit_id     ,EDW15B.install_location_id     ,EDW15B.install_location_type_code     ,EDW15B.inventory_item_id     ,EDW15B.inventory_master_org_id     ,EDW15B.inventory_org_id     ,EDW15B.invtry_item_desc     ,EDW15B.invtry_item_flag     ,EDW15B.invtry_item_sponsor_loc_id     ,EDW15B.invtry_item_status_code     ,EDW15B.invtry_org_tran_code     ,EDW15B.invtry_pass_nbr     ,EDW15B.invtry_tran_code     ,EDW15B.invtry_uom_code     ,EDW15B.item_instance_status_desc     ,EDW15B.item_instance_status_id     ,EDW15B.item_instance_status_name     ,EDW15B.item_instance_tran_code     ,EDW15B.item_type_name     ,EDW15B.last_order_line_id     ,EDW15B.last_valid_invtry_org_id     ,EDW15B.location_id     ,EDW15B.location_type_code     ,EDW15B.nl_trackable_flag     ,EDW15B.offering_acctg_type_code     ,EDW15B.oper_unit_country_code     ,EDW15B.operating_unit_id     ,EDW15B.operating_unit_name     ,EDW15B.order_date     ,EDW15B.order_header_id     ,EDW15B.order_line_nbr     ,EDW15B.order_nbr     ,EDW15B.product_class     ,EDW15B.product_class_model     ,EDW15B.product_id     ,EDW15B.product_id_formatted     ,EDW15B.product_model     ,EDW15B.product_source_org_id     ,EDW15B.product_submodel     ,EDW15B.return_by_date     ,NULL     ,EDW15B.serial_nbr_control_code     ,EDW15B.service_order_allowed_flag     ,EDW15B.serviceable_product_flag     ,EDW15B.ship_to_site_use_id     ,EDW15B.shippable_item_flag     ,EDW15B.status_tran_code     ,EDW15B.terminated_flag     ,EDW15B.version_label_date     ,EDW15B.version_label_desc     ,EDW15B.version_label_name     ,EDW15B.vldtn_inventory_org_id     ,EDW15B.vrsn_lbl_tran_code     ,EDW15B.invtry_item_desc_unc     ,EDW15B.prev_site_nbr        ,EDW15B.service_tier_name,  EDW15B.esd_flag,                        EDW15B.media_type,                      EDW15B.license_model,                   EDW15B.license_start_date,              EDW15B.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t15b EDW15BWHERE EDW15B.product_id IS NULL

--Translated Query: SUCCESS

    ---INSERT 
    ---INTO
    ---    TABLE
    ---    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t16           SELECT
    ---        EDW15B.instance_id,
    ---        EDW15B.item_instance_id,
    ---        EDW15B.active_end_date,
    ---        EDW15B.active_start_date,
    ---        EDW15B.actual_return_date,
    ---        EDW15B.actual_ship_date,
    ---        EDW15B.bill_to_site_use_id,
    ---        EDW15B.bom_enabled_flag,
    ---        EDW15B.country_code,
    ---        EDW15B.crnt_loc_cs_fml_org_code,
    ---        EDW15B.customer_po_date,
    ---        EDW15B.customer_po_nbr,
    ---        CAST( NULL AS STRING ),
    ---        EDW15B.external_reference_nbr,
    ---        EDW15B.gsdb_org_code,
    ---        EDW15B.install_date,
    ---        EDW15B.install_loc_oper_unit_id,
    ---        EDW15B.install_location_id,
    ---        EDW15B.install_location_type_code,
    ---        EDW15B.inventory_item_id,
    ---        EDW15B.inventory_master_org_id,
    ---        EDW15B.inventory_org_id,
    ---        EDW15B.invtry_item_desc,
    ---        EDW15B.invtry_item_flag,
    ---        EDW15B.invtry_item_sponsor_loc_id,
    ---        EDW15B.invtry_item_status_code,
    ---        EDW15B.invtry_org_tran_code,
    ---        EDW15B.invtry_pass_nbr,
    ---        EDW15B.invtry_tran_code,
    ---        EDW15B.invtry_uom_code,
    ---        EDW15B.item_instance_status_desc,
    ---        EDW15B.item_instance_status_id,
    ---        EDW15B.item_instance_status_name,
    ---        EDW15B.item_instance_tran_code,
    ---        EDW15B.item_type_name,
    ---        EDW15B.last_order_line_id,
    ---        EDW15B.last_valid_invtry_org_id,
    ---        EDW15B.location_id,
    ---        EDW15B.location_type_code,
    ---        EDW15B.nl_trackable_flag,
    ---        EDW15B.offering_acctg_type_code,
    ---        EDW15B.oper_unit_country_code,
    ---        EDW15B.operating_unit_id,
    ---        EDW15B.operating_unit_name,
    ---        EDW15B.order_date,
    ---        EDW15B.order_header_id,
    ---        EDW15B.order_line_nbr,
    ---        EDW15B.order_nbr,
    ---        EDW15B.product_class,
    ---        EDW15B.product_class_model,
    ---        EDW15B.product_id,
    ---        EDW15B.product_id_formatted,
    ---        EDW15B.product_model,
    ---        EDW15B.product_source_org_id,
    ---        EDW15B.product_submodel,
    ---        EDW15B.return_by_date,
    ---        CAST( NULL AS STRING ),
    ---        EDW15B.serial_nbr_control_code,
    ---        EDW15B.service_order_allowed_flag,
    ---        EDW15B.serviceable_product_flag,
    ---        EDW15B.ship_to_site_use_id,
    ---        EDW15B.shippable_item_flag,
    ---        EDW15B.status_tran_code,
    ---        EDW15B.terminated_flag,
    ---        EDW15B.version_label_date,
    ---        EDW15B.version_label_desc,
    ---        EDW15B.version_label_name,
    ---        EDW15B.vldtn_inventory_org_id,
    ---        EDW15B.vrsn_lbl_tran_code,
    ---        EDW15B.invtry_item_desc_unc,
    ---        EDW15B.prev_site_nbr,
    ---        EDW15B.service_tier_name,
    ---        EDW15B.esd_flag,
    ---        EDW15B.media_type,
    ---        EDW15B.license_model,
    ---        EDW15B.license_start_date,
    ---        EDW15B.license_end_date,
    ---    EDW15B.cit_vendor_code,
    ---        EDW15B.mdm_product_identifier,
    ---        EDW15B.mdm_solution_identifier
    ---
    ---    FROM
    ---        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15b    AS EDW15B   
    ---    WHERE
    ---        EDW15B.product_id  IS NULL;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t13 ALL

--Translated Query: SUCCESS w8c

   ---- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t13;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_17.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t17 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t17;

--Original Query:
  ----LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t16 FOR ACCESSLOCK TABLE ${EEDDWWDDBB2}.product_order_org FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t17(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr       ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW16.instance_id     ,EDW16.item_instance_id     ,EDW16.active_end_date     ,EDW16.active_start_date     ,EDW16.actual_return_date     ,EDW16.actual_ship_date     ,EDW16.bill_to_site_use_id     ,EDW16.bom_enabled_flag     ,EDW16.country_code     ,EDW16.crnt_loc_cs_fml_org_code     ,EDW16.customer_po_date     ,EDW16.customer_po_nbr     ,EDW16.dflt_service_product_id     <WM_COMMENT_START> dflt_wrnty_coverage_code <WM_COMMENT_END>     ,POO.warranty_coverage_code     <WM_COMMENT_START> dflt_wrnty_product_id <WM_COMMENT_END>     ,POO.warranty_product_id     <WM_COMMENT_START> dflt_wrnty_term_mth_cnt <WM_COMMENT_END>     ,POO.warranty_term_mth_cnt     ,EDW16.external_reference_nbr     ,EDW16.gsdb_org_code     ,EDW16.install_date     ,EDW16.install_loc_oper_unit_id     ,EDW16.install_location_id     ,EDW16.install_location_type_code     ,EDW16.inventory_item_id     ,EDW16.inventory_master_org_id     ,EDW16.inventory_org_id     ,EDW16.invtry_item_desc     ,EDW16.invtry_item_flag     ,EDW16.invtry_item_sponsor_loc_id     ,EDW16.invtry_item_status_code     ,EDW16.invtry_org_tran_code     ,EDW16.invtry_pass_nbr     ,EDW16.invtry_tran_code     ,EDW16.invtry_uom_code     ,EDW16.item_instance_status_desc     ,EDW16.item_instance_status_id     ,EDW16.item_instance_status_name     ,EDW16.item_instance_tran_code     ,EDW16.item_type_name     ,EDW16.last_order_line_id     ,EDW16.last_valid_invtry_org_id     ,EDW16.location_id     ,EDW16.location_type_code     ,EDW16.nl_trackable_flag     ,EDW16.offering_acctg_type_code     ,EDW16.oper_unit_country_code     ,EDW16.operating_unit_id     ,EDW16.operating_unit_name     ,EDW16.order_date     ,EDW16.order_header_id     ,EDW16.order_line_nbr     ,EDW16.order_nbr     <WM_COMMENT_START> prod_act_order_end_date <WM_COMMENT_END>     ,POO.actual_order_end_date     <WM_COMMENT_START> prod_act_order_start_date <WM_COMMENT_END>     ,POO.actual_order_start_date     <WM_COMMENT_START> prod_act_svc_end_date <WM_COMMENT_END>     ,POO.actual_svc_end_date     <WM_COMMENT_START> prod_act_svc_start_date <WM_COMMENT_END>     ,POO.actual_svc_start_date     ,EDW16.product_class     ,EDW16.product_class_model     ,EDW16.product_id     ,EDW16.product_id_formatted     ,EDW16.product_model     ,EDW16.product_source_org_id     ,EDW16.product_submodel     ,EDW16.return_by_date     ,EDW16.serial_nbr_control_code     ,EDW16.service_order_allowed_flag     ,EDW16.serviceable_product_flag     ,EDW16.ship_to_site_use_id     ,EDW16.shippable_item_flag     ,EDW16.status_tran_code     ,EDW16.terminated_flag     ,EDW16.version_label_date     ,EDW16.version_label_desc     ,EDW16.version_label_name     ,EDW16.vldtn_inventory_org_id     ,EDW16.vrsn_lbl_tran_code     ,EDW16.invtry_item_desc_unc     ,EDW16.prev_site_nbr       ,EDW16.service_tier_name,   EDW16.esd_flag,                        EDW16.media_type,                      EDW16.license_model,                   EDW16.license_start_date,              EDW16.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t16 EDW16,${EEDDWWDDBB2}.product_order_org POOWHERE EDW16.gsdb_org_code = POO.ordering_org_codeAND   EDW16.product_id  = POO.product_idAND   EDW16.gsdb_org_code IS NOT NULL

--Translated Query: SUCCESS
    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t17           SELECT
           distinct
            EDW16.instance_id,
            EDW16.item_instance_id,
            EDW16.active_end_date,
            EDW16.active_start_date,
            EDW16.actual_return_date,
            EDW16.actual_ship_date,
            EDW16.bill_to_site_use_id,
            EDW16.bom_enabled_flag,
            EDW16.country_code,
            EDW16.crnt_loc_cs_fml_org_code,
            EDW16.customer_po_date,
            EDW16.customer_po_nbr,
            EDW16.dflt_service_product_id,
            ( case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.warranty_coverage_code else null end),
            (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.warranty_product_id else null end),
            (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.warranty_term_mth_cnt else null end),
            EDW16.external_reference_nbr,
            EDW16.gsdb_org_code,
            EDW16.install_date,
            EDW16.install_loc_oper_unit_id,
            EDW16.install_location_id,
            EDW16.install_location_type_code,
            EDW16.inventory_item_id,
            EDW16.inventory_master_org_id,
            EDW16.inventory_org_id,
            EDW16.invtry_item_desc,
            EDW16.invtry_item_flag,
            EDW16.invtry_item_sponsor_loc_id,
            EDW16.invtry_item_status_code,
            EDW16.invtry_org_tran_code,
            EDW16.invtry_pass_nbr,
            EDW16.invtry_tran_code,
            EDW16.invtry_uom_code,
            EDW16.item_instance_status_desc,
            EDW16.item_instance_status_id,
            EDW16.item_instance_status_name,
            EDW16.item_instance_tran_code,
            EDW16.item_type_name,
            EDW16.last_order_line_id,
            EDW16.last_valid_invtry_org_id,
            EDW16.location_id,
            EDW16.location_type_code,
            EDW16.nl_trackable_flag,
            EDW16.offering_acctg_type_code,
            EDW16.oper_unit_country_code,
            EDW16.operating_unit_id,
            EDW16.operating_unit_name,
            EDW16.order_date,
            EDW16.order_header_id,
            EDW16.order_line_nbr,
            EDW16.order_nbr,
            (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.actual_order_end_date else null end),
            (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.actual_order_start_date else null end),
            (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.actual_svc_end_date else null end),
            (  case when EDW16.gsdb_org_code = POO.ordering_org_code  AND EDW16.product_id = POO.product_id then POO.actual_svc_start_date else null end),
            EDW16.product_class,
            EDW16.product_class_model,
            EDW16.product_id,
            EDW16.product_id_formatted,
            EDW16.product_model,
            EDW16.product_source_org_id,
            EDW16.product_submodel,
            EDW16.return_by_date,
            null,
            EDW16.serial_nbr_control_code,
            EDW16.service_order_allowed_flag,
            EDW16.serviceable_product_flag,
            EDW16.ship_to_site_use_id,
            EDW16.shippable_item_flag,
            EDW16.status_tran_code,
            EDW16.terminated_flag,
            EDW16.version_label_date,
            EDW16.version_label_desc,
            EDW16.version_label_name,
            EDW16.vldtn_inventory_org_id,
            EDW16.vrsn_lbl_tran_code,
            EDW16.invtry_item_desc_unc,
            EDW16.prev_site_nbr,
            EDW16.service_tier_name,
            EDW16.esd_flag,
            EDW16.media_type,
            EDW16.license_model,
            EDW16.license_start_date,
            EDW16.license_end_date,
        EDW16.cit_vendor_code,
            EDW16.mdm_product_identifier,
            EDW16.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t16    AS EDW16 LEFT OUTER JOIN
            ${EEDDWWDDBB2}.product_order_org    AS POO   
        on
            EDW16.gsdb_org_code = POO.ordering_org_code  
            AND EDW16.product_id = POO.product_id ;

--Original Query:
  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t17(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc      ,prev_site_nbr        ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW16.instance_id     ,EDW16.item_instance_id     ,EDW16.active_end_date     ,EDW16.active_start_date     ,EDW16.actual_return_date     ,EDW16.actual_ship_date     ,EDW16.bill_to_site_use_id     ,EDW16.bom_enabled_flag     ,EDW16.country_code     ,EDW16.crnt_loc_cs_fml_org_code     ,EDW16.customer_po_date     ,EDW16.customer_po_nbr     ,EDW16.dflt_service_product_id     <WM_COMMENT_START> dflt_wrnty_coverage_code <WM_COMMENT_END>     ,NULL     <WM_COMMENT_START> dflt_wrnty_product_id <WM_COMMENT_END>     ,NULL     <WM_COMMENT_START> dflt_wrnty_term_mth_cnt <WM_COMMENT_END>     ,NULL     ,EDW16.external_reference_nbr     ,EDW16.gsdb_org_code     ,EDW16.install_date     ,EDW16.install_loc_oper_unit_id     ,EDW16.install_location_id     ,EDW16.install_location_type_code     ,EDW16.inventory_item_id     ,EDW16.inventory_master_org_id     ,EDW16.invtry_item_desc     ,EDW16.invtry_item_flag     ,EDW16.invtry_item_sponsor_loc_id     ,EDW16.invtry_item_status_code     ,EDW16.invtry_org_tran_code     ,EDW16.invtry_pass_nbr     ,EDW16.invtry_tran_code     ,EDW16.invtry_uom_code     ,EDW16.item_instance_status_desc     ,EDW16.item_instance_status_id     ,EDW16.item_instance_status_name     ,EDW16.item_instance_tran_code     ,EDW16.item_type_name     ,EDW16.last_order_line_id     ,EDW16.last_valid_invtry_org_id     ,EDW16.location_id     ,EDW16.location_type_code     ,EDW16.nl_trackable_flag     ,EDW16.offering_acctg_type_code     ,EDW16.oper_unit_country_code     ,EDW16.operating_unit_id     ,EDW16.operating_unit_name     ,EDW16.order_date     ,EDW16.order_header_id     ,EDW16.order_line_nbr     ,EDW16.order_nbr     <WM_COMMENT_START> prod_act_order_end_date <WM_COMMENT_END>     ,NULL     <WM_COMMENT_START> prod_act_order_start_date <WM_COMMENT_END>     ,NULL     <WM_COMMENT_START> prod_act_svc_end_date <WM_COMMENT_END>     ,NULL     <WM_COMMENT_START> prod_act_svc_start_date <WM_COMMENT_END>     ,NULL     ,EDW16.product_class     ,EDW16.product_class_model     ,EDW16.product_id     ,EDW16.product_id_formatted     ,EDW16.product_model     ,EDW16.product_source_org_id     ,EDW16.product_submodel     ,EDW16.return_by_date     ,EDW16.serial_nbr_control_code     ,EDW16.service_order_allowed_flag     ,EDW16.serviceable_product_flag     ,EDW16.ship_to_site_use_id     ,EDW16.shippable_item_flag     ,EDW16.status_tran_code     ,EDW16.terminated_flag     ,EDW16.version_label_date     ,EDW16.version_label_desc     ,EDW16.version_label_name     ,EDW16.vldtn_inventory_org_id     ,EDW16.vrsn_lbl_tran_code     ,EDW16.invtry_item_desc_unc      ,EDW16.prev_site_nbr        ,EDW16.service_tier_name,   EDW16.esd_flag,                        EDW16.media_type,                      EDW16.license_model,                   EDW16.license_start_date,              EDW16.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t16 EDW16WHERE (instance_id,        item_instance_id) NOT IN ( SELECT instance_id,     item_instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t17)

--Translated Query: SUCCESS

    ----WITH qq1 AS (    SELECT
    ----    EDW16.instance_id,
    ----    EDW16.item_instance_id,
    ----    EDW16.active_end_date,
    ----    EDW16.active_start_date,
    ----    EDW16.actual_return_date,
    ----    EDW16.actual_ship_date,
    ----    EDW16.bill_to_site_use_id,
    ----    EDW16.bom_enabled_flag,
    ----    EDW16.country_code,
    ----    EDW16.crnt_loc_cs_fml_org_code,
    ----    EDW16.customer_po_date,
    ----    EDW16.customer_po_nbr,
    ----    EDW16.dflt_service_product_id,
    ----    CAST( NULL AS STRING ),
    ----    CAST( NULL AS STRING ),
    ----    CAST( NULL AS STRING ),
    ----    EDW16.external_reference_nbr,
    ----    EDW16.gsdb_org_code,
    ----    EDW16.install_date,
    ----    EDW16.install_loc_oper_unit_id,
    ----    EDW16.install_location_id,
    ----    EDW16.install_location_type_code,
    ----    EDW16.inventory_item_id,
    ----    EDW16.inventory_master_org_id,
    ----    null,
    ----    EDW16.invtry_item_desc,
    ----    EDW16.invtry_item_flag,
    ----    EDW16.invtry_item_sponsor_loc_id,
    ----    EDW16.invtry_item_status_code,
    ----    EDW16.invtry_org_tran_code,
    ----    EDW16.invtry_pass_nbr,
    ----    EDW16.invtry_tran_code,
    ----    EDW16.invtry_uom_code,
    ----    EDW16.item_instance_status_desc,
    ----    EDW16.item_instance_status_id,
    ----    EDW16.item_instance_status_name,
    ----    EDW16.item_instance_tran_code,
    ----    EDW16.item_type_name,
    ----    EDW16.last_order_line_id,
    ----    EDW16.last_valid_invtry_org_id,
    ----    EDW16.location_id,
    ----    EDW16.location_type_code,
    ----    EDW16.nl_trackable_flag,
    ----    EDW16.offering_acctg_type_code,
    ----    EDW16.oper_unit_country_code,
    ----    EDW16.operating_unit_id,
    ----    EDW16.operating_unit_name,
    ----    EDW16.order_date,
    ----    EDW16.order_header_id,
    ----    EDW16.order_line_nbr,
    ----    EDW16.order_nbr,
    ----    CAST( NULL AS STRING ),
    ----    CAST( NULL AS STRING ),
    ----    CAST( NULL AS STRING ),
    ----    CAST( NULL AS STRING ),
    ----    EDW16.product_class,
    ----    EDW16.product_class_model,
    ----    EDW16.product_id,
    ----    EDW16.product_id_formatted,
    ----    EDW16.product_model,
    ----    EDW16.product_source_org_id,
    ----    EDW16.product_submodel,
    ----    EDW16.return_by_date,
    ----    null,
    ----    EDW16.serial_nbr_control_code,
    ----    EDW16.service_order_allowed_flag,
    ----    EDW16.serviceable_product_flag,
    ----    EDW16.ship_to_site_use_id,
    ----    EDW16.shippable_item_flag,
    ----    EDW16.status_tran_code,
    ----    EDW16.terminated_flag,
    ----    EDW16.version_label_date,
    ----    EDW16.version_label_desc,
    ----    EDW16.version_label_name,
    ----    EDW16.vldtn_inventory_org_id,
    ----    EDW16.vrsn_lbl_tran_code,
    ----    EDW16.invtry_item_desc_unc,
    ----    EDW16.prev_site_nbr,
    ----    EDW16.service_tier_name,
    ----    EDW16.esd_flag,
    ----    EDW16.media_type,
    ----    EDW16.license_model,
    ----    EDW16.license_start_date,
    ----    EDW16.license_end_date,
    ----    EDW16.cit_vendor_code,
    ----    EDW16.mdm_product_identifier,
    ----    EDW16.mdm_solution_identifier
    ----
    ----FROM
    ----    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t16    AS EDW16 
    ----LEFT OUTER JOIN
    ----    (SELECT
    ----        DISTINCT instance_id AS auto_c00,
    ----        item_instance_id AS auto_c01  
    ----    FROM
    ----        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t17 ) AS autoAlias_42 
    ----        ON ( upper(trim(instance_id)) = upper(trim(autoAlias_42.AUTO_C00)) 
    ----        AND item_instance_id = autoAlias_42.AUTO_C01 ) 
    ----WHERE
    ----    autoAlias_42.AUTO_C00 IS  NULL  
    ----    AND autoAlias_42.AUTO_C01 IS  NULL         ) INSERT 
    ----    INTO
    ----        TABLE
    ----        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t17       SELECT
    ----            * 
    ----        FROM
    ----            qq1;
    ----
--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t14 ALL

--Translated Query: SUCCESS W8c

   ---- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t14;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_18.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t18 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t18;

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t17  COLUMN gsdb_org_code

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t17 COMPUTE STATISTICS  FOR COLUMNS gsdb_org_code;

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t17 COLUMN dflt_service_product_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t17 COMPUTE STATISTICS  FOR COLUMNS dflt_service_product_id;

--Original Query:
  ----LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t17 FOR ACCESSLOCK TABLE ${EEDDWWDDBB2}.product_order_org FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t18(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_coverage_code     ,dflt_service_duration_cnt     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,svc_act_order_end_date     ,svc_act_order_start_date     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr      ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date                )SELECT      EDW17.instance_id     ,EDW17.item_instance_id     ,EDW17.active_end_date     ,EDW17.active_start_date     ,EDW17.actual_return_date     ,EDW17.actual_ship_date     ,EDW17.bill_to_site_use_id     ,EDW17.bom_enabled_flag     ,EDW17.country_code     ,EDW17.crnt_loc_cs_fml_org_code     ,EDW17.customer_po_date     ,EDW17.customer_po_nbr     ,POO.service_coverage_code                             (NAMED dflt_service_coverage_code)     ,POO.service_duration_cnt                              (NAMED dflt_service_duration_cnt)     ,EDW17.dflt_service_product_id     ,EDW17.dflt_wrnty_coverage_code     ,EDW17.dflt_wrnty_product_id     ,EDW17.dflt_wrnty_term_mth_cnt     ,EDW17.external_reference_nbr     ,EDW17.gsdb_org_code     ,EDW17.install_date     ,EDW17.install_loc_oper_unit_id     ,EDW17.install_location_id     ,EDW17.install_location_type_code     ,EDW17.inventory_item_id     ,EDW17.inventory_master_org_id     ,EDW17.inventory_org_id     ,EDW17.invtry_item_desc     ,EDW17.invtry_item_flag     ,EDW17.invtry_item_sponsor_loc_id     ,EDW17.invtry_item_status_code     ,EDW17.invtry_org_tran_code     ,EDW17.invtry_pass_nbr     ,EDW17.invtry_tran_code     ,EDW17.invtry_uom_code     ,EDW17.item_instance_status_desc     ,EDW17.item_instance_status_id     ,EDW17.item_instance_status_name     ,EDW17.item_instance_tran_code     ,EDW17.item_type_name     ,EDW17.last_order_line_id     ,EDW17.last_valid_invtry_org_id     ,EDW17.location_id     ,EDW17.location_type_code     ,EDW17.nl_trackable_flag     ,EDW17.offering_acctg_type_code     ,EDW17.oper_unit_country_code     ,EDW17.operating_unit_id     ,EDW17.operating_unit_name     ,EDW17.order_date     ,EDW17.order_header_id     ,EDW17.order_line_nbr     ,EDW17.order_nbr     ,EDW17.prod_act_order_end_date     ,EDW17.prod_act_order_start_date     ,EDW17.prod_act_svc_end_date     ,EDW17.prod_act_svc_start_date     ,EDW17.product_class     ,EDW17.product_class_model     ,EDW17.product_id     ,EDW17.product_id_formatted     ,EDW17.product_model     ,EDW17.product_source_org_id     ,EDW17.product_submodel     ,EDW17.return_by_date     ,EDW17.serial_nbr_control_code     ,EDW17.service_order_allowed_flag     ,EDW17.serviceable_product_flag     ,EDW17.ship_to_site_use_id     ,EDW17.shippable_item_flag     ,EDW17.status_tran_code     ,POO.actual_order_end_date                             (NAMED svc_act_order_end_date)     ,POO.actual_order_start_date                           (NAMED svc_act_order_start_date)     ,EDW17.terminated_flag     ,EDW17.version_label_date     ,EDW17.version_label_desc     ,EDW17.version_label_name     ,EDW17.vldtn_inventory_org_id     ,EDW17.vrsn_lbl_tran_code     ,EDW17.invtry_item_desc_unc     ,EDW17.prev_site_nbr        ,EDW17.service_tier_name,   EDW17.esd_flag,                        EDW17.media_type,                      EDW17.license_model,                   EDW17.license_start_date,              EDW17.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t17 EDW17LEFT OUTER JOIN      ${EEDDWWDDBB2}.product_order_org           POOON    EDW17.gsdb_org_code                   = POO.ordering_org_codeAND   EDW17.dflt_service_product_id         = POO.product_id

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t18           SELECT distinct
            EDW17.instance_id,
            EDW17.item_instance_id,
            EDW17.active_end_date,
            EDW17.active_start_date,
            EDW17.actual_return_date,
            EDW17.actual_ship_date,
            EDW17.bill_to_site_use_id,
            EDW17.bom_enabled_flag,
            EDW17.country_code,
            EDW17.crnt_loc_cs_fml_org_code,
            EDW17.customer_po_date,
            EDW17.customer_po_nbr,
            POO.service_coverage_code AS dflt_service_coverage_code,
            null,
            POO.service_duration_cnt AS dflt_service_duration_cnt,
            EDW17.dflt_service_product_id,
            EDW17.dflt_wrnty_coverage_code,
            EDW17.dflt_wrnty_product_id,
            EDW17.dflt_wrnty_term_mth_cnt,
            EDW17.external_reference_nbr,
            EDW17.gsdb_org_code,
            EDW17.install_date,
            EDW17.install_loc_oper_unit_id,
            EDW17.install_location_id,
            EDW17.install_location_type_code,
            EDW17.inventory_item_id,
            EDW17.inventory_master_org_id,
            EDW17.inventory_org_id,
            EDW17.invtry_item_desc,
            EDW17.invtry_item_flag,
            EDW17.invtry_item_sponsor_loc_id,
            EDW17.invtry_item_status_code,
            EDW17.invtry_org_tran_code,
            EDW17.invtry_pass_nbr,
            EDW17.invtry_tran_code,
            EDW17.invtry_uom_code,
            EDW17.item_instance_status_desc,
            EDW17.item_instance_status_id,
            EDW17.item_instance_status_name,
            EDW17.item_instance_tran_code,
            EDW17.item_type_name,
            EDW17.last_order_line_id,
            EDW17.last_valid_invtry_org_id,
            EDW17.location_id,
            EDW17.location_type_code,
            EDW17.nl_trackable_flag,
            EDW17.offering_acctg_type_code,
            EDW17.oper_unit_country_code,
            EDW17.operating_unit_id,
            EDW17.operating_unit_name,
            EDW17.order_date,
            EDW17.order_header_id,
            EDW17.order_line_nbr,
            EDW17.order_nbr,
            EDW17.prod_act_order_end_date,
            EDW17.prod_act_order_start_date,
            EDW17.prod_act_svc_end_date,
            EDW17.prod_act_svc_start_date,
            EDW17.product_class,
            EDW17.product_class_model,
            EDW17.product_id,
            EDW17.product_id_formatted,
            EDW17.product_model,
            EDW17.product_source_org_id,
            EDW17.product_submodel,
            EDW17.return_by_date,
            null,
            EDW17.serial_nbr_control_code,
            EDW17.service_order_allowed_flag,
            EDW17.serviceable_product_flag,
            EDW17.ship_to_site_use_id,
            EDW17.shippable_item_flag,
            EDW17.status_tran_code,
            POO.actual_order_end_date AS svc_act_order_end_date,
            POO.actual_order_start_date AS svc_act_order_start_date,
            EDW17.terminated_flag,
            EDW17.version_label_date,
            EDW17.version_label_desc,
            EDW17.version_label_name,
            EDW17.vldtn_inventory_org_id,
            EDW17.vrsn_lbl_tran_code,
            EDW17.invtry_item_desc_unc,
            EDW17.prev_site_nbr,
            EDW17.service_tier_name,
            EDW17.esd_flag,
            EDW17.media_type,
            EDW17.license_model,
            EDW17.license_start_date,
            EDW17.license_end_date,
            EDW17.cit_vendor_code,
            EDW17.mdm_product_identifier,
            EDW17.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t17    AS EDW17   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.product_order_org    AS POO    
                ON EDW17.gsdb_org_code = POO.ordering_org_code  
                AND EDW17.dflt_service_product_id = POO.product_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t15a ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15a;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t15b ALL

--Translated Query: SUCCESS W8c

  ----  TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t15b;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_19.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t19 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t19;

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t18                     COLUMN dflt_wrnty_product_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t18 COMPUTE STATISTICS  FOR COLUMNS dflt_wrnty_product_id;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.product_language FOR ACCESS          LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t18 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t19(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_coverage_code     ,dflt_service_duration_cnt     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_desc     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,svc_act_order_end_date     ,svc_act_order_start_date     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr        ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW18.instance_id     ,EDW18.item_instance_id     ,EDW18.active_end_date     ,EDW18.active_start_date     ,EDW18.actual_return_date     ,EDW18.actual_ship_date     ,EDW18.bill_to_site_use_id     ,EDW18.bom_enabled_flag     ,EDW18.country_code     ,EDW18.crnt_loc_cs_fml_org_code     ,EDW18.customer_po_date     ,EDW18.customer_po_nbr     ,EDW18.dflt_service_coverage_code     ,EDW18.dflt_service_duration_cnt     ,EDW18.dflt_service_product_id     ,EDW18.dflt_wrnty_coverage_code     <WM_COMMENT_START> dflt_wrnty_product_desc <WM_COMMENT_END>     ,PRL.product_desc     ,EDW18.dflt_wrnty_product_id     ,EDW18.dflt_wrnty_term_mth_cnt     ,EDW18.external_reference_nbr     ,EDW18.gsdb_org_code     ,EDW18.install_date     ,EDW18.install_loc_oper_unit_id     ,EDW18.install_location_id     ,EDW18.install_location_type_code     ,EDW18.inventory_item_id     ,EDW18.inventory_master_org_id     ,EDW18.inventory_org_id     ,EDW18.invtry_item_desc     ,EDW18.invtry_item_flag     ,EDW18.invtry_item_sponsor_loc_id     ,EDW18.invtry_item_status_code     ,EDW18.invtry_org_tran_code     ,EDW18.invtry_pass_nbr     ,EDW18.invtry_tran_code     ,EDW18.invtry_uom_code     ,EDW18.item_instance_status_desc     ,EDW18.item_instance_status_id     ,EDW18.item_instance_status_name     ,EDW18.item_instance_tran_code     ,EDW18.item_type_name     ,EDW18.last_order_line_id     ,EDW18.last_valid_invtry_org_id     ,EDW18.location_id     ,EDW18.location_type_code     ,EDW18.nl_trackable_flag     ,EDW18.offering_acctg_type_code     ,EDW18.oper_unit_country_code     ,EDW18.operating_unit_id     ,EDW18.operating_unit_name     ,EDW18.order_date     ,EDW18.order_header_id     ,EDW18.order_line_nbr     ,EDW18.order_nbr     ,EDW18.prod_act_order_end_date     ,EDW18.prod_act_order_start_date     ,EDW18.prod_act_svc_end_date     ,EDW18.prod_act_svc_start_date     ,EDW18.product_class     ,EDW18.product_class_model     ,EDW18.product_id     ,EDW18.product_id_formatted     ,EDW18.product_model     ,EDW18.product_source_org_id     ,EDW18.product_submodel     ,EDW18.return_by_date     ,EDW18.serial_nbr_control_code     ,EDW18.service_order_allowed_flag     ,EDW18.serviceable_product_flag     ,EDW18.ship_to_site_use_id     ,EDW18.shippable_item_flag     ,EDW18.status_tran_code     ,EDW18.svc_act_order_end_date     ,EDW18.svc_act_order_start_date     ,EDW18.terminated_flag     ,EDW18.version_label_date     ,EDW18.version_label_desc     ,EDW18.version_label_name     ,EDW18.vldtn_inventory_org_id     ,EDW18.vrsn_lbl_tran_code     ,EDW18.invtry_item_desc_unc     ,EDW18.prev_site_nbr       ,EDW18.service_tier_name,   EDW18.esd_flag,                   EDW18.media_type,                 EDW18.license_model,              EDW18.license_start_date,         EDW18.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t18 EDW18LEFT OUTER JOIN${EEDDWWDDBB2}.product_language PRLON  EDW18.dflt_wrnty_product_id = PRL.product_idAND PRL.primary_flag = 'Y'

--Translated Query: MANUAL

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t19           SELECT
            EDW18.instance_id,
            EDW18.item_instance_id,
            EDW18.active_end_date,
            EDW18.active_start_date,
            EDW18.actual_return_date,
            EDW18.actual_ship_date,
            EDW18.bill_to_site_use_id,
            EDW18.bom_enabled_flag,
            EDW18.country_code,
            EDW18.crnt_loc_cs_fml_org_code,
            EDW18.customer_po_date,
            EDW18.customer_po_nbr,
            EDW18.dflt_service_coverage_code,
            null,
            EDW18.dflt_service_duration_cnt,
            EDW18.dflt_service_product_id,
            EDW18.dflt_wrnty_coverage_code,
            PRL.product_desc,
            EDW18.dflt_wrnty_product_id,
            EDW18.dflt_wrnty_term_mth_cnt,
            EDW18.external_reference_nbr,
            EDW18.gsdb_org_code,
            EDW18.install_date,
            EDW18.install_loc_oper_unit_id,
            EDW18.install_location_id,
            EDW18.install_location_type_code,
            EDW18.inventory_item_id,
            EDW18.inventory_master_org_id,
            EDW18.inventory_org_id,
            EDW18.invtry_item_desc,
            EDW18.invtry_item_flag,
            EDW18.invtry_item_sponsor_loc_id,
            EDW18.invtry_item_status_code,
            EDW18.invtry_org_tran_code,
            EDW18.invtry_pass_nbr,
            EDW18.invtry_tran_code,
            EDW18.invtry_uom_code,
            EDW18.item_instance_status_desc,
            EDW18.item_instance_status_id,
            EDW18.item_instance_status_name,
            EDW18.item_instance_tran_code,
            EDW18.item_type_name,
            EDW18.last_order_line_id,
            EDW18.last_valid_invtry_org_id,
            EDW18.location_id,
            EDW18.location_type_code,
            EDW18.nl_trackable_flag,
            EDW18.offering_acctg_type_code,
            EDW18.oper_unit_country_code,
            EDW18.operating_unit_id,
            EDW18.operating_unit_name,
            EDW18.order_date,
            EDW18.order_header_id,
            EDW18.order_line_nbr,
            EDW18.order_nbr,
            EDW18.prod_act_order_end_date,
            EDW18.prod_act_order_start_date,
            EDW18.prod_act_svc_end_date,
            EDW18.prod_act_svc_start_date,
            EDW18.product_class,
            EDW18.product_class_model,
            EDW18.product_id,
            EDW18.product_id_formatted,
            EDW18.product_model,
            EDW18.product_source_org_id,
            EDW18.product_submodel,
            EDW18.return_by_date,
            null,
            EDW18.serial_nbr_control_code,
            EDW18.service_order_allowed_flag,
            EDW18.serviceable_product_flag,
            EDW18.ship_to_site_use_id,
            EDW18.shippable_item_flag,
            EDW18.status_tran_code,
            EDW18.svc_act_order_end_date,
            EDW18.svc_act_order_start_date,
            EDW18.terminated_flag,
            EDW18.version_label_date,
            EDW18.version_label_desc,
            EDW18.version_label_name,
            EDW18.vldtn_inventory_org_id,
            EDW18.vrsn_lbl_tran_code,
            EDW18.invtry_item_desc_unc,
            EDW18.prev_site_nbr,
            EDW18.service_tier_name,
            EDW18.esd_flag,
            EDW18.media_type,
            EDW18.license_model,
            EDW18.license_start_date,
            EDW18.license_end_date,
            EDW18.cit_vendor_code,
            EDW18.mdm_product_identifier,
            EDW18.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t18    AS EDW18   
        LEFT OUTER  JOIN
            (select * from ${EEDDWWDDBB2}.product_language  where upper(trim(primary_flag)) = 'Y')  AS PRL  
                        ON EDW18.dflt_wrnty_product_id = PRL.product_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t16 ALL

--Translated Query: SUCCESS W8c

   ---- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t16;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_20.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t19 COLUMN dflt_service_product_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t19 COMPUTE STATISTICS  FOR COLUMNS dflt_service_product_id;

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t19 COLUMN item_instance_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t19 COMPUTE STATISTICS  FOR COLUMNS item_instance_id;

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t19 COLUMN instance_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t19 COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t20 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t20;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.product FOR ACCESS                   LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t19 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t20(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_coverage_code     ,dflt_service_duration     ,dflt_service_duration_cnt     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_desc     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,svc_act_order_end_date     ,svc_act_order_start_date     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     ,invtry_item_desc_unc     ,prev_site_nbr      ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW19.instance_id     ,EDW19.item_instance_id     ,EDW19.active_end_date     ,EDW19.active_start_date     ,EDW19.actual_return_date     ,EDW19.actual_ship_date     ,EDW19.bill_to_site_use_id     ,EDW19.bom_enabled_flag     ,EDW19.country_code     ,EDW19.crnt_loc_cs_fml_org_code     ,EDW19.customer_po_date     ,EDW19.customer_po_nbr     ,EDW19.dflt_service_coverage_code     ,PROD.service_duration                                 (NAMED dflt_service_duration)     ,EDW19.dflt_service_duration_cnt     ,EDW19.dflt_service_product_id     ,EDW19.dflt_wrnty_coverage_code     ,EDW19.dflt_wrnty_product_desc     ,EDW19.dflt_wrnty_product_id     ,EDW19.dflt_wrnty_term_mth_cnt     ,EDW19.external_reference_nbr     ,EDW19.gsdb_org_code     ,EDW19.install_date     ,EDW19.install_loc_oper_unit_id     ,EDW19.install_location_id     ,EDW19.install_location_type_code     ,EDW19.inventory_item_id     ,EDW19.inventory_master_org_id     ,EDW19.inventory_org_id     ,EDW19.invtry_item_desc     ,EDW19.invtry_item_flag     ,EDW19.invtry_item_sponsor_loc_id     ,EDW19.invtry_item_status_code     ,EDW19.invtry_org_tran_code     ,EDW19.invtry_pass_nbr     ,EDW19.invtry_tran_code     ,EDW19.invtry_uom_code     ,EDW19.item_instance_status_desc     ,EDW19.item_instance_status_id     ,EDW19.item_instance_status_name     ,EDW19.item_instance_tran_code     ,EDW19.item_type_name     ,EDW19.last_order_line_id     ,EDW19.last_valid_invtry_org_id     ,EDW19.location_id     ,EDW19.location_type_code     ,EDW19.nl_trackable_flag     ,EDW19.offering_acctg_type_code     ,EDW19.oper_unit_country_code     ,EDW19.operating_unit_id     ,EDW19.operating_unit_name     ,EDW19.order_date     ,EDW19.order_header_id     ,EDW19.order_line_nbr     ,EDW19.order_nbr     ,EDW19.prod_act_order_end_date     ,EDW19.prod_act_order_start_date     ,EDW19.prod_act_svc_end_date     ,EDW19.prod_act_svc_start_date     ,EDW19.product_class     ,EDW19.product_class_model     ,EDW19.product_id     ,EDW19.product_id_formatted     ,EDW19.product_model     ,EDW19.product_source_org_id     ,EDW19.product_submodel     ,EDW19.return_by_date     ,EDW19.serial_nbr_control_code     ,EDW19.service_order_allowed_flag     ,EDW19.serviceable_product_flag     ,EDW19.ship_to_site_use_id     ,EDW19.shippable_item_flag     ,EDW19.status_tran_code     ,EDW19.svc_act_order_end_date     ,EDW19.svc_act_order_start_date     ,EDW19.terminated_flag     ,EDW19.version_label_date     ,EDW19.version_label_desc     ,EDW19.version_label_name     ,EDW19.vldtn_inventory_org_id     ,EDW19.vrsn_lbl_tran_code     ,EDW19.invtry_item_desc_unc     ,EDW19.prev_site_nbr         ,EDW19.service_tier_name,   EDW19.esd_flag,                        EDW19.media_type,                      EDW19.license_model,                   EDW19.license_start_date,              EDW19.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t19 EDW19LEFT OUTER JOIN     ${EEDDWWDDBB2}.product              PRODON   EDW19.dflt_service_product_id   = PROD.product_id

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t20           SELECT
            EDW19.instance_id,
            EDW19.item_instance_id,
            EDW19.active_end_date,
            EDW19.active_start_date,
            EDW19.actual_return_date,
            EDW19.actual_ship_date,
            EDW19.bill_to_site_use_id,
            EDW19.bom_enabled_flag,
            EDW19.country_code,
            EDW19.crnt_loc_cs_fml_org_code,
            EDW19.customer_po_date,
            EDW19.customer_po_nbr,
            EDW19.dflt_service_coverage_code,
            PROD.service_duration AS dflt_service_duration,
            EDW19.dflt_service_duration_cnt,
            EDW19.dflt_service_product_id,
            EDW19.dflt_wrnty_coverage_code,
            EDW19.dflt_wrnty_product_desc,
            EDW19.dflt_wrnty_product_id,
            EDW19.dflt_wrnty_term_mth_cnt,
            EDW19.external_reference_nbr,
            EDW19.gsdb_org_code,
            EDW19.install_date,
            EDW19.install_loc_oper_unit_id,
            EDW19.install_location_id,
            EDW19.install_location_type_code,
            EDW19.inventory_item_id,
            EDW19.inventory_master_org_id,
            EDW19.inventory_org_id,
            EDW19.invtry_item_desc,
            EDW19.invtry_item_flag,
            EDW19.invtry_item_sponsor_loc_id,
            EDW19.invtry_item_status_code,
            EDW19.invtry_org_tran_code,
            EDW19.invtry_pass_nbr,
            EDW19.invtry_tran_code,
            EDW19.invtry_uom_code,
            EDW19.item_instance_status_desc,
            EDW19.item_instance_status_id,
            EDW19.item_instance_status_name,
            EDW19.item_instance_tran_code,
            EDW19.item_type_name,
            EDW19.last_order_line_id,
            EDW19.last_valid_invtry_org_id,
            EDW19.location_id,
            EDW19.location_type_code,
            EDW19.nl_trackable_flag,
            EDW19.offering_acctg_type_code,
            EDW19.oper_unit_country_code,
            EDW19.operating_unit_id,
            EDW19.operating_unit_name,
            EDW19.order_date,
            EDW19.order_header_id,
            EDW19.order_line_nbr,
            EDW19.order_nbr,
            EDW19.prod_act_order_end_date,
            EDW19.prod_act_order_start_date,
            EDW19.prod_act_svc_end_date,
            EDW19.prod_act_svc_start_date,
            EDW19.product_class,
            EDW19.product_class_model,
            EDW19.product_id,
            EDW19.product_id_formatted,
            EDW19.product_model,
            EDW19.product_source_org_id,
            EDW19.product_submodel,
            EDW19.return_by_date,
            null,
            EDW19.serial_nbr_control_code,
            EDW19.service_order_allowed_flag,
            EDW19.serviceable_product_flag,
            EDW19.ship_to_site_use_id,
            EDW19.shippable_item_flag,
            EDW19.status_tran_code,
            EDW19.svc_act_order_end_date,
            EDW19.svc_act_order_start_date,
            EDW19.terminated_flag,
            EDW19.version_label_date,
            EDW19.version_label_desc,
            EDW19.version_label_name,
            EDW19.vldtn_inventory_org_id,
            EDW19.vrsn_lbl_tran_code,
            EDW19.invtry_item_desc_unc,
            EDW19.prev_site_nbr,
            EDW19.service_tier_name,
            EDW19.esd_flag,
            EDW19.media_type,
            EDW19.license_model,
            EDW19.license_start_date,
            EDW19.license_end_date,
            EDW19.cit_vendor_code,
            EDW19.mdm_product_identifier,
            EDW19.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t19    AS EDW19   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.product    AS PROD    
                ON EDW19.dflt_service_product_id = PROD.product_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t17 ALL

--Translated Query: SUCCESS W8c

  ----  TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t17;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_21.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t21 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t21;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.wcs_country_area FOR ACCESS          LOCK TABLE ${EEDDWWDDBB2}.branch_country_area_xref FOR ACCESS  LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t20 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t21(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,area_code     ,area_desc     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_coverage_code     ,dflt_service_duration     ,dflt_service_duration_cnt     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_desc     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,svc_act_order_end_date     ,svc_act_order_start_date     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr        ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW20.instance_id     ,EDW20.item_instance_id     ,EDW20.active_end_date     ,EDW20.active_start_date     ,EDW20.actual_return_date     ,EDW20.actual_ship_date     <WM_COMMENT_START> area_code <WM_COMMENT_END>     ,WCA.country_area_code     <WM_COMMENT_START> area_desc <WM_COMMENT_END>     ,WCA.country_area_desc     ,EDW20.bill_to_site_use_id     ,EDW20.bom_enabled_flag     ,EDW20.country_code     ,EDW20.crnt_loc_cs_fml_org_code     ,EDW20.customer_po_date     ,EDW20.customer_po_nbr     ,EDW20.dflt_service_coverage_code     ,EDW20.dflt_service_duration     ,EDW20.dflt_service_duration_cnt     ,EDW20.dflt_service_product_id     ,EDW20.dflt_wrnty_coverage_code     ,EDW20.dflt_wrnty_product_desc     ,EDW20.dflt_wrnty_product_id     ,EDW20.dflt_wrnty_term_mth_cnt     ,EDW20.external_reference_nbr     ,EDW20.gsdb_org_code     ,EDW20.install_date     ,EDW20.install_loc_oper_unit_id     ,EDW20.install_location_id     ,EDW20.install_location_type_code     ,EDW20.inventory_item_id     ,EDW20.inventory_master_org_id     ,EDW20.inventory_org_id     ,EDW20.invtry_item_desc     ,EDW20.invtry_item_flag     ,EDW20.invtry_item_sponsor_loc_id     ,EDW20.invtry_item_status_code     ,EDW20.invtry_org_tran_code     ,EDW20.invtry_pass_nbr     ,EDW20.invtry_tran_code     ,EDW20.invtry_uom_code     ,EDW20.item_instance_status_desc     ,EDW20.item_instance_status_id     ,EDW20.item_instance_status_name     ,EDW20.item_instance_tran_code     ,EDW20.item_type_name     ,EDW20.last_order_line_id     ,EDW20.last_valid_invtry_org_id     ,EDW20.location_id     ,EDW20.location_type_code     ,EDW20.nl_trackable_flag     ,EDW20.offering_acctg_type_code     ,EDW20.oper_unit_country_code     ,EDW20.operating_unit_id     ,EDW20.operating_unit_name     ,EDW20.order_date     ,EDW20.order_header_id     ,EDW20.order_line_nbr     ,EDW20.order_nbr     ,EDW20.prod_act_order_end_date     ,EDW20.prod_act_order_start_date     ,EDW20.prod_act_svc_end_date     ,EDW20.prod_act_svc_start_date     ,EDW20.product_class     ,EDW20.product_class_model     ,EDW20.product_id     ,EDW20.product_id_formatted     ,EDW20.product_model     ,EDW20.product_source_org_id     ,EDW20.product_submodel     ,EDW20.return_by_date     ,EDW20.serial_nbr_control_code     ,EDW20.service_order_allowed_flag     ,EDW20.serviceable_product_flag     ,EDW20.ship_to_site_use_id     ,EDW20.shippable_item_flag     ,EDW20.status_tran_code     ,EDW20.svc_act_order_end_date     ,EDW20.svc_act_order_start_date     ,EDW20.terminated_flag     ,EDW20.version_label_date     ,EDW20.version_label_desc     ,EDW20.version_label_name     ,EDW20.vldtn_inventory_org_id     ,EDW20.vrsn_lbl_tran_code     ,EDW20.invtry_item_desc_unc     ,EDW20.prev_site_nbr       ,EDW20.service_tier_name,   EDW20.esd_flag,                        EDW20.media_type,                      EDW20.license_model,                   EDW20.license_start_date,              EDW20.license_end_date           FROM  (${AAPPLLIIDD1EENNVV}_install_base_dn_t20 EDW2 0LEFT OUTER JOIN (${EEDDWWDDBB2}.wcs_country_area WCA     INNER JOIN  ${EEDDWWDDBB2}.branch_country_area_xref BCAX    ON WCA.country_area_code = BCAX.country_area_code   )ON BCAX.branch_code = EDW20.crnt_loc_cs_fml_org_codeAND EDW20.country_code = 'US')

--Translated Query: MANUAL

with qq1 as (select
                wca.country_area_code,
                country_area_desc,
                branch_code 
            from
                ${EEDDWWDDBB2}.wcs_country_area wca 
            INNER JOIN
                ${EEDDWWDDBB2}.branch_country_area_xref BCAX 
                    on  WCA.country_area_code = BCAX.country_area_code )  INSERT  
            INTO
                TABLE
                ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t21           SELECT
                    EDW20.instance_id,
                    EDW20.item_instance_id,
                    EDW20.active_end_date,
                    EDW20.active_start_date,
                    EDW20.actual_return_date,
                    EDW20.actual_ship_date,
                    WCA.country_area_code,
                    WCA.country_area_desc,
                    EDW20.bill_to_site_use_id,
                    EDW20.bom_enabled_flag,
                    EDW20.country_code,
                    EDW20.crnt_loc_cs_fml_org_code,
                    EDW20.customer_po_date,
                    EDW20.customer_po_nbr,
                    EDW20.dflt_service_coverage_code,
                    EDW20.dflt_service_duration,
                    EDW20.dflt_service_duration_cnt,
                    EDW20.dflt_service_product_id,
                    EDW20.dflt_wrnty_coverage_code,
                    EDW20.dflt_wrnty_product_desc,
                    EDW20.dflt_wrnty_product_id,
                    EDW20.dflt_wrnty_term_mth_cnt,
                    EDW20.external_reference_nbr,
                    EDW20.gsdb_org_code,
                    EDW20.install_date,
                    EDW20.install_loc_oper_unit_id,
                    EDW20.install_location_id,
                    EDW20.install_location_type_code,
                    EDW20.inventory_item_id,
                    EDW20.inventory_master_org_id,
                    EDW20.inventory_org_id,
                    EDW20.invtry_item_desc,
                    EDW20.invtry_item_flag,
                    EDW20.invtry_item_sponsor_loc_id,
                    EDW20.invtry_item_status_code,
                    EDW20.invtry_org_tran_code,
                    EDW20.invtry_pass_nbr,
                    EDW20.invtry_tran_code,
                    EDW20.invtry_uom_code,
                    EDW20.item_instance_status_desc,
                    EDW20.item_instance_status_id,
                    EDW20.item_instance_status_name,
                    EDW20.item_instance_tran_code,
                    EDW20.item_type_name,
                    EDW20.last_order_line_id,
                    EDW20.last_valid_invtry_org_id,
                    EDW20.location_id,
                    EDW20.location_type_code,
                    EDW20.nl_trackable_flag,
                    EDW20.offering_acctg_type_code,
                    EDW20.oper_unit_country_code,
                    EDW20.operating_unit_id,
                    EDW20.operating_unit_name,
                    EDW20.order_date,
                    EDW20.order_header_id,
                    EDW20.order_line_nbr,
                    EDW20.order_nbr,
                    EDW20.prod_act_order_end_date,
                    EDW20.prod_act_order_start_date,
                    EDW20.prod_act_svc_end_date,
                    EDW20.prod_act_svc_start_date,
                    EDW20.product_class,
                    EDW20.product_class_model,
                    EDW20.product_id,
                    EDW20.product_id_formatted,
                    EDW20.product_model,
                    EDW20.product_source_org_id,
                    EDW20.product_submodel,
                    EDW20.return_by_date,
                    null,
                    EDW20.serial_nbr_control_code,
                    EDW20.service_order_allowed_flag,
                    EDW20.serviceable_product_flag,
                    EDW20.ship_to_site_use_id,
                    EDW20.shippable_item_flag,
                    EDW20.status_tran_code,
                    EDW20.svc_act_order_end_date,
                    EDW20.svc_act_order_start_date,
                    EDW20.terminated_flag,
                    EDW20.version_label_date,
                    EDW20.version_label_desc,
                    EDW20.version_label_name,
                    EDW20.vldtn_inventory_org_id,
                    EDW20.vrsn_lbl_tran_code,
                    EDW20.invtry_item_desc_unc,
                    EDW20.prev_site_nbr,
                    EDW20.service_tier_name,
                    EDW20.esd_flag,
                    EDW20.media_type,
                    EDW20.license_model,
                    EDW20.license_start_date,
                    EDW20.license_end_date,
                EDW20.cit_vendor_code,        
                EDW20.mdm_product_identifier, 
                EDW20.mdm_solution_identifier
  
                FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t20 AS EDW20
                   
                LEFT OUTER  JOIN
                    qq1 as wca  
                        ON wca.branch_code = EDW20.crnt_loc_cs_fml_org_code AND trim(upper(country_code)) = 'US'
            ;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t18 ALL

--Translated Query: SUCCESS w8c

  ----  TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t18;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_22.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t22 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t22;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.geo_rollup_xref FOR ACCESS           LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t21 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t22(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,area_code     ,area_desc     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,country_desc     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_coverage_code     ,dflt_service_duration     ,dflt_service_duration_cnt     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_desc     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,region_code     ,region_desc     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,svc_act_order_end_date     ,svc_act_order_start_date     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc      ,prev_site_nbr      ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW21.instance_id     ,EDW21.item_instance_id     ,EDW21.active_end_date     ,EDW21.active_start_date     ,EDW21.actual_return_date     ,EDW21.actual_ship_date     <WM_COMMENT_START> area_code <WM_COMMENT_END>     ,GRX.area_code     <WM_COMMENT_START> area_desc <WM_COMMENT_END>     ,GRX.area_desc     ,EDW21.bill_to_site_use_id     ,EDW21.bom_enabled_flag     ,EDW21.country_code     <WM_COMMENT_START> country_desc <WM_COMMENT_END>     ,GRX.country_desc     ,EDW21.crnt_loc_cs_fml_org_code     ,EDW21.customer_po_date     ,EDW21.customer_po_nbr     ,EDW21.dflt_service_coverage_code     ,EDW21.dflt_service_duration     ,EDW21.dflt_service_duration_cnt     ,EDW21.dflt_service_product_id     ,EDW21.dflt_wrnty_coverage_code     ,EDW21.dflt_wrnty_product_desc     ,EDW21.dflt_wrnty_product_id     ,EDW21.dflt_wrnty_term_mth_cnt     ,EDW21.external_reference_nbr     ,EDW21.gsdb_org_code     ,EDW21.install_date     ,EDW21.install_loc_oper_unit_id     ,EDW21.install_location_id     ,EDW21.install_location_type_code     ,EDW21.inventory_item_id     ,EDW21.inventory_master_org_id     ,EDW21.inventory_org_id     ,EDW21.invtry_item_desc     ,EDW21.invtry_item_flag     ,EDW21.invtry_item_sponsor_loc_id     ,EDW21.invtry_item_status_code     ,EDW21.invtry_org_tran_code     ,EDW21.invtry_pass_nbr     ,EDW21.invtry_tran_code     ,EDW21.invtry_uom_code     ,EDW21.item_instance_status_desc     ,EDW21.item_instance_status_id     ,EDW21.item_instance_status_name     ,EDW21.item_instance_tran_code     ,EDW21.item_type_name     ,EDW21.last_order_line_id     ,EDW21.last_valid_invtry_org_id     ,EDW21.location_id     ,EDW21.location_type_code     ,EDW21.nl_trackable_flag     ,EDW21.offering_acctg_type_code     ,EDW21.oper_unit_country_code     ,EDW21.operating_unit_id     ,EDW21.operating_unit_name     ,EDW21.order_date     ,EDW21.order_header_id     ,EDW21.order_line_nbr     ,EDW21.order_nbr     ,EDW21.prod_act_order_end_date     ,EDW21.prod_act_order_start_date     ,EDW21.prod_act_svc_end_date     ,EDW21.prod_act_svc_start_date     ,EDW21.product_class     ,EDW21.product_class_model     ,EDW21.product_id     ,EDW21.product_id_formatted     ,EDW21.product_model     ,EDW21.product_source_org_id     ,EDW21.product_submodel     <WM_COMMENT_START> region_code <WM_COMMENT_END>     ,GRX.region_code     <WM_COMMENT_START> region_desc <WM_COMMENT_END>     ,GRX.region_desc     ,EDW21.return_by_date     ,EDW21.serial_nbr_control_code     ,EDW21.service_order_allowed_flag     ,EDW21.serviceable_product_flag     ,EDW21.ship_to_site_use_id     ,EDW21.shippable_item_flag     ,EDW21.status_tran_code     ,EDW21.svc_act_order_end_date     ,EDW21.svc_act_order_start_date     ,EDW21.terminated_flag     ,EDW21.version_label_date     ,EDW21.version_label_desc     ,EDW21.version_label_name     ,EDW21.vldtn_inventory_org_id     ,EDW21.vrsn_lbl_tran_code     ,EDW21.invtry_item_desc_unc     ,EDW21.prev_site_nbr      ,EDW21.service_tier_name,   EDW21.esd_flag,                        EDW21.media_type,                      EDW21.license_model,                   EDW21.license_start_date,              EDW21.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t21 EDW21LEFT OUTER JOIN${EEDDWWDDBB1}.geo_rollup_xref GRXON EDW21.country_code = GRX.country_codeAND GRX.business_unit_code = 'B4'WHERE EDW21.area_code IS NULL

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t22           SELECT
            EDW21.instance_id,
            EDW21.item_instance_id,
            EDW21.active_end_date,
            EDW21.active_start_date,
            EDW21.actual_return_date,
            EDW21.actual_ship_date,
            GRX.area_code,
            GRX.area_desc,
            EDW21.bill_to_site_use_id,
            EDW21.bom_enabled_flag,
            EDW21.country_code,
            GRX.country_desc,
            EDW21.crnt_loc_cs_fml_org_code,
            EDW21.customer_po_date,
            EDW21.customer_po_nbr,
            EDW21.dflt_service_coverage_code,
            EDW21.dflt_service_duration,
            EDW21.dflt_service_duration_cnt,
            EDW21.dflt_service_product_id,
            EDW21.dflt_wrnty_coverage_code,
            EDW21.dflt_wrnty_product_desc,
            EDW21.dflt_wrnty_product_id,
            EDW21.dflt_wrnty_term_mth_cnt,
            EDW21.external_reference_nbr,
            EDW21.gsdb_org_code,
            EDW21.install_date,
            EDW21.install_loc_oper_unit_id,
            EDW21.install_location_id,
            EDW21.install_location_type_code,
            EDW21.inventory_item_id,
            EDW21.inventory_master_org_id,
            EDW21.inventory_org_id,
            EDW21.invtry_item_desc,
            EDW21.invtry_item_flag,
            EDW21.invtry_item_sponsor_loc_id,
            EDW21.invtry_item_status_code,
            EDW21.invtry_org_tran_code,
            EDW21.invtry_pass_nbr,
            EDW21.invtry_tran_code,
            EDW21.invtry_uom_code,
            EDW21.item_instance_status_desc,
            EDW21.item_instance_status_id,
            EDW21.item_instance_status_name,
            EDW21.item_instance_tran_code,
            EDW21.item_type_name,
            EDW21.last_order_line_id,
            EDW21.last_valid_invtry_org_id,
            EDW21.location_id,
            EDW21.location_type_code,
            EDW21.nl_trackable_flag,
            EDW21.offering_acctg_type_code,
            EDW21.oper_unit_country_code,
            EDW21.operating_unit_id,
            EDW21.operating_unit_name,
            EDW21.order_date,
            EDW21.order_header_id,
            EDW21.order_line_nbr,
            EDW21.order_nbr,
            EDW21.prod_act_order_end_date,
            EDW21.prod_act_order_start_date,
            EDW21.prod_act_svc_end_date,
            EDW21.prod_act_svc_start_date,
            EDW21.product_class,
            EDW21.product_class_model,
            EDW21.product_id,
            EDW21.product_id_formatted,
            EDW21.product_model,
            EDW21.product_source_org_id,
            EDW21.product_submodel,
            GRX.region_code,
            GRX.region_desc,
            EDW21.return_by_date,
            null,
            EDW21.serial_nbr_control_code,
            EDW21.service_order_allowed_flag,
            EDW21.serviceable_product_flag,
            EDW21.ship_to_site_use_id,
            EDW21.shippable_item_flag,
            EDW21.status_tran_code,
            EDW21.svc_act_order_end_date,
            EDW21.svc_act_order_start_date,
            EDW21.terminated_flag,
            EDW21.version_label_date,
            EDW21.version_label_desc,
            EDW21.version_label_name,
            EDW21.vldtn_inventory_org_id,
            EDW21.vrsn_lbl_tran_code,
            EDW21.invtry_item_desc_unc,
            EDW21.prev_site_nbr,
            EDW21.service_tier_name,
            EDW21.esd_flag,
            EDW21.media_type,
            EDW21.license_model,
            EDW21.license_start_date,
            EDW21.license_end_date,
            EDW21.cit_vendor_code,
            EDW21.mdm_product_identifier,
            EDW21.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t21    AS EDW21   
        LEFT OUTER  JOIN
            (select * from ${EEDDWWDDBB1}.geo_rollup_xref where upper(trim(business_unit_code)) = 'B4')    AS GRX  
                        ON EDW21.country_code = GRX.country_code    
        WHERE
            EDW21.area_code  IS NULL;

--Original Query:
  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t22(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,area_code     ,area_desc     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,country_desc     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_coverage_code     ,dflt_service_duration     ,dflt_service_duration_cnt     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_desc     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,region_code     ,region_desc     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,svc_act_order_end_date     ,svc_act_order_start_date     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc      ,prev_site_nbr         ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW21.instance_id     ,EDW21.item_instance_id     ,EDW21.active_end_date     ,EDW21.active_start_date     ,EDW21.actual_return_date     ,EDW21.actual_ship_date     <WM_COMMENT_START> area_code <WM_COMMENT_END>     ,EDW21.area_code     <WM_COMMENT_START> area_desc <WM_COMMENT_END>     ,EDW21.area_desc     ,EDW21.bill_to_site_use_id     ,EDW21.bom_enabled_flag     ,EDW21.country_code     <WM_COMMENT_START> country_desc <WM_COMMENT_END>     ,GRX.country_desc     ,EDW21.crnt_loc_cs_fml_org_code     ,EDW21.customer_po_date     ,EDW21.customer_po_nbr     ,EDW21.dflt_service_coverage_code     ,EDW21.dflt_service_duration     ,EDW21.dflt_service_duration_cnt     ,EDW21.dflt_service_product_id     ,EDW21.dflt_wrnty_coverage_code     ,EDW21.dflt_wrnty_product_desc     ,EDW21.dflt_wrnty_product_id     ,EDW21.dflt_wrnty_term_mth_cnt     ,EDW21.external_reference_nbr     ,EDW21.gsdb_org_code     ,EDW21.install_date     ,EDW21.install_loc_oper_unit_id     ,EDW21.install_location_id     ,EDW21.install_location_type_code     ,EDW21.inventory_item_id     ,EDW21.inventory_master_org_id     ,EDW21.invtry_item_desc     ,EDW21.invtry_item_flag     ,EDW21.invtry_item_sponsor_loc_id     ,EDW21.invtry_item_status_code     ,EDW21.invtry_org_tran_code     ,EDW21.invtry_pass_nbr     ,EDW21.invtry_tran_code     ,EDW21.invtry_uom_code     ,EDW21.item_instance_status_desc     ,EDW21.item_instance_status_id     ,EDW21.item_instance_status_name     ,EDW21.item_instance_tran_code     ,EDW21.item_type_name     ,EDW21.last_order_line_id     ,EDW21.last_valid_invtry_org_id     ,EDW21.location_id     ,EDW21.location_type_code     ,EDW21.nl_trackable_flag     ,EDW21.offering_acctg_type_code     ,EDW21.oper_unit_country_code     ,EDW21.operating_unit_id     ,EDW21.operating_unit_name     ,EDW21.order_date     ,EDW21.order_header_id     ,EDW21.order_line_nbr     ,EDW21.order_nbr     ,EDW21.prod_act_order_end_date     ,EDW21.prod_act_order_start_date     ,EDW21.prod_act_svc_end_date     ,EDW21.prod_act_svc_start_date     ,EDW21.product_class     ,EDW21.product_class_model     ,EDW21.product_id     ,EDW21.product_id_formatted     ,EDW21.product_model     ,EDW21.product_source_org_id     ,EDW21.product_submodel     <WM_COMMENT_START> region_code <WM_COMMENT_END>     ,GRX.region_code     <WM_COMMENT_START> region_desc <WM_COMMENT_END>     ,GRX.region_desc     ,EDW21.return_by_date     ,EDW21.serial_nbr_control_code     ,EDW21.service_order_allowed_flag     ,EDW21.serviceable_product_flag     ,EDW21.ship_to_site_use_id     ,EDW21.shippable_item_flag     ,EDW21.status_tran_code     ,EDW21.svc_act_order_end_date     ,EDW21.svc_act_order_start_date     ,EDW21.terminated_flag     ,EDW21.version_label_date     ,EDW21.version_label_desc     ,EDW21.version_label_name     ,EDW21.vldtn_inventory_org_id     ,EDW21.vrsn_lbl_tran_code     ,EDW21.invtry_item_desc_unc       ,EDW21.prev_site_nbr        ,EDW21.service_tier_name,   EDW21.esd_flag,                        EDW21.media_type,                      EDW21.license_model,                   EDW21.license_start_date,              EDW21.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t21 EDW21LEFT OUTER JOIN${EEDDWWDDBB1}.geo_rollup_xref GRXON EDW21.country_code = GRX.country_codeAND GRX.business_unit_code = 'B4'WHERE (instance_id, item_instance_id) NOT IN    (SELECT instance_id, item_instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t22)

--Translated Query: MANUAL

   WITH qq1 AS (    SELECT
                EDW21.instance_id,
                EDW21.item_instance_id,
                EDW21.active_end_date,
                EDW21.active_start_date,
                EDW21.actual_return_date,
                EDW21.actual_ship_date,
                EDW21.area_code,
                EDW21.area_desc,
                EDW21.bill_to_site_use_id,
                EDW21.bom_enabled_flag,
                EDW21.country_code,
                GRX.country_desc,
                EDW21.crnt_loc_cs_fml_org_code,
                EDW21.customer_po_date,
                EDW21.customer_po_nbr,
                EDW21.dflt_service_coverage_code,
                EDW21.dflt_service_duration,
                EDW21.dflt_service_duration_cnt,
                EDW21.dflt_service_product_id,
                EDW21.dflt_wrnty_coverage_code,
                EDW21.dflt_wrnty_product_desc,
                EDW21.dflt_wrnty_product_id,
                EDW21.dflt_wrnty_term_mth_cnt,
                EDW21.external_reference_nbr,
                EDW21.gsdb_org_code,
                EDW21.install_date,
                EDW21.install_loc_oper_unit_id,
                EDW21.install_location_id,
                EDW21.install_location_type_code,
                EDW21.inventory_item_id,
                EDW21.inventory_master_org_id,
                null,
                EDW21.invtry_item_desc,
                EDW21.invtry_item_flag,
                EDW21.invtry_item_sponsor_loc_id,
                EDW21.invtry_item_status_code,
                EDW21.invtry_org_tran_code,
                EDW21.invtry_pass_nbr,
                EDW21.invtry_tran_code,
                EDW21.invtry_uom_code,
                EDW21.item_instance_status_desc,
                EDW21.item_instance_status_id,
                EDW21.item_instance_status_name,
                EDW21.item_instance_tran_code,
                EDW21.item_type_name,
                EDW21.last_order_line_id,
                EDW21.last_valid_invtry_org_id,
                EDW21.location_id,
                EDW21.location_type_code,
                EDW21.nl_trackable_flag,
                EDW21.offering_acctg_type_code,
                EDW21.oper_unit_country_code,
                EDW21.operating_unit_id,
                EDW21.operating_unit_name,
                EDW21.order_date,
                EDW21.order_header_id,
                EDW21.order_line_nbr,
                EDW21.order_nbr,
                EDW21.prod_act_order_end_date,
                EDW21.prod_act_order_start_date,
                EDW21.prod_act_svc_end_date,
                EDW21.prod_act_svc_start_date,
                EDW21.product_class,
                EDW21.product_class_model,
                EDW21.product_id,
                EDW21.product_id_formatted,
                EDW21.product_model,
                EDW21.product_source_org_id,
                EDW21.product_submodel,
                GRX.region_code,
                GRX.region_desc,
                EDW21.return_by_date,
                null,
                EDW21.serial_nbr_control_code,
                EDW21.service_order_allowed_flag,
                EDW21.serviceable_product_flag,
                EDW21.ship_to_site_use_id,
                EDW21.shippable_item_flag,
                EDW21.status_tran_code,
                EDW21.svc_act_order_end_date,
                EDW21.svc_act_order_start_date,
                EDW21.terminated_flag,
                EDW21.version_label_date,
                EDW21.version_label_desc,
                EDW21.version_label_name,
                EDW21.vldtn_inventory_org_id,
                EDW21.vrsn_lbl_tran_code,
                EDW21.invtry_item_desc_unc,
                EDW21.prev_site_nbr,
                EDW21.service_tier_name,
                EDW21.esd_flag,
                EDW21.media_type,
                EDW21.license_model,
                EDW21.license_start_date,
                EDW21.license_end_date,
            EDW21.cit_vendor_code,        
            EDW21.mdm_product_identifier, 
            EDW21.mdm_solution_identifier
 
            FROM
                ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t21    AS EDW21  
            LEFT OUTER  JOIN
                (select * from ${EEDDWWDDBB1}.geo_rollup_xref  where upper(trim(business_unit_code)) = 'B4')  AS GRX  
                    ON EDW21.country_code = GRX.country_code  
                      
            LEFT OUTER JOIN
                (SELECT
                    DISTINCT instance_id AS auto_c00,
                    item_instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t22 ) AS autoAlias_44  
                    ON ( upper(trim(instance_id)) = upper(trim(autoAlias_44.AUTO_C00))  
                    AND item_instance_id = autoAlias_44.AUTO_C01 )  
            WHERE
                autoAlias_44.AUTO_C00 IS  NULL  
                AND autoAlias_44.AUTO_C01 IS  NULL         ) INSERT  
                INTO
                    TABLE
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t22       SELECT
                        *  
                    FROM
                        qq1;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t19 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t19;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_23.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t23 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t23;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.currency_xref FOR ACCESS             LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t22 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t23(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,area_code     ,area_desc     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,country_desc     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_coverage_code     ,dflt_service_duration     ,dflt_service_duration_cnt     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_desc     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,func_curr_code     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,region_code     ,region_desc     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,svc_act_order_end_date     ,svc_act_order_start_date     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc      ,prev_site_nbr      ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW22.instance_id     ,EDW22.item_instance_id     ,EDW22.active_end_date     ,EDW22.active_start_date     ,EDW22.actual_return_date     ,EDW22.actual_ship_date     ,EDW22.area_code     ,EDW22.area_desc     ,EDW22.bill_to_site_use_id     ,EDW22.bom_enabled_flag     ,EDW22.country_code     ,EDW22.country_desc     ,EDW22.crnt_loc_cs_fml_org_code     ,EDW22.customer_po_date     ,EDW22.customer_po_nbr     ,EDW22.dflt_service_coverage_code     ,EDW22.dflt_service_duration     ,EDW22.dflt_service_duration_cnt     ,EDW22.dflt_service_product_id     ,EDW22.dflt_wrnty_coverage_code     ,EDW22.dflt_wrnty_product_desc     ,EDW22.dflt_wrnty_product_id     ,EDW22.dflt_wrnty_term_mth_cnt     <WM_COMMENT_START> func_curr_code <WM_COMMENT_END>     ,CXREF.currency_code     ,EDW22.external_reference_nbr     ,EDW22.gsdb_org_code     ,EDW22.install_date     ,EDW22.install_loc_oper_unit_id     ,EDW22.install_location_id     ,EDW22.install_location_type_code     ,EDW22.inventory_item_id     ,EDW22.inventory_master_org_id     ,EDW22.inventory_org_id     ,EDW22.invtry_item_desc     ,EDW22.invtry_item_flag     ,EDW22.invtry_item_sponsor_loc_id     ,EDW22.invtry_item_status_code     ,EDW22.invtry_org_tran_code     ,EDW22.invtry_pass_nbr     ,EDW22.invtry_tran_code     ,EDW22.invtry_uom_code     ,EDW22.item_instance_status_desc     ,EDW22.item_instance_status_id     ,EDW22.item_instance_status_name     ,EDW22.item_instance_tran_code     ,EDW22.item_type_name     ,EDW22.last_order_line_id     ,EDW22.last_valid_invtry_org_id     ,EDW22.location_id     ,EDW22.location_type_code     ,EDW22.nl_trackable_flag     ,EDW22.offering_acctg_type_code     ,EDW22.oper_unit_country_code     ,EDW22.operating_unit_id     ,EDW22.operating_unit_name     ,EDW22.order_date     ,EDW22.order_header_id     ,EDW22.order_line_nbr     ,EDW22.order_nbr     ,EDW22.prod_act_order_end_date     ,EDW22.prod_act_order_start_date     ,EDW22.prod_act_svc_end_date     ,EDW22.prod_act_svc_start_date     ,EDW22.product_class     ,EDW22.product_class_model     ,EDW22.product_id     ,EDW22.product_id_formatted     ,EDW22.product_model     ,EDW22.product_source_org_id     ,EDW22.product_submodel     ,EDW22.region_code     ,EDW22.region_desc     ,EDW22.return_by_date     ,EDW22.serial_nbr_control_code     ,EDW22.service_order_allowed_flag     ,EDW22.serviceable_product_flag     ,EDW22.ship_to_site_use_id     ,EDW22.shippable_item_flag     ,EDW22.status_tran_code     ,EDW22.svc_act_order_end_date     ,EDW22.svc_act_order_start_date     ,EDW22.terminated_flag     ,EDW22.version_label_date     ,EDW22.version_label_desc     ,EDW22.version_label_name     ,EDW22.vldtn_inventory_org_id     ,EDW22.vrsn_lbl_tran_code     ,EDW22.invtry_item_desc_unc      ,EDW22.prev_site_nbr        ,EDW22.service_tier_name,   EDW22.esd_flag,                        EDW22.media_type,                      EDW22.license_model,                   EDW22.license_start_date,              EDW22.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t22 EDW22LEFT OUTER JOIN${EEDDWWDDBB1}.currency_xref CXREFON EDW22.country_code = CXREF.country_codeAND CXREF.data_source_code = 'COS'

--Translated Query: MANUAL

    INSERT  
            INTO
                TABLE
                ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t23           SELECT
                    EDW22.instance_id,
                    EDW22.item_instance_id,
                    EDW22.active_end_date,
                    EDW22.active_start_date,
                    EDW22.actual_return_date,
                    EDW22.actual_ship_date,
                    EDW22.area_code,
                    EDW22.area_desc,
                    EDW22.bill_to_site_use_id,
                    EDW22.bom_enabled_flag,
                    EDW22.country_code,
                    EDW22.country_desc,
                    EDW22.crnt_loc_cs_fml_org_code,
                    EDW22.customer_po_date,
                    EDW22.customer_po_nbr,
                    EDW22.dflt_service_coverage_code,
                    EDW22.dflt_service_duration,
                    EDW22.dflt_service_duration_cnt,
                    EDW22.dflt_service_product_id,
                    EDW22.dflt_wrnty_coverage_code,
                    EDW22.dflt_wrnty_product_desc,
                    EDW22.dflt_wrnty_product_id,
                    EDW22.dflt_wrnty_term_mth_cnt,
                    EDW22.external_reference_nbr,
                    CXREF.currency_code,
                    EDW22.gsdb_org_code,
                    EDW22.install_date,
                    EDW22.install_loc_oper_unit_id,
                    EDW22.install_location_id,
                    EDW22.install_location_type_code,
                    EDW22.inventory_item_id,
                    EDW22.inventory_master_org_id,
                    EDW22.inventory_org_id,
                    EDW22.invtry_item_desc,
                    EDW22.invtry_item_flag,
                    EDW22.invtry_item_sponsor_loc_id,
                    EDW22.invtry_item_status_code,
                    EDW22.invtry_org_tran_code,
                    EDW22.invtry_pass_nbr,
                    EDW22.invtry_tran_code,
                    EDW22.invtry_uom_code,
                    EDW22.item_instance_status_desc,
                    EDW22.item_instance_status_id,
                    EDW22.item_instance_status_name,
                    EDW22.item_instance_tran_code,
                    EDW22.item_type_name,
                    EDW22.last_order_line_id,
                    EDW22.last_valid_invtry_org_id,
                    EDW22.location_id,
                    EDW22.location_type_code,
                    EDW22.nl_trackable_flag,
                    EDW22.offering_acctg_type_code,
                    EDW22.oper_unit_country_code,
                    EDW22.operating_unit_id,
                    EDW22.operating_unit_name,
                    EDW22.order_date,
                    EDW22.order_header_id,
                    EDW22.order_line_nbr,
                    EDW22.order_nbr,
                    EDW22.prod_act_order_end_date,
                    EDW22.prod_act_order_start_date,
                    EDW22.prod_act_svc_end_date,
                    EDW22.prod_act_svc_start_date,
                    EDW22.product_class,
                    EDW22.product_class_model,
                    EDW22.product_id,
                    EDW22.product_id_formatted,
                    EDW22.product_model,
                    EDW22.product_source_org_id,
                    EDW22.product_submodel,
                    EDW22.region_code,
                    EDW22.region_desc,
                    EDW22.return_by_date,
                    null,
                    EDW22.serial_nbr_control_code,
                    EDW22.service_order_allowed_flag,
                    EDW22.serviceable_product_flag,
                    EDW22.ship_to_site_use_id,
                    EDW22.shippable_item_flag,
                    EDW22.status_tran_code,
                    EDW22.svc_act_order_end_date,
                    EDW22.svc_act_order_start_date,
                    EDW22.terminated_flag,
                    EDW22.version_label_date,
                    EDW22.version_label_desc,
                    EDW22.version_label_name,
                    EDW22.vldtn_inventory_org_id,
                    EDW22.vrsn_lbl_tran_code,
                    EDW22.invtry_item_desc_unc,
                    EDW22.prev_site_nbr,
                    EDW22.service_tier_name,
                    EDW22.esd_flag,
                    EDW22.media_type,
                    EDW22.license_model,
                    EDW22.license_start_date,
                    EDW22.license_end_date,
                EDW22.cit_vendor_code,        
                EDW22.mdm_product_identifier, 
                EDW22.mdm_solution_identifier
 
                FROM
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t22    AS EDW22  
                LEFT OUTER  JOIN
                    (select * from ${EEDDWWDDBB1}.currency_xref  where upper(trim(data_source_code)) = 'COS')  AS CXREF  
                        ON EDW22.country_code = CXREF.country_code;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t20 ALL

--Translated Query: SUCCESS W8c

    ---TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t20;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_24a.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t24a ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24a;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.product_price FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t24a(      gsdb_org_code     ,product_id     ,svc_bmr_amt_ent     ,svc_bmr_curr_code)SELECT      gsdb_org_code     ,product_id     ,prod_value_amt_ent     ,currency_codeFROM ${EEDDWWDDBB1}.product_priceWHERE price_type_desc = 'BMR'AND product_price_start_date IS NOT NULLAND (    gsdb_org_code  ,product_id ,product_price_start_date) IN    (SELECT       gsdb_org_code      ,product_id     ,MAX(product_price_start_date)      FROM ${EEDDWWDDBB1}.product_price   WHERE price_type_desc = 'BMR'   AND product_price_start_date IS NOT NULL    GROUP BY 1,2)

--Translated Query: MANUAL

    with qq1 as (select * from ${EEDDWWDDBB1}.product_price where upper(trim(price_type_desc)) = 'BMR' and  product_price_start_date  IS NOT NULL)
            INSERT  
            INTO
                TABLE
                ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24a           SELECT
                    gsdb_org_code,
                    product_id,
                    prod_value_amt_ent,
                    currency_code  
                FROM
                    qq1  
                INNER JOIN
                    (
                        SELECT
                            gsdb_org_code AS auto_c00,
                            product_id AS auto_c01,
                            MAX( product_price_start_date ) AS auto_c02  
                        FROM
                            qq1                         
                        GROUP BY
                            gsdb_org_code ,
                            product_id  
                    ) AS autoAlias_46  
                        ON (
                            upper(trim(gsdb_org_code)) = upper(trim(autoAlias_46.AUTO_C00))  
                            AND upper(trim(product_id)) = upper(trim(autoAlias_46.AUTO_C01))  
                            AND product_price_start_date = autoAlias_46.AUTO_C02  
                        );

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.product_price FOR ACCESS   INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t24a    ( gsdb_org_code  ,product_id     ,svc_bmr_amt_ent    ,svc_bmr_curr_code )SELECT   gsdb_org_code  ,product_id     ,prod_value_amt_ent     ,currency_codeFROM  ${EEDDWWDDBB1}.product_priceWHERE price_type_desc = 'BMR'AND   product_price_start_date IS NULLAND   (                                              gsdb_org_code                                  ,product_id                                       ) NOT IN (    SELECT                                   gsdb_org_code                          ,product_id                             FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t24a     GROUP BY 1,2                                    )

--Translated Query: SUCCESS

    WITH qq1 AS (    SELECT
                gsdb_org_code,
                product_id,
                prod_value_amt_ent,
                currency_code  
            FROM
                ${EEDDWWDDBB1}.product_price  
            LEFT OUTER JOIN
                (SELECT
                    gsdb_org_code AS auto_c00,
                    product_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24a  
                GROUP BY
                    gsdb_org_code ,
                    product_id ) AS autoAlias_48  
                    ON ( upper(trim(gsdb_org_code)) = upper(trim(autoAlias_48.AUTO_C00))  
                    AND upper(trim(product_id)) = upper(trim(autoAlias_48.AUTO_C01)) )  
            WHERE
                upper(trim(price_type_desc)) = 'BMR'  
                AND product_price_start_date  IS NULL  
                AND autoAlias_48.AUTO_C00 IS  NULL  
                AND autoAlias_48.AUTO_C01 IS  NULL          ) INSERT  
                INTO
                    TABLE
                    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24a       SELECT
                        *  
                    FROM
                        qq1;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_24b.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t23 COLUMN gsdb_org_code

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t23 COMPUTE STATISTICS  FOR COLUMNS gsdb_org_code;

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t23 COLUMN dflt_service_product_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t23 COMPUTE STATISTICS  FOR COLUMNS dflt_service_product_id;

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t24a COLUMN gsdb_org_code

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24a COMPUTE STATISTICS  FOR COLUMNS gsdb_org_code;

--Original Query:
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t24a COLUMN product_id

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24a COMPUTE STATISTICS  FOR COLUMNS product_id;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t24b ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24b;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_dn_t23 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_dn_t24a FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t24b(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,area_code     ,area_desc     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,country_desc     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_coverage_code     ,dflt_service_duration     ,dflt_service_duration_cnt     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_desc     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,func_curr_code     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,region_code     ,region_desc     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,svc_act_order_end_date     ,svc_act_order_start_date     ,svc_bmr_amt_ent     ,svc_bmr_curr_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr       ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW23.instance_id     ,EDW23.item_instance_id     ,EDW23.active_end_date     ,EDW23.active_start_date     ,EDW23.actual_return_date     ,EDW23.actual_ship_date     ,EDW23.area_code     ,EDW23.area_desc     ,EDW23.bill_to_site_use_id     ,EDW23.bom_enabled_flag     ,EDW23.country_code     ,EDW23.country_desc     ,EDW23.crnt_loc_cs_fml_org_code     ,EDW23.customer_po_date     ,EDW23.customer_po_nbr     ,EDW23.dflt_service_coverage_code     ,EDW23.dflt_service_duration     ,EDW23.dflt_service_duration_cnt     ,EDW23.dflt_service_product_id     ,EDW23.dflt_wrnty_coverage_code     ,EDW23.dflt_wrnty_product_desc     ,EDW23.dflt_wrnty_product_id     ,EDW23.dflt_wrnty_term_mth_cnt     ,EDW23.func_curr_code     ,EDW23.external_reference_nbr     ,EDW23.gsdb_org_code     ,EDW23.install_date     ,EDW23.install_loc_oper_unit_id     ,EDW23.install_location_id     ,EDW23.install_location_type_code     ,EDW23.inventory_item_id     ,EDW23.inventory_master_org_id     ,EDW23.inventory_org_id     ,EDW23.invtry_item_desc     ,EDW23.invtry_item_flag     ,EDW23.invtry_item_sponsor_loc_id     ,EDW23.invtry_item_status_code     ,EDW23.invtry_org_tran_code     ,EDW23.invtry_pass_nbr     ,EDW23.invtry_tran_code     ,EDW23.invtry_uom_code     ,EDW23.item_instance_status_desc     ,EDW23.item_instance_status_id     ,EDW23.item_instance_status_name     ,EDW23.item_instance_tran_code     ,EDW23.item_type_name     ,EDW23.last_order_line_id     ,EDW23.last_valid_invtry_org_id     ,EDW23.location_id     ,EDW23.location_type_code     ,EDW23.nl_trackable_flag     ,EDW23.offering_acctg_type_code     ,EDW23.oper_unit_country_code     ,EDW23.operating_unit_id     ,EDW23.operating_unit_name     ,EDW23.order_date     ,EDW23.order_header_id     ,EDW23.order_line_nbr     ,EDW23.order_nbr     ,EDW23.prod_act_order_end_date     ,EDW23.prod_act_order_start_date     ,EDW23.prod_act_svc_end_date     ,EDW23.prod_act_svc_start_date     ,EDW23.product_class     ,EDW23.product_class_model     ,EDW23.product_id     ,EDW23.product_id_formatted     ,EDW23.product_model     ,EDW23.product_source_org_id     ,EDW23.product_submodel     ,EDW23.region_code     ,EDW23.region_desc     ,EDW23.return_by_date     ,EDW23.serial_nbr_control_code     ,EDW23.service_order_allowed_flag     ,EDW23.serviceable_product_flag     ,EDW23.ship_to_site_use_id     ,EDW23.shippable_item_flag     ,EDW23.status_tran_code     ,EDW23.svc_act_order_end_date     ,EDW23.svc_act_order_start_date     ,EDW24A.svc_bmr_amt_ent     ,EDW24A.svc_bmr_curr_code     ,EDW23.terminated_flag     ,EDW23.version_label_date     ,EDW23.version_label_desc     ,EDW23.version_label_name     ,EDW23.vldtn_inventory_org_id     ,EDW23.vrsn_lbl_tran_code     ,EDW23.invtry_item_desc_unc     ,EDW23.prev_site_nbr         ,EDW23.service_tier_name,   EDW23.esd_flag,                        EDW23.media_type,                      EDW23.license_model,                   EDW23.license_start_date,              EDW23.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t23 EDW23,${AAPPLLIIDD1EENNVV}_install_base_dn_t24a EDW24AWHERE  EDW23.gsdb_org_code = EDW24A.gsdb_org_codeAND EDW23.dflt_service_product_id = EDW24A.product_idAND EDW23.gsdb_org_code IS NOT NULL

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24b           SELECT
            EDW23.instance_id,
            EDW23.item_instance_id,
            EDW23.active_end_date,
            EDW23.active_start_date,
            EDW23.actual_return_date,
            EDW23.actual_ship_date,
            EDW23.area_code,
            EDW23.area_desc,
            EDW23.bill_to_site_use_id,
            EDW23.bom_enabled_flag,
            EDW23.country_code,
            EDW23.country_desc,
            EDW23.crnt_loc_cs_fml_org_code,
            EDW23.customer_po_date,
            EDW23.customer_po_nbr,
            EDW23.dflt_service_coverage_code,
            EDW23.dflt_service_duration,
            EDW23.dflt_service_duration_cnt,
            EDW23.dflt_service_product_id,
            EDW23.dflt_wrnty_coverage_code,
            EDW23.dflt_wrnty_product_desc,
            EDW23.dflt_wrnty_product_id,
            EDW23.dflt_wrnty_term_mth_cnt,
            EDW23.external_reference_nbr,
            EDW23.func_curr_code,
            EDW23.gsdb_org_code,
            EDW23.install_date,
            EDW23.install_loc_oper_unit_id,
            EDW23.install_location_id,
            EDW23.install_location_type_code,
            EDW23.inventory_item_id,
            EDW23.inventory_master_org_id,
            (Case when EDW23.gsdb_org_code  IS NOT NULL then EDW23.inventory_org_id else null end),
            EDW23.invtry_item_desc,
            EDW23.invtry_item_flag,
            EDW23.invtry_item_sponsor_loc_id,
            EDW23.invtry_item_status_code,
            EDW23.invtry_org_tran_code,
            EDW23.invtry_pass_nbr,
            EDW23.invtry_tran_code,
            EDW23.invtry_uom_code,
            EDW23.item_instance_status_desc,
            EDW23.item_instance_status_id,
            EDW23.item_instance_status_name,
            EDW23.item_instance_tran_code,
            EDW23.item_type_name,
            EDW23.last_order_line_id,
            EDW23.last_valid_invtry_org_id,
            EDW23.location_id,
            EDW23.location_type_code,
            EDW23.nl_trackable_flag,
            EDW23.offering_acctg_type_code,
            EDW23.oper_unit_country_code,
            EDW23.operating_unit_id,
            EDW23.operating_unit_name,
            EDW23.order_date,
            EDW23.order_header_id,
            EDW23.order_line_nbr,
            EDW23.order_nbr,
            EDW23.prod_act_order_end_date,
            EDW23.prod_act_order_start_date,
            EDW23.prod_act_svc_end_date,
            EDW23.prod_act_svc_start_date,
            EDW23.product_class,
            EDW23.product_class_model,
            EDW23.product_id,
            EDW23.product_id_formatted,
            EDW23.product_model,
            EDW23.product_source_org_id,
            EDW23.product_submodel,
            EDW23.region_code,
            EDW23.region_desc,
            EDW23.return_by_date,
            null,
            EDW23.serial_nbr_control_code,
            EDW23.service_order_allowed_flag,
            EDW23.serviceable_product_flag,
            EDW23.ship_to_site_use_id,
            EDW23.shippable_item_flag,
            EDW23.status_tran_code,
            EDW23.svc_act_order_end_date,
            EDW23.svc_act_order_start_date,
            (Case when EDW23.gsdb_org_code  IS NOT NULL then EDW24A.svc_bmr_amt_ent else null end),
            (Case when EDW23.gsdb_org_code  IS NOT NULL then EDW24A.svc_bmr_curr_code else null end),
            EDW23.terminated_flag,
            EDW23.version_label_date,
            EDW23.version_label_desc,
            EDW23.version_label_name,
            EDW23.vldtn_inventory_org_id,
            EDW23.vrsn_lbl_tran_code,
            EDW23.invtry_item_desc_unc,
            EDW23.prev_site_nbr,
            EDW23.service_tier_name,
            EDW23.esd_flag,
            EDW23.media_type,
            EDW23.license_model,
            EDW23.license_start_date,
            EDW23.license_end_date,
            EDW23.cit_vendor_code,
            EDW23.mdm_product_identifier,
            EDW23.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t23    AS EDW23   LEFT OUTER JOIN
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24a    AS EDW24A   
        on
            EDW23.gsdb_org_code = EDW24A.gsdb_org_code  
            AND EDW23.dflt_service_product_id = EDW24A.product_id ;

--Original Query:
  ----INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t24b(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,area_code     ,area_desc     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,country_desc     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_coverage_code     ,dflt_service_duration     ,dflt_service_duration_cnt     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_desc     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,func_curr_code     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,region_code     ,region_desc     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,svc_act_order_end_date     ,svc_act_order_start_date     ,svc_bmr_amt_ent     ,svc_bmr_curr_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr         ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW23.instance_id     ,EDW23.item_instance_id     ,EDW23.active_end_date     ,EDW23.active_start_date     ,EDW23.actual_return_date     ,EDW23.actual_ship_date     ,EDW23.area_code     ,EDW23.area_desc     ,EDW23.bill_to_site_use_id     ,EDW23.bom_enabled_flag     ,EDW23.country_code     ,EDW23.country_desc     ,EDW23.crnt_loc_cs_fml_org_code     ,EDW23.customer_po_date     ,EDW23.customer_po_nbr     ,EDW23.dflt_service_coverage_code     ,EDW23.dflt_service_duration     ,EDW23.dflt_service_duration_cnt     ,EDW23.dflt_service_product_id     ,EDW23.dflt_wrnty_coverage_code     ,EDW23.dflt_wrnty_product_desc     ,EDW23.dflt_wrnty_product_id     ,EDW23.dflt_wrnty_term_mth_cnt     ,EDW23.func_curr_code     ,EDW23.external_reference_nbr     ,EDW23.gsdb_org_code     ,EDW23.install_date     ,EDW23.install_loc_oper_unit_id     ,EDW23.install_location_id     ,EDW23.install_location_type_code     ,EDW23.inventory_item_id     ,EDW23.inventory_master_org_id     ,EDW23.invtry_item_desc     ,EDW23.invtry_item_flag     ,EDW23.invtry_item_sponsor_loc_id     ,EDW23.invtry_item_status_code     ,EDW23.invtry_org_tran_code     ,EDW23.invtry_pass_nbr     ,EDW23.invtry_tran_code     ,EDW23.invtry_uom_code     ,EDW23.item_instance_status_desc     ,EDW23.item_instance_status_id     ,EDW23.item_instance_status_name     ,EDW23.item_instance_tran_code     ,EDW23.item_type_name     ,EDW23.last_order_line_id     ,EDW23.last_valid_invtry_org_id     ,EDW23.location_id     ,EDW23.location_type_code     ,EDW23.nl_trackable_flag     ,EDW23.offering_acctg_type_code     ,EDW23.oper_unit_country_code     ,EDW23.operating_unit_id     ,EDW23.operating_unit_name     ,EDW23.order_date     ,EDW23.order_header_id     ,EDW23.order_line_nbr     ,EDW23.order_nbr     ,EDW23.prod_act_order_end_date     ,EDW23.prod_act_order_start_date     ,EDW23.prod_act_svc_end_date     ,EDW23.prod_act_svc_start_date     ,EDW23.product_class     ,EDW23.product_class_model     ,EDW23.product_id     ,EDW23.product_id_formatted     ,EDW23.product_model     ,EDW23.product_source_org_id     ,EDW23.product_submodel     ,EDW23.region_code     ,EDW23.region_desc     ,EDW23.return_by_date     ,EDW23.serial_nbr_control_code     ,EDW23.service_order_allowed_flag     ,EDW23.serviceable_product_flag     ,EDW23.ship_to_site_use_id     ,EDW23.shippable_item_flag     ,EDW23.status_tran_code     ,EDW23.svc_act_order_end_date     ,EDW23.svc_act_order_start_date     <WM_COMMENT_START> svc_bmr_amt_ent <WM_COMMENT_END>     ,NULL     <WM_COMMENT_START> svc_bmr_curr_code <WM_COMMENT_END>     ,NULL     ,EDW23.terminated_flag     ,EDW23.version_label_date     ,EDW23.version_label_desc     ,EDW23.version_label_name     ,EDW23.vldtn_inventory_org_id     ,EDW23.vrsn_lbl_tran_code     ,EDW23.invtry_item_desc_unc     ,EDW23.prev_site_nbr         ,EDW23.service_tier_name,   EDW23.esd_flag,                        EDW23.media_type,                      EDW23.license_model,                   EDW23.license_start_date,              EDW23.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t23 EDW23WHERE (instance_id, item_instance_id) NOT IN (    SELECT instance_id, item_instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t24b)

--Translated Query: SUCCESS

    ----WITH qq1 AS (    SELECT
    ----    EDW23.instance_id,
    ----    EDW23.item_instance_id,
    ----    EDW23.active_end_date,
    ----    EDW23.active_start_date,
    ----    EDW23.actual_return_date,
    ----    EDW23.actual_ship_date,
    ----    EDW23.area_code,
    ----    EDW23.area_desc,
    ----    EDW23.bill_to_site_use_id,
    ----    EDW23.bom_enabled_flag,
    ----    EDW23.country_code,
    ----    EDW23.country_desc,
    ----    EDW23.crnt_loc_cs_fml_org_code,
    ----    EDW23.customer_po_date,
    ----    EDW23.customer_po_nbr,
    ----    EDW23.dflt_service_coverage_code,
    ----    EDW23.dflt_service_duration,
    ----    EDW23.dflt_service_duration_cnt,
    ----    EDW23.dflt_service_product_id,
    ----    EDW23.dflt_wrnty_coverage_code,
    ----    EDW23.dflt_wrnty_product_desc,
    ----    EDW23.dflt_wrnty_product_id,
    ----    EDW23.dflt_wrnty_term_mth_cnt,
    ----    EDW23.external_reference_nbr,
    ----    EDW23.func_curr_code,
    ----    EDW23.gsdb_org_code,
    ----    EDW23.install_date,
    ----    EDW23.install_loc_oper_unit_id,
    ----    EDW23.install_location_id,
    ----    EDW23.install_location_type_code,
    ----    EDW23.inventory_item_id,
    ----    EDW23.inventory_master_org_id,
    ----    null,
    ----    EDW23.invtry_item_desc,
    ----    EDW23.invtry_item_flag,
    ----    EDW23.invtry_item_sponsor_loc_id,
    ----    EDW23.invtry_item_status_code,
    ----    EDW23.invtry_org_tran_code,
    ----    EDW23.invtry_pass_nbr,
    ----    EDW23.invtry_tran_code,
    ----    EDW23.invtry_uom_code,
    ----    EDW23.item_instance_status_desc,
    ----    EDW23.item_instance_status_id,
    ----    EDW23.item_instance_status_name,
    ----    EDW23.item_instance_tran_code,
    ----    EDW23.item_type_name,
    ----    EDW23.last_order_line_id,
    ----    EDW23.last_valid_invtry_org_id,
    ----    EDW23.location_id,
    ----    EDW23.location_type_code,
    ----    EDW23.nl_trackable_flag,
    ----    EDW23.offering_acctg_type_code,
    ----    EDW23.oper_unit_country_code,
    ----    EDW23.operating_unit_id,
    ----    EDW23.operating_unit_name,
    ----    EDW23.order_date,
    ----    EDW23.order_header_id,
    ----    EDW23.order_line_nbr,
    ----    EDW23.order_nbr,
    ----    EDW23.prod_act_order_end_date,
    ----    EDW23.prod_act_order_start_date,
    ----    EDW23.prod_act_svc_end_date,
    ----    EDW23.prod_act_svc_start_date,
    ----    EDW23.product_class,
    ----    EDW23.product_class_model,
    ----    EDW23.product_id,
    ----    EDW23.product_id_formatted,
    ----    EDW23.product_model,
    ----    EDW23.product_source_org_id,
    ----    EDW23.product_submodel,
    ----    EDW23.region_code,
    ----    EDW23.region_desc,
    ----    EDW23.return_by_date,
    ----    null,
    ----    EDW23.serial_nbr_control_code,
    ----    EDW23.service_order_allowed_flag,
    ----    EDW23.serviceable_product_flag,
    ----    EDW23.ship_to_site_use_id,
    ----    EDW23.shippable_item_flag,
    ----    EDW23.status_tran_code,
    ----    EDW23.svc_act_order_end_date,
    ----    EDW23.svc_act_order_start_date,
    ----    CAST( NULL AS STRING ),
    ----    CAST( NULL AS STRING ),
    ----    EDW23.terminated_flag,
    ----    EDW23.version_label_date,
    ----    EDW23.version_label_desc,
    ----    EDW23.version_label_name,
    ----    EDW23.vldtn_inventory_org_id,
    ----    EDW23.vrsn_lbl_tran_code,
    ----    EDW23.invtry_item_desc_unc,
    ----    EDW23.prev_site_nbr,
    ----    EDW23.service_tier_name,
    ----    EDW23.esd_flag,
    ----    EDW23.media_type,
    ----    EDW23.license_model,
    ----    EDW23.license_start_date,
    ----    EDW23.license_end_date,
    ----    EDW23.cit_vendor_code,
    ----    EDW23.mdm_product_identifier,
    ----    EDW23.mdm_solution_identifier
    ----FROM
    ----    ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t23    AS EDW23 
    ----LEFT OUTER JOIN
    ----    (SELECT
    ----        DISTINCT instance_id AS auto_c00,
    ----        item_instance_id AS auto_c01  
    ----    FROM
    ----        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24b ) AS autoAlias_50 
    ----        ON ( upper(trim(instance_id)) = upper(trim(autoAlias_50.AUTO_C00)) 
    ----        AND item_instance_id = autoAlias_50.AUTO_C01 ) 
    ----WHERE
    ----    autoAlias_50.AUTO_C00 IS  NULL  
    ----    AND autoAlias_50.AUTO_C01 IS  NULL         ) INSERT 
    ----    INTO
    ----        TABLE
    ----        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24b       SELECT
    ----            * 
    ----        FROM
    ----            qq1;
    ----
--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t21 ALL

--Translated Query: SUCCESS w8c

   ---- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t21;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_dn_25.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t25 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t25;

--Original Query:
  ----COLLECT STAT ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t24b COLUMN country_code

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24b COMPUTE STATISTICS  FOR COLUMNS country_code;

--Original Query:
  ----COLLECT STAT ON ${AAPPLLIIDD1EENNVV}_install_base_dn_t24b COLUMN svc_bmr_curr_code

--Translated Query: SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24b COMPUTE STATISTICS  FOR COLUMNS svc_bmr_curr_code;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.cfs_curr_calc_country FOR ACCESS      LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_dn_t24b FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_dn_t25(      instance_id     ,item_instance_id     ,active_end_date     ,active_start_date     ,actual_return_date     ,actual_ship_date     ,area_code     ,area_desc     ,bill_to_site_use_id     ,bom_enabled_flag     ,country_code     ,country_desc     ,crnt_loc_cs_fml_org_code     ,customer_po_date     ,customer_po_nbr     ,dflt_service_coverage_code     ,dflt_service_duration     ,dflt_service_duration_cnt     ,dflt_service_product_id     ,dflt_wrnty_coverage_code     ,dflt_wrnty_product_desc     ,dflt_wrnty_product_id     ,dflt_wrnty_term_mth_cnt     ,func_curr_code     ,external_reference_nbr     ,gsdb_org_code     ,install_date     ,install_loc_oper_unit_id     ,install_location_id     ,install_location_type_code     ,inventory_item_id     ,inventory_master_org_id     ,inventory_org_id     ,invtry_item_desc     ,invtry_item_flag     ,invtry_item_sponsor_loc_id     ,invtry_item_status_code     ,invtry_org_tran_code     ,invtry_pass_nbr     ,invtry_tran_code     ,invtry_uom_code     ,item_instance_status_desc     ,item_instance_status_id     ,item_instance_status_name     ,item_instance_tran_code     ,item_type_name     ,last_order_line_id     ,last_valid_invtry_org_id     ,location_id     ,location_type_code     ,nl_trackable_flag     ,offering_acctg_type_code     ,oper_unit_country_code     ,operating_unit_id     ,operating_unit_name     ,order_date     ,order_header_id     ,order_line_nbr     ,order_nbr     ,prod_act_order_end_date     ,prod_act_order_start_date     ,prod_act_svc_end_date     ,prod_act_svc_start_date     ,product_class     ,product_class_model     ,product_id     ,product_id_formatted     ,product_model     ,product_source_org_id     ,product_submodel     ,region_code     ,region_desc     ,return_by_date     ,serial_nbr_control_code     ,service_order_allowed_flag     ,serviceable_product_flag     ,ship_to_site_use_id     ,shippable_item_flag     ,status_tran_code     ,svc_act_order_end_date     ,svc_act_order_start_date     ,svc_bmr_amt_ent     ,svc_bmr_amt_euro     ,svc_bmr_amt_func     ,svc_bmr_amt_us     ,svc_bmr_curr_code     ,terminated_flag     ,version_label_date     ,version_label_desc     ,version_label_name     ,vldtn_inventory_org_id     ,vrsn_lbl_tran_code     <WM_COMMENT_START> SDI TAR to add Unicode field <WM_COMMENT_END>     ,invtry_item_desc_unc     ,prev_site_nbr         ,service_tier_name,     esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT      EDW24B.instance_id     ,EDW24B.item_instance_id     ,EDW24B.active_end_date     ,EDW24B.active_start_date     ,EDW24B.actual_return_date     ,EDW24B.actual_ship_date     ,EDW24B.area_code     ,EDW24B.area_desc     ,EDW24B.bill_to_site_use_id     ,EDW24B.bom_enabled_flag     ,EDW24B.country_code     ,EDW24B.country_desc     ,EDW24B.crnt_loc_cs_fml_org_code     ,EDW24B.customer_po_date     ,EDW24B.customer_po_nbr     ,EDW24B.dflt_service_coverage_code     ,EDW24B.dflt_service_duration     ,EDW24B.dflt_service_duration_cnt     ,EDW24B.dflt_service_product_id     ,EDW24B.dflt_wrnty_coverage_code     ,EDW24B.dflt_wrnty_product_desc     ,EDW24B.dflt_wrnty_product_id     ,EDW24B.dflt_wrnty_term_mth_cnt     ,EDW24B.func_curr_code     ,EDW24B.external_reference_nbr     ,EDW24B.gsdb_org_code     ,EDW24B.install_date     ,EDW24B.install_loc_oper_unit_id     ,EDW24B.install_location_id     ,EDW24B.install_location_type_code     ,EDW24B.inventory_item_id     ,EDW24B.inventory_master_org_id     ,EDW24B.inventory_org_id     ,EDW24B.invtry_item_desc     ,EDW24B.invtry_item_flag     ,EDW24B.invtry_item_sponsor_loc_id     ,EDW24B.invtry_item_status_code     ,EDW24B.invtry_org_tran_code     ,EDW24B.invtry_pass_nbr     ,EDW24B.invtry_tran_code     ,EDW24B.invtry_uom_code     ,EDW24B.item_instance_status_desc     ,EDW24B.item_instance_status_id     ,EDW24B.item_instance_status_name     ,EDW24B.item_instance_tran_code     ,EDW24B.item_type_name     ,EDW24B.last_order_line_id     ,EDW24B.last_valid_invtry_org_id     ,EDW24B.location_id     ,EDW24B.location_type_code     ,EDW24B.nl_trackable_flag     ,EDW24B.offering_acctg_type_code     ,EDW24B.oper_unit_country_code     ,EDW24B.operating_unit_id     ,EDW24B.operating_unit_name     ,EDW24B.order_date     ,EDW24B.order_header_id     ,EDW24B.order_line_nbr     ,EDW24B.order_nbr     ,EDW24B.prod_act_order_end_date     ,EDW24B.prod_act_order_start_date     ,EDW24B.prod_act_svc_end_date     ,EDW24B.prod_act_svc_start_date     ,EDW24B.product_class     ,EDW24B.product_class_model     ,EDW24B.product_id     ,EDW24B.product_id_formatted     ,EDW24B.product_model     ,EDW24B.product_source_org_id     ,EDW24B.product_submodel     ,EDW24B.region_code     ,EDW24B.region_desc     ,EDW24B.return_by_date     ,EDW24B.serial_nbr_control_code     ,EDW24B.service_order_allowed_flag     ,EDW24B.serviceable_product_flag     ,EDW24B.ship_to_site_use_id     ,EDW24B.shippable_item_flag     ,EDW24B.status_tran_code     ,EDW24B.svc_act_order_end_date     ,EDW24B.svc_act_order_start_date     ,EDW24B.svc_bmr_amt_ent     <WM_COMMENT_START> svc_bmr_amt_euro <WM_COMMENT_END>     ,EDW24B.svc_bmr_amt_ent * CCCC.euro_average_rate     <WM_COMMENT_START> svc_bmr_amt_func <WM_COMMENT_END>     ,EDW24B.svc_bmr_amt_ent * CCCC.local_average_rate     <WM_COMMENT_START> svc_bmr_amt_us <WM_COMMENT_END>     ,EDW24B.svc_bmr_amt_ent * CCCC.us_average_rate     ,EDW24B.svc_bmr_curr_code     ,EDW24B.terminated_flag     ,EDW24B.version_label_date     ,EDW24B.version_label_desc     ,EDW24B.version_label_name     ,EDW24B.vldtn_inventory_org_id     ,EDW24B.vrsn_lbl_tran_code     ,EDW24B.invtry_item_desc_unc     ,EDW24B.prev_site_nbr         ,EDW24B.service_tier_name,  EDW24B.esd_flag,                        EDW24B.media_type,                      EDW24B.license_model,                   EDW24B.license_start_date,              EDW24B.license_end_date           FROM  ${AAPPLLIIDD1EENNVV}_install_base_dn_t24b EDW24BLEFT OUTER JOIN${EEDDWWDDBB1}.cfs_curr_calc_country CCCCON  EDW24B.country_code = CCCC.country_codeAND EDW24B.svc_bmr_curr_code = CCCC.translate_from_curr_code

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t25           SELECT
            EDW24B.instance_id,
            EDW24B.item_instance_id,
            EDW24B.active_end_date,
            EDW24B.active_start_date,
            EDW24B.actual_return_date,
            EDW24B.actual_ship_date,
            EDW24B.area_code,
            EDW24B.area_desc,
            EDW24B.bill_to_site_use_id,
            EDW24B.bom_enabled_flag,
            EDW24B.country_code,
            EDW24B.country_desc,
            EDW24B.crnt_loc_cs_fml_org_code,
            EDW24B.customer_po_date,
            EDW24B.customer_po_nbr,
            EDW24B.dflt_service_coverage_code,
            EDW24B.dflt_service_duration,
            EDW24B.dflt_service_duration_cnt,
            EDW24B.dflt_service_product_id,
            EDW24B.dflt_wrnty_coverage_code,
            EDW24B.dflt_wrnty_product_desc,
            EDW24B.dflt_wrnty_product_id,
            EDW24B.dflt_wrnty_term_mth_cnt,
            EDW24B.external_reference_nbr,
            EDW24B.func_curr_code,
            EDW24B.gsdb_org_code,
            EDW24B.install_date,
            EDW24B.install_loc_oper_unit_id,
            EDW24B.install_location_id,
            EDW24B.install_location_type_code,
            EDW24B.inventory_item_id,
            EDW24B.inventory_master_org_id,
            EDW24B.inventory_org_id,
            EDW24B.invtry_item_desc,
            EDW24B.invtry_item_flag,
            EDW24B.invtry_item_sponsor_loc_id,
            EDW24B.invtry_item_status_code,
            EDW24B.invtry_org_tran_code,
            EDW24B.invtry_pass_nbr,
            EDW24B.invtry_tran_code,
            EDW24B.invtry_uom_code,
            EDW24B.item_instance_status_desc,
            EDW24B.item_instance_status_id,
            EDW24B.item_instance_status_name,
            EDW24B.item_instance_tran_code,
            EDW24B.item_type_name,
            EDW24B.last_order_line_id,
            EDW24B.last_valid_invtry_org_id,
            EDW24B.location_id,
            EDW24B.location_type_code,
            EDW24B.nl_trackable_flag,
            EDW24B.offering_acctg_type_code,
            EDW24B.oper_unit_country_code,
            EDW24B.operating_unit_id,
            EDW24B.operating_unit_name,
            EDW24B.order_date,
            EDW24B.order_header_id,
            EDW24B.order_line_nbr,
            EDW24B.order_nbr,
            EDW24B.prod_act_order_end_date,
            EDW24B.prod_act_order_start_date,
            EDW24B.prod_act_svc_end_date,
            EDW24B.prod_act_svc_start_date,
            EDW24B.product_class,
            EDW24B.product_class_model,
            EDW24B.product_id,
            EDW24B.product_id_formatted,
            EDW24B.product_model,
            EDW24B.product_source_org_id,
            EDW24B.product_submodel,
            EDW24B.region_code,
            EDW24B.region_desc,
            EDW24B.return_by_date,
            null,
            EDW24B.serial_nbr_control_code,
            EDW24B.service_order_allowed_flag,
            EDW24B.serviceable_product_flag,
            EDW24B.ship_to_site_use_id,
            EDW24B.shippable_item_flag,
            EDW24B.status_tran_code,
            EDW24B.svc_act_order_end_date,
            EDW24B.svc_act_order_start_date,
            EDW24B.svc_bmr_amt_ent,
            EDW24B.svc_bmr_amt_ent * CCCC.euro_average_rate AS auto_c082,
            EDW24B.svc_bmr_amt_ent * CCCC.local_average_rate AS auto_c083,
            EDW24B.svc_bmr_amt_ent * CCCC.us_average_rate AS auto_c084,
            EDW24B.svc_bmr_curr_code,
            EDW24B.terminated_flag,
            EDW24B.version_label_date,
            EDW24B.version_label_desc,
            EDW24B.version_label_name,
            EDW24B.vldtn_inventory_org_id,
            EDW24B.vrsn_lbl_tran_code,
            EDW24B.invtry_item_desc_unc,
            EDW24B.prev_site_nbr,
            EDW24B.service_tier_name,
            EDW24B.esd_flag,
            EDW24B.media_type,
            EDW24B.license_model,
            EDW24B.license_start_date,
            EDW24B.license_end_date,
            EDW24B.cit_vendor_code,
            EDW24B.mdm_product_identifier,
            EDW24B.mdm_solution_identifier
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t24b    AS EDW24B   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB1}.cfs_curr_calc_country    AS CCCC    
                ON upper(trim(EDW24B.country_code)) = upper(trim(CCCC.country_code)) 
                AND upper(trim(EDW24B.svc_bmr_curr_code)) = upper(trim(CCCC.translate_from_curr_code));

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t22 ALL

--Translated Query: SUCCESS W8c

   ---- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t22;
