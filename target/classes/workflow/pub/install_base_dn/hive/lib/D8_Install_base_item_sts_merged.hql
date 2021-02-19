----------------------------------------------------------------------------------------
---Script Name: d8_install_base_item_sts_ld.ins
---Inputs:  install_base_item_sts
---Outputs: install_base_item_sts_ld 
---
---Modification History:
---Date:	MM/DD/YY	Who:	
---Control:	TAR# 	REL# 
---Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${EEDDWWDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${EEDDWWDDBB1} ;

--Original Query:
  --DELETE FROM install_base_item_sts_ld;

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${EEDDWWDDBB1}.install_base_item_sts_ld;

--Original Query:


--Translated Query: STATUS SUCCESS

---INSERT INTO install_base_item_sts_ld
---SELECT *
---FROM install_base_item_sts
---;

INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_item_sts_ld          
        SELECT
            X.*  
        FROM
            ${EEDDWWDDBB1}.install_base_item_sts X;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_sts_01.ins.sql
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

    use ${TTMMPPDDBB1};

--Original Query:
  ----DELETE FROM AAPPLLIIDD1EENNVV_install_base_item_sts_t1 ALL;
--Translated Query: SUCCESS
Truncate Table ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t1;
	
	
--Original Query:
  ----INSERT INTO AAPPLLIIDD1EENNVV_install_base_item_sts_t1 ( instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, as_of_date_time, batch_nbr, created_by_name, creation_date_time, incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag ) SELECT instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, as_of_date_time, batch_nbr, created_by_name, creation_date_time, incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag FROM AAPPLLIIDD1EENNVV_install_base_item_sts_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr, 	instance_id, 	item_instance_status_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr), 	instance_id, 	item_instance_status_id     FROM AAPPLLIIDD1EENNVV_install_base_item_sts_ml     WHERE tran_code <> 'D'     GROUP BY 2,3 ) AND tran_code <> 'D' ; 
--Translated Query: SUCCESS


INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t1
SELECT 
instance_id,
item_instance_status_id,
active_end_date_time,
active_start_date_time,
as_of_date_time,
batch_nbr,
created_by_name,
creation_date_time,
incident_allowed_flag,
item_instance_status_desc,
item_instance_status_name,
last_update_date_time,
last_update_login_name,
last_updated_by_name,
migrated_flag,
seed_status_update_flag,
seeded_flag,
service_order_allowed_flag,
status_change_allowed_flag,
terminated_flag,
tran_code,
update_flag
FROM ${AAPPLLIIDD1EENNVV}_install_base_item_sts_ml
INNER JOIN
            (
                SELECT
                    MAX( CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                    batch_nbr) ) AS auto_c00,
                    instance_id AS auto_c01,
                    item_instance_status_id AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_ml     
                WHERE
                    upper(trim(tran_code)) <> upper(trim('D'))  
                GROUP BY
                    instance_id ,
                    item_instance_status_id 
            ) AS autoAlias_52 
on 
 CONCAT(CAST (as_of_date_time AS CHAR(26)) ,batch_nbr) = autoAlias_52.AUTO_C00 
                AND upper(trim(instance_id)) = upper(trim(autoAlias_52.AUTO_C01)) 
                AND item_instance_status_id = autoAlias_52.AUTO_C02 
            WHERE
                upper(trim(tran_code)) <> upper(trim('D'));


----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_sts_02.ins
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

    use ${TTMMPPDDBB1};

--Original Query:
  ----DELETE FROM AAPPLLIIDD1EENNVV_install_base_item_sts_t2 ALL;
--Translated Query: SUCCESS
Truncate Table ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2;
	
	
--Original Query:
  ----INSERT INTO AAPPLLIIDD1EENNVV_install_base_item_sts_t2 ( instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, current_record_ind, incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag ) SELECT instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, 'Y', incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag FROM EEDDWWDDBB1.install_base_item_sts_ld WHERE (instance_id, item_instance_status_id ) IN (SELECT     instance_id,     item_instance_status_id     FROM AAPPLLIIDD1EENNVV_install_base_item_sts_t1     ) ;
--Translated Query: SUCCESS


INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2
SELECT 
X.instance_id,
X.item_instance_status_id,
active_end_date_time,
active_start_date_time,
alt_as_of_date_time,
alt_batch_nbr,
as_of_date_time,
batch_nbr,
created_by_name,
creation_date_time,
'Y',
incident_allowed_flag,
item_instance_status_desc,
item_instance_status_name,
last_update_date_time,
last_update_login_name,
last_updated_by_name,
migrated_flag,
seed_status_update_flag,
seeded_flag,
service_order_allowed_flag,
status_change_allowed_flag,
terminated_flag,
tran_code,
update_flag
FROM ${EEDDWWDDBB1}.install_base_item_sts_ld X
INNER JOIN
            (
SELECT Distinct
    instance_id ,
    item_instance_status_id 
    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t1 
            ) AS autoAlias_52 
on 
X.instance_id =  autoAlias_52.instance_id and
X.item_instance_status_id = autoAlias_52.item_instance_status_id;
--Translated Query: SUCCESS
TRUNCATE TABLE ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t3;
--Translated Query: SUCCESS
INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t3
SELECT
A.instance_id,
A.item_instance_status_id,
active_end_date_time,
active_start_date_time,
created_by_name,
creation_date_time,
incident_allowed_flag,
item_instance_status_desc,
item_instance_status_name,
last_update_date_time,
last_update_login_name,
last_updated_by_name,
migrated_flag,
seed_status_update_flag,
seeded_flag,
service_order_allowed_flag,
status_change_allowed_flag,
terminated_flag,
tran_code,
update_flag
FROM ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2 A;

--Translated Query: SUCCESS
use  ${TTMMPPDDBB1};
--Translated Query: SUCCESS
INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t3
SELECT
A.instance_id,
A.item_instance_status_id,
active_end_date_time,
active_start_date_time,
created_by_name,
creation_date_time,
incident_allowed_flag,
item_instance_status_desc,
item_instance_status_name,
last_update_date_time,
last_update_login_name,
last_updated_by_name,
migrated_flag,
seed_status_update_flag,
seeded_flag,
service_order_allowed_flag,
status_change_allowed_flag,
terminated_flag,
tran_code,
update_flag
FROM ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t1 A
INNER JOIN
 (SELECT  DISTINCT
    instance_id,
    item_instance_status_id 
    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2
    ) X
on A.instance_id = X.instance_id and
A.item_instance_status_id = X.item_instance_status_id
;
--Translated Query: SUCCESS
INSERT OVERWRITE TABLE ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t3 SELECT DISTINCT * FROM ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t3;----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_sts_04.upd.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------



--Original Query:
  ----UPDATE AAPPLLIIDD1EENNVV_install_base_item_sts_t2 SET current_record_ind = 'D' WHERE (instance_id, item_instance_status_id )  IN     (SELECT instance_id, item_instance_status_id     FROM AAPPLLIIDD1EENNVV_install_base_item_sts_t3     GROUP BY instance_id, item_instance_status_id     HAVING COUNT(*) = 1     ) ;
--Translated Query: SUCCESS
use ${TTMMPPDDBB1};
--Translated Query: SUCCESS
INSERT OVERWRITE TABLE ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2
SELECT
       A.instance_id AS instance_id ,
            A.item_instance_status_id AS item_instance_status_id ,
            A.active_end_date_time AS active_end_date_time ,
            A.active_start_date_time AS active_start_date_time ,
            A.alt_as_of_date_time AS alt_as_of_date_time ,
            A.alt_batch_nbr AS alt_batch_nbr ,
            A.as_of_date_time AS as_of_date_time ,
            A.batch_nbr AS batch_nbr ,
            A.created_by_name AS created_by_name ,
            A.creation_date_time AS creation_date_time ,
            CASE 
                WHEN X.autoc0 IS not null THEN 'D' 
                ELSE A.current_record_ind 
            END AS current_record_ind ,
A.incident_allowed_flag         
,A.item_instance_status_desc     
,A.item_instance_status_name     
,A.last_update_date_time         
,A.last_update_login_name        
,A.last_updated_by_name          
,A.migrated_flag                 
,A.seed_status_update_flag       
,A.seeded_flag                   
,A.service_order_allowed_flag    
,A.status_change_allowed_flag    
,A.terminated_flag               
,A.tran_code                     
,A.update_flag                   

FROM ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2 A
LEFT OUTER JOIN (SELECT instance_id as autoc0, item_instance_status_id as autoc1
    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_sts_t3
    GROUP BY instance_id, item_instance_status_id
    HAVING COUNT(*) = 1
    ) X
on
A.instance_id = X.autoc0 and
A.item_instance_status_id = X.autoc1
;


----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/31 11:36:31 
--Script Name: d8_install_base_item_sts_05.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${EEDDWWDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${EEDDWWDDBB1} ;

--Original Query:
  --DELETE FROM install_base_item_sts_ld WHERE (instance_id, item_instance_status_id ) IN (SELECT instance_id, item_instance_status_id     FROM TTMMPPDDBB1.AAPPLLIIDD1EENNVV_install_base_item_sts_t1     WHERE (instance_id, item_instance_status_id ) NOT IN     (SELECT instance_id, item_instance_status_id     FROM TTMMPPDDBB1.AAPPLLIIDD1EENNVV_install_base_item_sts_t2     WHERE current_record_ind = 'D'     )
    

--Translated Query: STATUS SUCCESS
with qq1 as (SELECT distinct instance_id, item_instance_status_id FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2  WHERE upper(trim(current_record_ind)) = 'D' ),
qq2 as (SELECT DISTINCT A.instance_id,A.item_instance_status_id from ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t1 A left outer join qq1 B on   A.instance_id = B.instance_id and A.item_instance_status_id = B.item_instance_status_id where B.instance_id is null and B.item_instance_status_id is null)
    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.install_base_item_sts_ld SELECT
            X.* 
        FROM install_base_item_sts_ld X
            left outer join 
             qq2 Y
        on X.instance_id = Y.instance_id AND
           X.item_instance_status_id = Y.item_instance_status_id
        where Y.instance_id is null and Y.item_instance_status_id is null;


--Original Query:
  --INSERT INTO install_base_item_sts_ld ( instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag ) SELECT instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, as_of_date_time, batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag FROM TTMMPPDDBB1.AAPPLLIIDD1EENNVV_install_base_item_sts_t1 WHERE ( instance_id, item_instance_status_id ) NOT IN (SELECT  instance_id, item_instance_status_id     FROM TTMMPPDDBB1.AAPPLLIIDD1EENNVV_install_base_item_sts_t2     WHERE current_record_ind = 'D'     ) ;

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_item_sts_ld           SELECT
instance_id,
 item_instance_status_id,
 active_end_date_time,
 active_start_date_time,
 as_of_date_time,
 batch_nbr,
 as_of_date_time,
 batch_nbr,
 created_by_name,
 creation_date_time,
 incident_allowed_flag,
 item_instance_status_desc,
 item_instance_status_name,
 last_update_date_time,
 last_update_login_name,
 last_updated_by_name,
 migrated_flag,
 seed_status_update_flag,
 seeded_flag,
 service_order_allowed_flag,
 status_change_allowed_flag,
 terminated_flag,
 tran_code,
 update_flag
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t1 
        LEFT OUTER JOIN
            (
                SELECT
                    DISTINCT instance_id AS auto_c00,
                    item_instance_status_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2     
                WHERE
                    upper(trim(current_record_ind)) = upper(trim('D')) 
            ) AS autoAlias_13 
                ON (
                    upper(trim(instance_id)) = upper(trim(autoAlias_13.AUTO_C00)) 
                    AND item_instance_status_id = autoAlias_13.AUTO_C01 
                ) 
        WHERE
            autoAlias_13.AUTO_C00 IS  NULL  
            AND autoAlias_13.AUTO_C01 IS  NULL;


----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/31 11:36:31 
--Script Name: d8_install_base_item_sts_06.ins.sql
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
  --DELETE FROM AAPPLLIIDD1EENNVV_install_base_item_sts_t1 ALL; 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t1;

--Original Query:
  -- INSERT INTO AAPPLLIIDD1EENNVV_install_base_item_sts_t1 ( instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, as_of_date_time, batch_nbr, created_by_name, creation_date_time, incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag ) SELECT instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, as_of_date_time, batch_nbr, created_by_name, creation_date_time, incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag FROM AAPPLLIIDD1EENNVV_install_base_item_sts_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr, 	instance_id, 	item_instance_status_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr), 	instance_id, 	item_instance_status_id     FROM AAPPLLIIDD1EENNVV_install_base_item_sts_ml     WHERE tran_code = 'D'     GROUP BY 2,3 ) AND tran_code = 'D' ;

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t1           
SELECT 
instance_id,
item_instance_status_id,
active_end_date_time,
active_start_date_time,
as_of_date_time,
batch_nbr,
created_by_name,
creation_date_time,
incident_allowed_flag,
item_instance_status_desc,
item_instance_status_name,
last_update_date_time,
last_update_login_name,
last_updated_by_name,
migrated_flag,
seed_status_update_flag,
seeded_flag,
service_order_allowed_flag,
status_change_allowed_flag,
terminated_flag,
tran_code,
update_flag
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_ml
        INNER JOIN
            (
                SELECT
                    MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    batch_nbr) ) AS auto_c00,
                    instance_id AS auto_c01,
                    item_instance_status_id AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_ml     
                WHERE
                    upper(trim(tran_code)) = upper(trim('D'))  
                GROUP BY
                    instance_id ,
                    item_instance_status_id 
            ) AS autoAlias_15 
                ON (
                    CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                batch_nbr) = autoAlias_15.AUTO_C00 
                AND upper(trim(instance_id)) = upper(trim(autoAlias_15.AUTO_C01)) 
                AND item_instance_status_id = autoAlias_15.AUTO_C02 ) 
            WHERE
                upper(trim(tran_code)) = upper(trim('D'));

--Original Query:
  --DELETE FROM AAPPLLIIDD1EENNVV_install_base_item_sts_t2;

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2;

--Original Query:
  --INSERT INTO AAPPLLIIDD1EENNVV_install_base_item_sts_t2 ( instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag ) SELECT l.instance_id, l.item_instance_status_id, l.active_end_date_time, l.active_start_date_time, t.as_of_date_time, t.batch_nbr, l.as_of_date_time, l.batch_nbr, l.created_by_name, l.creation_date_time, l.incident_allowed_flag, l.item_instance_status_desc, l.item_instance_status_name, l.last_update_date_time, l.last_update_login_name, l.last_updated_by_name, l.migrated_flag, l.seed_status_update_flag, l.seeded_flag, l.service_order_allowed_flag, l.status_change_allowed_flag, l.terminated_flag, t.tran_code, l.update_flag FROM EEDDWWDDBB1.install_base_item_sts_ld l,      AAPPLLIIDD1EENNVV_install_base_item_sts_t1 t WHERE  l.instance_id = t.instance_id AND    l.item_instance_status_id = t.item_instance_status_id AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr < CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr ;

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2           SELECT
l.instance_id                   
,l.item_instance_status_id       
,l.active_end_date_time          
,l.active_start_date_time        
,l.alt_as_of_date_time           
,l.alt_batch_nbr                 
,l.as_of_date_time               
,l.batch_nbr                     
,l.created_by_name               
,l.creation_date_time            
,null            
,l.incident_allowed_flag         
,l.item_instance_status_desc     
,l.item_instance_status_name     
,l.last_update_date_time         
,l.last_update_login_name        
,l.last_updated_by_name          
,l.migrated_flag                 
,l.seed_status_update_flag       
,l.seeded_flag                   
,l.service_order_allowed_flag    
,l.status_change_allowed_flag    
,l.terminated_flag               
,l.tran_code                     
,l.update_flag                   

        FROM
            ${EEDDWWDDBB1}.install_base_item_sts_ld    AS l   ,
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t1    AS t   
        WHERE
            l.instance_id = t.instance_id  
            AND l.item_instance_status_id = t.item_instance_status_id   
            AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);


----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/31 11:36:31 
--Script Name: d8_install_base_item_sts_07.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DATABASE ${EEDDWWDDBB1}  

--Translated Query: STATUS SUCCESS

    use ${EEDDWWDDBB1} ;

--Original Query:
  --DELETE FROM install_base_item_sts_ld WHERE (instance_id, item_instance_status_id ) IN (SELECT instance_id, item_instance_status_id     FROM TTMMPPDDBB1.AAPPLLIIDD1EENNVV_install_base_item_sts_t2     ) ;

--Translated Query: STATUS SUCCESS

    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.install_base_item_sts_ld SELECT
            A.* 
        FROM install_base_item_sts_ld A Left outer join
    ( select distinct instance_id, item_instance_status_id from ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2) B
on A.instance_id = B.instance_id and
A.item_instance_status_id = B.item_instance_status_id
where B.instance_id is null and B.item_instance_status_id is null;


--Original Query:
  -- INSERT INTO install_base_item_sts_ld ( instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag ) SELECT instance_id, item_instance_status_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, incident_allowed_flag, item_instance_status_desc, item_instance_status_name, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, seed_status_update_flag, seeded_flag, service_order_allowed_flag, status_change_allowed_flag, terminated_flag, tran_code, update_flag FROM TTMMPPDDBB1.AAPPLLIIDD1EENNVV_install_base_item_sts_t2 ;  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_item_sts_ld           
SELECT 
instance_id,
item_instance_status_id,
active_end_date_time,
active_start_date_time,
alt_as_of_date_time,
alt_batch_nbr,
as_of_date_time,
batch_nbr,
created_by_name,
creation_date_time,
incident_allowed_flag,
item_instance_status_desc,
item_instance_status_name,
last_update_date_time,
last_update_login_name,
last_updated_by_name,
migrated_flag,
seed_status_update_flag,
seeded_flag,
service_order_allowed_flag,
status_change_allowed_flag,
terminated_flag,
tran_code,
update_flag  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_sts_t2;


