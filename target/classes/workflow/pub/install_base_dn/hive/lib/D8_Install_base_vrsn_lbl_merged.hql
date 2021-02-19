----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/31 11:36:31 
--Script Name: d8_install_base_vrsn_lbl_ld.ins.sql
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
  --DELETE FROM install_base_vrsn_lbl_ld 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${EEDDWWDDBB1}.install_base_vrsn_lbl_ld;

--Original Query:
  --INSERT INTO install_base_vrsn_lbl_ld SELECT * FROM install_base_vrsn_lbl  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${EEDDWWDDBB1}.install_base_vrsn_lbl_ld           SELECT
            -- install_base_vrsn_lbl.*  
        -- FROM
            -- ${EEDDWWDDBB1}.install_base_vrsn_lbl;
INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_vrsn_lbl_ld          
        SELECT
            a.*  
        FROM
            ${EEDDWWDDBB1}.install_base_vrsn_lbl a;
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

    use ${TTMMPPDDBB1};

--Original Query:
  ----DELETE FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t1 ALL;
--Translated Query: STATUS SUCCESS
Truncate Table ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1;
	
	
--Original Query:
  ----INSERT INTO AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t1 ( instance_id, version_label_id, active_end_date_time, active_start_date_time, as_of_date_time, batch_nbr, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name ) SELECT instance_id, version_label_id, active_end_date_time, active_start_date_time, as_of_date_time, batch_nbr, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr, 	instance_id, version_label_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr), instance_id, version_label_id     FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_ml     WHERE tran_code <> 'D'     GROUP BY 2,3 ) AND tran_code <> 'D' ;

--Translated Query: SUCCESS


INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1
SELECT 
instance_id,
version_label_id,
active_end_date_time,
active_start_date_time,
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
version_label_date_time,
version_label_desc,
version_label_name
FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_ml
INNER JOIN
            (
                SELECT
                    MAX( CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                    batch_nbr) ) AS auto_c00,
                    instance_id AS auto_c01,
                    version_label_id AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_ml     
                WHERE
                    upper(trim(tran_code)) <> upper(trim('D'))  
                GROUP BY
                    instance_id ,
                    version_label_id 
            ) AS autoAlias_52 
on 
 CONCAT(CAST (as_of_date_time AS CHAR(26)) ,batch_nbr) = autoAlias_52.AUTO_C00 
                AND upper(trim(instance_id)) = upper(trim(autoAlias_52.AUTO_C01)) 
                AND version_label_id = autoAlias_52.AUTO_C02 
            WHERE
                upper(trim(tran_code)) <> upper(trim('D'));

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

    use ${TTMMPPDDBB1};

--Original Query:
  ----DELETE FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t2 ALL;
--Translated Query: STATUS SUCCESS
Truncate Table ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2;
	
	
--Original Query:
  ----INSERT INTO AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t2 ( instance_id, version_label_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, current_record_ind, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name ) SELECT instance_id, version_label_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, 'Y', item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name FROM EEDDWWDDBB1.install_base_vrsn_lbl_ld WHERE (instance_id, version_label_id) IN (SELECT     instance_id,     version_label_id     FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t1     ) ;

--Translated Query: SUCCESS

INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2
SELECT 
instance_id,
version_label_id,
active_end_date_time,
active_start_date_time,
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
version_label_date_time,
version_label_desc,
version_label_name
FROM ${EEDDWWDDBB1}.install_base_vrsn_lbl_ld B
INNER JOIN
 (SELECT DISTINCT
    instance_id as AUTO_C0,
    version_label_id as AUTO_C1
    FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1
    ) X
on X.AUTO_C0 = B.instance_id AND
X.AUTO_C1 = B.version_label_id;


---DELETE FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t3 ALL;
--Translated Query: STATUS SUCCESS
TRUNCATE TABLE ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t3;


---INSERT INTO AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t3 ( instance_id, version_label_id, active_end_date_time, active_start_date_time, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name ) SELECT instance_id, version_label_id, active_end_date_time, active_start_date_time, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t2 WHERE (instance_id, version_label_id) IN (SELECT     instance_id,     version_label_id     FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t1     ) ;


--Translated Query: STATUS SUCCESS
INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t3
SELECT
instance_id,
version_label_id,
active_end_date_time,
active_start_date_time,
created_by_name,
creation_date_time,
item_instance_id,
last_update_date_time,
last_update_login_name,
last_updated_by_name,
migrated_flag,
tran_code,
version_label_date_time,
version_label_desc,
version_label_name
FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_vrsn_lbl_03.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------

--Translated Query: SUCCESS
use ${TTMMPPDDBB1};

--Original Query:
  ----INSERT INTO AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t3 ( instance_id, version_label_id, active_end_date_time, active_start_date_time, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name ) SELECT instance_id, version_label_id, active_end_date_time, active_start_date_time, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t1 WHERE (instance_id, version_label_id) IN (SELECT     instance_id,     version_label_id     FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t2     ) ;

--Translated Query: SUCCESS


INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t3
SELECT
instance_id,
version_label_id,
active_end_date_time,
active_start_date_time,
created_by_name,
creation_date_time,
item_instance_id,
last_update_date_time,
last_update_login_name,
last_updated_by_name,
migrated_flag,
tran_code,
version_label_date_time,
version_label_desc,
version_label_name
FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1 A
INNER JOIN (SELECT Distinct
    instance_id as autoc0,
    version_label_id as autoc1
    FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2
    ) X
on
A.instance_id = X.autoc0 and
A.version_label_id = X.autoc1
;

--Translated Query: SUCCESS
INSERT OVERWRITE TABLE ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t3 SELECT DISTINCT * FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t3;----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_vrsn_lbl_04.upd.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------



--Original Query:
  ----UPDATE AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t2 SET current_record_ind = 'D' WHERE (instance_id, version_label_id)  IN     (SELECT instance_id, version_label_id     FROM AAPPLLIIDD1EENNVV_install_base_vrsn_lbl_t3     GROUP BY instance_id, version_label_id     HAVING COUNT(*) = 1     ) ;

--Translated Query: SUCCESS
use ${TTMMPPDDBB1};
--Translated Query: STATUS SUCCESS
INSERT OVERWRITE TABLE ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2
SELECT
       A.instance_id AS instance_id ,
            A.version_label_id AS version_label_id ,
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
            A.item_instance_id AS item_instance_id ,
            A.last_update_date_time AS last_update_date_time ,
            A.last_update_login_name AS last_update_login_name ,
            A.last_updated_by_name AS last_updated_by_name ,
            A.migrated_flag AS migrated_flag ,
            A.tran_code AS tran_code ,
            A.version_label_date_time AS version_label_date_time ,
            A.version_label_desc AS version_label_desc ,
            A.version_label_name AS version_label_name 
FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2 A
LEFT OUTER JOIN (SELECT instance_id as autoc0, version_label_id as autoc1
    FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t3
    GROUP BY instance_id, version_label_id
    HAVING COUNT(*) = 1
    ) X
on
A.instance_id = X.autoc0 and
A.version_label_id = X.autoc1
;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/31 11:36:31 
--Script Name: d8_install_base_vrsn_lbl_05.ins.sql
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
  --DELETE FROM install_base_vrsn_lbl_ld  WHERE (instance_id, version_label_id ) IN (SELECT instance_id, version_label_id      FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1     WHERE (instance_id, version_label_id) NOT IN 	(SELECT instance_id, version_label_id 	FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2 	WHERE current_record_ind = 'D' 	)     )  

--Translated Query: STATUS SUCCESS
with qq1 as (SELECT distinct instance_id, version_label_id FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2  WHERE upper(trim(current_record_ind)) = 'D' ),
qq2 as (SELECT DISTINCT A.instance_id,A.version_label_id from ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1 A left outer join qq1 B on   A.instance_id = B.instance_id and A.version_label_id = B.version_label_id where B.instance_id is null and B.version_label_id is null)
    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.install_base_vrsn_lbl_ld SELECT
            X.* 
        FROM install_base_vrsn_lbl_ld X
            left outer join 
             qq2 Y
        on X.instance_id = Y.instance_id AND
           X.version_label_id = Y.version_label_id
        where Y.instance_id is null and Y.version_label_id is null;


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1 FOR ACCESS  INSERT INTO install_base_vrsn_lbl_ld ( instance_id, version_label_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name ) SELECT  instance_id, version_label_id, active_end_date_time, active_start_date_time, as_of_date_time, batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1 WHERE ( instance_id, version_label_id ) NOT IN (SELECT  instance_id, version_label_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2     WHERE current_record_ind = 'D'      )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_vrsn_lbl_ld           SELECT
            instance_id,
            version_label_id,
            active_end_date_time,
            active_start_date_time,
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
            version_label_date_time,
            version_label_desc,
            version_label_name  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1 
        LEFT OUTER JOIN
            (
                SELECT
                    DISTINCT instance_id AS auto_c00,
                    version_label_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2     
                WHERE
                    upper(trim(current_record_ind)) = upper(trim('D')) 
            ) AS autoAlias_13 
                ON (
                    upper(trim(instance_id)) = upper(trim(autoAlias_13.AUTO_C00)) 
                    AND version_label_id = autoAlias_13.AUTO_C01 
                ) 
        WHERE
            autoAlias_13.AUTO_C00 IS  NULL  
            AND autoAlias_13.AUTO_C01 IS  NULL;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/31 11:36:31 
--Script Name: d8_install_base_vrsn_lbl_06.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1 ( instance_id, version_label_id, active_end_date_time, active_start_date_time, as_of_date_time, batch_nbr, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name ) SELECT  instance_id, version_label_id, active_end_date_time, active_start_date_time, as_of_date_time, batch_nbr, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,  	instance_id, 	version_label_id) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),  	instance_id, 	version_label_id     FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_ml     WHERE tran_code = 'D'     GROUP BY 2,3 ) AND tran_code = 'D'  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1           SELECT
            instance_id,
            version_label_id,
            active_end_date_time,
            active_start_date_time,
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
            version_label_date_time,
            version_label_desc,
            version_label_name  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_ml 
        INNER JOIN
            (
                SELECT
                    MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    batch_nbr) ) AS auto_c00,
                    instance_id AS auto_c01,
                    version_label_id AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_ml     
                WHERE
                    upper(trim(tran_code)) = upper(trim('D'))  
                GROUP BY
                    instance_id ,
                    version_label_id 
            ) AS autoAlias_15 
                ON (
                    CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                batch_nbr) = autoAlias_15.AUTO_C00 
                AND upper(trim(instance_id)) = upper(trim(autoAlias_15.AUTO_C01)) 
                AND version_label_id = autoAlias_15.AUTO_C02 ) 
            WHERE
                upper(trim(tran_code)) = upper(trim('D'));

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2 ( instance_id, version_label_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name ) SELECT l.instance_id, l.version_label_id, l.active_end_date_time, l.active_start_date_time, t.as_of_date_time, t.batch_nbr, l.as_of_date_time, l.batch_nbr, l.created_by_name, l.creation_date_time, l.item_instance_id, l.last_update_date_time, l.last_update_login_name, l.last_updated_by_name, l.migrated_flag, t.tran_code, l.version_label_date_time, l.version_label_desc, l.version_label_name FROM ${EEDDWWDDBB1}.install_base_vrsn_lbl_ld l,       ${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1 t WHERE  l.instance_id = t.instance_id  AND    l.version_label_id = t.version_label_id AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <  	CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2           SELECT
            l.instance_id,
            l.version_label_id,
            l.active_end_date_time,
            l.active_start_date_time,
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
            l.version_label_date_time,
            l.version_label_desc,
            l.version_label_name  
        FROM
            ${EEDDWWDDBB1}.install_base_vrsn_lbl_ld    AS l   ,
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t1    AS t   
        WHERE
            l.instance_id = t.instance_id  
            AND l.version_label_id = t.version_label_id   
            AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/31 11:36:31 
--Script Name: d8_install_base_vrsn_lbl_07.ins.sql
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
  --DELETE FROM install_base_vrsn_lbl_ld  WHERE (instance_id, version_label_id) IN (SELECT instance_id, version_label_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2     )  

--Translated Query: STATUS SUCCESS

    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.install_base_vrsn_lbl_ld SELECT
            A.* 
        FROM install_base_vrsn_lbl_ld A Left outer join
    ( select distinct instance_id, version_label_id from ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2) B
on A.instance_id = B.instance_id and
A.version_label_id = B.version_label_id
where B.instance_id is null and B.version_label_id is null;


--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2 FOR ACCESS  INSERT INTO install_base_vrsn_lbl_ld ( instance_id, version_label_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name ) SELECT  instance_id, version_label_id, active_end_date_time, active_start_date_time, alt_as_of_date_time, alt_batch_nbr, as_of_date_time, batch_nbr, created_by_name, creation_date_time, item_instance_id, last_update_date_time, last_update_login_name, last_updated_by_name, migrated_flag, tran_code, version_label_date_time, version_label_desc, version_label_name FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_vrsn_lbl_ld           SELECT
            instance_id,
            version_label_id,
            active_end_date_time,
            active_start_date_time,
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
            version_label_date_time,
            version_label_desc,
            version_label_name  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_vrsn_lbl_t2;

