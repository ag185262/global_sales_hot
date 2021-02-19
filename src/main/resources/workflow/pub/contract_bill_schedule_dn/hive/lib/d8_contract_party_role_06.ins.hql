----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:31 
--Script Name: d8_contract_party_role_06.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_party_role_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t1;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_party_role_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_party_role_t1 (   instance_id,    party_role_id,  alt_contract_header_id,     as_of_date_time,    batch_nbr,  created_by_name,    contract_line_id,   contract_header_id,     creation_date_time,     facility_code,  government_code,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   minority_group_code,    object1_code,   object1_id1,    object1_id2,    role_code,  parent_party_role_id,   primary_party_flag,     tran_code,  small_business_flag,    women_owned_flag ) SELECT   instance_id,    party_role_id,  alt_contract_header_id,     as_of_date_time,    batch_nbr,  created_by_name,    contract_line_id,   contract_header_id,     creation_date_time,     facility_code,  government_code,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   minority_group_code,    object1_code,   object1_id1,    object1_id2,    role_code,  parent_party_role_id,   primary_party_flag,     tran_code,  small_business_flag,    women_owned_flag FROM ${AAPPLLIIDD1EENNVV}_contract_party_role_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,      party_role_id,      instance_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),    party_role_id,      instance_id     FROM ${AAPPLLIIDD1EENNVV}_contract_party_role_ml     WHERE tran_code = 'D'     GROUP BY 2,3 ) AND tran_code = 'D'  

--Translated Query: STATUS MANUAL

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t1           SELECT
            -- instance_id,
            -- party_role_id,
            -- alt_contract_header_id,
            -- as_of_date_time,
            -- batch_nbr,
            -- created_by_name,
            -- contract_line_id,
            -- contract_header_id,
            -- creation_date_time,
            -- facility_code,
            -- government_code,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- minority_group_code,
            -- object1_code,
            -- object1_id1,
            -- object1_id2,
            -- role_code,
            -- parent_party_role_id,
            -- primary_party_flag,
            -- tran_code,
            -- small_business_flag,
            -- women_owned_flag  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- party_role_id AS auto_c01,
                    -- instance_id AS auto_c02  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_ml     
                -- WHERE
                    -- upper(trim(tran_code)) = upper(trim('D'))  
                -- GROUP BY
                    -- party_role_id ,
                    -- instance_id 
            -- ) AS autoAlias_229 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_229.AUTO_C00 
                -- AND upper(trim(party_role_id)) = upper(trim(autoAlias_229.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_229.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) = upper(trim('D'));

--Corrected Query: Transformed Query build with self join for max values.by using rank() function we can achieve the same result 
    with qq1 as (
   SELECT
                    instance_id,
                    party_role_id,
                    alt_contract_header_id,
                    as_of_date_time,
                    batch_nbr,
                    created_by_name,
                    contract_line_id,
                    contract_header_id,
                    creation_date_time,
                    facility_code,
                    government_code,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    minority_group_code,
                    object1_code,
                    object1_id1,
                    object1_id2,
                    role_code,
                    parent_party_role_id,
                    primary_party_flag,
                    tran_code,
                    small_business_flag,
                    women_owned_flag,
                    ROW_NUMBER() over (PARTITION BY party_role_id, instance_id 
                        ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) , batch_nbr) DESC) As RNK 
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_ml 
                WHERE
                    upper(trim(tran_code)) = upper(trim('D'))  ) 
INSERT INTO  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t1 
SELECT
           EDW2.instance_id,
           EDW2.party_role_id,
           EDW2.alt_contract_header_id,
           EDW2.as_of_date_time,
           EDW2.batch_nbr,
           EDW2.created_by_name,
           EDW2.contract_line_id,
           EDW2.contract_header_id,
           EDW2.creation_date_time,
           EDW2.facility_code,
           EDW2.government_code,
           EDW2.last_update_date_time,
           EDW2.last_update_login_name,
           EDW2.last_updated_by_name,
           EDW2.minority_group_code,
           EDW2.object1_code,
           EDW2.object1_id1,
           EDW2.object1_id2,
           EDW2.role_code,
           EDW2.parent_party_role_id,
           EDW2.primary_party_flag,
           EDW2.tran_code,
           EDW2.small_business_flag,
           EDW2.women_owned_flag
FROM      ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_ml  
 EDW2 
  INNER JOIN (
    SELECT 
            instance_id,
            party_role_id,
            alt_contract_header_id,
            as_of_date_time,
            batch_nbr,
            created_by_name,
            contract_line_id,
            contract_header_id,
            creation_date_time,
            facility_code,
            government_code,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            minority_group_code,
            object1_code,
            object1_id1,
            object1_id2,
            role_code,
            parent_party_role_id,
            primary_party_flag,
            tran_code,
            small_business_flag,
            women_owned_flag, 
           RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND upper(trim(EDW2.party_role_id)) =upper(trim(IBVL.party_role_id ))
   AND upper(trim(EDW2.tran_code)) = upper(trim('D'));              

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_party_role_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2;


--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_party_role_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_party_role_t2 (   instance_id,    party_role_id,  alt_as_of_date_time,    alt_batch_nbr,  alt_contract_header_id,     as_of_date_time,    batch_nbr,  created_by_name,    contract_line_id,   contract_header_id,     creation_date_time,     facility_code,  government_code,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   minority_group_code,    object1_code,   object1_id1,    object1_id2,    role_code,  parent_party_role_id,   primary_party_flag,     tran_code,  small_business_flag,    women_owned_flag ) SELECT   l.instance_id,  l.party_role_id,    t.as_of_date_time,  t.batch_nbr,    l.alt_contract_header_id,   l.as_of_date_time,  l.batch_nbr,    l.created_by_name,  l.contract_line_id,     l.contract_header_id,   l.creation_date_time,   l.facility_code,    l.government_code,  l.last_update_date_time,    l.last_update_login_name,   l.last_updated_by_name,     l.minority_group_code,  l.object1_code,     l.object1_id1,  l.object1_id2,  l.role_code,    l.parent_party_role_id,     l.primary_party_flag,   t.tran_code,    l.small_business_flag,  l.women_owned_flag FROM ${EEDDWWDDBB1}.contract_party_role_ld l,       ${AAPPLLIIDD1EENNVV}_contract_party_role_t1 t WHERE  l.party_role_id = t.party_role_id AND    l.instance_id = t.instance_id  AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <      CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS MANUAL
--(Nikhil: Replaced the Inequality condition with CTE block)
    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2           SELECT
            -- l.instance_id,
            -- l.party_role_id,
            -- t.as_of_date_time,
            -- t.batch_nbr,
            -- l.alt_contract_header_id,
            -- l.as_of_date_time,
            -- l.batch_nbr,
            -- l.created_by_name,
            -- l.contract_line_id,
            -- l.contract_header_id,
            -- l.creation_date_time,
            -- null,
            -- l.facility_code,
            -- l.government_code,
            -- l.last_update_date_time,
            -- l.last_update_login_name,
            -- l.last_updated_by_name,
            -- l.minority_group_code,
            -- l.object1_code,
            -- l.object1_id1,
            -- l.object1_id2,
            -- l.role_code,
            -- l.parent_party_role_id,
            -- l.primary_party_flag,
            -- t.tran_code,
            -- l.small_business_flag,
            -- l.women_owned_flag  
        -- FROM
            -- ${EEDDWWDDBB1}.contract_party_role_ld    AS l   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t1    AS t   
        -- WHERE
            -- l.party_role_id = t.party_role_id  
            -- AND l.instance_id = t.instance_id   
            -- AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);

--Translated Query:(Corrected Manually)
WITH CTE AS 
(
SELECT t.party_role_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
 CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat
 
from  ${EEDDWWDDBB1}.contract_party_role_ld    AS l
INNER JOIN 
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t1    AS t   
ON        upper(trim(l.party_role_id)) = upper(trim(t.party_role_id )) 
            AND upper(trim(l.instance_id)) = upper(trim(t.instance_id))
            WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
)
INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_party_role_t2           SELECT
            l.instance_id,
            l.party_role_id,
            t.as_of_date_time,
            t.batch_nbr,
            l.alt_contract_header_id,
            l.as_of_date_time,
            l.batch_nbr,
            l.created_by_name,
            l.contract_line_id,
            l.contract_header_id,
            l.creation_date_time,
            null,
            l.facility_code,
            l.government_code,
            l.last_update_date_time,
            l.last_update_login_name,
            l.last_updated_by_name,
            l.minority_group_code,
            l.object1_code,
            l.object1_id1,
            l.object1_id2,
            l.role_code,
            l.parent_party_role_id,
            l.primary_party_flag,
            t.tran_code,
            l.small_business_flag,
            l.women_owned_flag  
        FROM
            ${EEDDWWDDBB1}.contract_party_role_ld    AS l   
            INNER JOIN CTE as t
            ON
            upper(trim(l.party_role_id)) = upper(trim(t.party_role_id))
            AND upper(trim(l.instance_id)) = upper(trim(t.instance_id ))  
            WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) =t.iCat;