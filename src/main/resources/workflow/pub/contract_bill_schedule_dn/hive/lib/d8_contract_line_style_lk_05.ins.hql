----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:17 
--Script Name: d8_contract_line_style_lk_05.ins.sql
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

--(Nikhil: Corrected the translated query,The table used in join was skipped, added that and also kept only limited columns in corrected query )
--Original Query:
  --DELETE FROM contract_line_style_lk_ld  WHERE (contract_line_style_id,instance_id,language_code) IN (SELECT contract_line_style_id,instance_id,language_code     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t1     WHERE (contract_line_style_id,instance_id,language_code) NOT IN  (SELECT contract_line_style_id,instance_id,language_code    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2  WHERE current_record_ind = 'D'  )     )  

--Translated Query: STATUS SUCCESS
-- /*
    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_line_style_lk_ld SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.contract_line_style_lk_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${EEDDWWDDBB1}.contract_line_style_lk_ld      
            -- ) AS Q2 
                -- ON COALESCE( Q1.contract_line_style_id ,
            -- 1) = COALESCE( Q2.contract_line_style_id ,
            -- 1) 
            -- AND COALESCE( Q1.instance_id ,
            -- '1' ) = COALESCE( Q2.instance_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.language_code ,
            -- '1' ) = COALESCE( Q2.language_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.alt_batch_nbr ,
            -- 1) = COALESCE( Q2.alt_batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.batch_nbr ,
            -- 1) = COALESCE( Q2.batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.created_by_name ,
            -- '1' ) = COALESCE( Q2.created_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_update_login_name ,
            -- '1' ) = COALESCE( Q2.last_update_login_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_updated_by_name ,
            -- '1' ) = COALESCE( Q2.last_updated_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.line_style_desc ,
            -- '1' ) = COALESCE( Q2.line_style_desc ,
            -- '1' ) 
            -- AND COALESCE( Q1.line_style_name ,
            -- '1' ) = COALESCE( Q2.line_style_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.source_language_code ,
            -- '1' ) = COALESCE( Q2.source_language_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.tran_code ,
            -- '1' ) = COALESCE( Q2.tran_code ,
            -- '1' ) 
        -- WHERE
            -- Q2.contract_line_style_id IS NULL 
            -- AND Q2.instance_id IS NULL 
            -- AND Q2.language_code IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_update_login_name IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.line_style_desc IS NULL 
            -- AND Q2.line_style_name IS NULL 
            -- AND Q2.source_language_code IS NULL 
            -- AND Q2.tran_code IS NULL;*/
--Corrected Query:
INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.contract_line_style_lk_ld 
select
   Q1.* 
From
   ${EEDDWWDDBB1}.contract_line_style_lk_ld Q1 
   Left Join
      (
         select
            A.contract_line_style_id,
            A.instance_id,
            A.language_code 
         From
           ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t1 As A 
            Left Join
               (
                  SELECT
                     contract_line_style_id,
                     instance_id,
                     language_code 
                  FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2
                  WHERE
                     current_record_ind = 'D' 
               )
               As B 
               On A.contract_line_style_id = B.contract_line_style_id 
               And trim(upper(A.instance_id)) =  trim(upper(B.instance_id)) 
               And  trim(upper(A.language_code)) = trim(upper(B.language_code)) 
         Where
            B.contract_line_style_id Is NULL 
            And B.instance_id Is Null 
            And B.language_code Is Null 
      )
      As Q2 
      On Q1.contract_line_style_id = Q2.contract_line_style_id 
      And trim(upper(Q1.instance_id)) = trim(upper(Q2.instance_id)) 
      And trim(upper(Q1.language_code)) = trim(upper(Q2.language_code)) 
Where
   Q2.contract_line_style_id Is NULL 
   And Q2.instance_id Is Null 
   And Q2.language_code Is Null;            

--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t1 FOR ACCESS  INSERT INTO contract_line_style_lk_ld (   contract_line_style_id,     instance_id,    language_code,  alt_as_of_date_time,    alt_batch_nbr,  as_of_date_time,    batch_nbr,  created_by_name,    creation_date_time,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   line_style_desc,    line_style_name,    source_language_code,   tran_code ) SELECT      contract_line_style_id,     instance_id,    language_code,  as_of_date_time,    batch_nbr,  as_of_date_time,    batch_nbr,  created_by_name,    creation_date_time,     last_update_date_time,  last_update_login_name,     last_updated_by_name,   line_style_desc,    line_style_name,    source_language_code,   tran_code  FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t1 WHERE ( contract_line_style_id,instance_id,language_code) NOT IN (SELECT  contract_line_style_id,instance_id,language_code     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2     WHERE current_record_ind = 'D'      )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_line_style_lk_ld           SELECT
            contract_line_style_id,
            instance_id,
            language_code,
            as_of_date_time,
            batch_nbr,
            as_of_date_time,
            batch_nbr,
            created_by_name,
            creation_date_time,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            line_style_desc,
            line_style_name,
            source_language_code,
            tran_code  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t1 
        LEFT OUTER JOIN
            (
                SELECT
                    DISTINCT contract_line_style_id AS auto_c00,
                    instance_id AS auto_c01,
                    language_code AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_style_lk_t2     
                WHERE
                    upper(trim(current_record_ind)) = upper(trim('D')) 
            ) AS autoAlias_157 
                ON (
                    contract_line_style_id = autoAlias_157.AUTO_C00 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_157.AUTO_C01)) 
                    AND upper(trim(language_code)) = upper(trim(autoAlias_157.AUTO_C02)) 
                ) 
        WHERE
            autoAlias_157.AUTO_C00 IS  NULL  
            AND autoAlias_157.AUTO_C01 IS  NULL  
            AND autoAlias_157.AUTO_C02 IS  NULL;
