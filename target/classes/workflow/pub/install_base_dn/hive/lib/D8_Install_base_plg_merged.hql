----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_plg_dn_01.ins.sql
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
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.mtl_item_category_xref FOR ACCESSLOCK TABLE ${EEDDWWDDBB2}.mtl_item_category FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01(      instance_id,      inventory_item_id,      inventory_source_code,      category_id,      category_set_id,      source_country_code,      item_category_code,      item_category_desc,      best_fit_tran_code)SELECT      EDW1.instance_id,      EDW1.inventory_item_id,      EDW1.inventory_source_code,      EDW1.category_id,      EDW1.category_set_id,      EDW1.source_country_code,      EDW2.item_category_code,      EDW2.item_category_desc,     (CASE      WHEN EDW1.tran_code = 'D'      OR   EDW2.tran_code = 'D'      THEN 'D'      ELSE 'U'      END)FROM  ${EEDDWWDDBB2}.mtl_item_category_xref EDW1,      ${EEDDWWDDBB2}.mtl_item_category EDW2WHERE EDW1.instance_id = EDW2.instance_idAND   EDW1.category_id = EDW2.category_idAND   EDW1.category_set_id = 6<WM_COMMENT_START>AND   EDW1.inventory_item_id = EDW2.inventory_item_id <WM_COMMENT_END>

--Translated Query: SUCCESS


     INSERT 
     INTO
         TABLE
         ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01           SELECT
             EDW1.instance_id,
             EDW1.inventory_item_id,
             EDW1.inventory_source_code,
             EDW1.category_id,
             EDW1.category_set_id,
             EDW1.source_country_code,
             EDW2.item_category_code,
             EDW2.item_category_desc,
             CASE 
                 WHEN upper(trim(EDW1.tran_code)) = upper(trim('D'))  
                 OR upper(trim(EDW2.tran_code)) = upper(trim('D'))   THEN 'D'  
                 ELSE 'U'  
             END AS auto_c08  
         FROM
             ${EEDDWWDDBB2}.mtl_item_category_xref    AS EDW1   ,
             ${EEDDWWDDBB2}.mtl_item_category    AS EDW2   
         WHERE
             upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))  
             AND EDW1.category_id = EDW2.category_id   
             AND EDW1.category_set_id = 6;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_dn_t05 ALL

--Translated Query: SUCCESS

    ----TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_dn_t05;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_plg_dn_02.ins.sql
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
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02;

--Original Query:
  ----COLLECT STATS ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01 COLUMN item_category_code

--Translated Query: SUCCESS
--DA:query from d8_install_base_plg_dn_01.ins and d8_install_base_plg_dn_02.ins merged .Hence not needed
--    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01 COMPUTE STATISTICS  FOR COLUMNS item_category_code;

--Original Query:
  ----LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01 FOR ACCESSLOCK TABLE ${EEDDWWDDBB2}.fml_product_service_xref FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02(      instance_id,      inventory_item_id,      inventory_source_code,      category_id,      category_set_id,      source_country_code,      item_category_code,      item_category_desc,      product_line_nbr,      corp_product_type_code,      best_fit_tran_code)SELECT      EDW1.instance_id,      EDW1.inventory_item_id,      EDW1.inventory_source_code,      EDW1.category_id,      EDW1.category_set_id,      EDW1.source_country_code,      EDW1.item_category_code,      EDW1.item_category_desc,      COALESCE(EDW2.product_line_nbr,0),      COALESCE(EDW2.corp_product_type_code,' '),      EDW1.best_fit_tran_codeFROM  ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01 EDW1LEFT OUTER JOIN      ${EEDDWWDDBB2}.fml_product_service_xref EDW2ON EDW1.item_category_code = EDW2.corp_fml_prod_service_code

--DA:query from d8_install_base_plg_dn_01.ins and d8_install_base_plg_dn_02.ins merged.Hence commented
--    INSERT 
--    INTO
--        TABLE
--        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02           SELECT
--            EDW1.instance_id,
--            EDW1.inventory_item_id,
--            EDW1.inventory_source_code,
--            EDW1.category_id,
--            EDW1.category_set_id,
--            EDW1.source_country_code,
--            EDW1.item_category_code,
--            EDW1.item_category_desc,
--            COALESCE( EDW2.product_line_nbr ,
--            0 ) AS auto_c08,
--            COALESCE( EDW2.corp_product_type_code ,
--            ' ' ) AS auto_c09,
--            EDW1.best_fit_tran_code  
--        FROM
--            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01    AS EDW1   
--        LEFT OUTER  JOIN
--            ${EEDDWWDDBB2}.fml_product_service_xref    AS EDW2    
--                ON EDW1.item_category_code = EDW2.corp_fml_prod_service_code;

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02           SELECT
            EDW1.instance_id,
            EDW1.inventory_item_id,
            EDW1.inventory_source_code,
            EDW1.category_id,
            EDW1.category_set_id,
            EDW1.source_country_code,
            EDW1.item_category_code,
            EDW1.item_category_desc,
            COALESCE( EDW2.product_line_nbr ,
            0 ) AS auto_c08,
            COALESCE( EDW2.corp_product_type_code ,
            ' ' ) AS auto_c09,
            EDW1.best_fit_tran_code  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.fml_product_service_xref    AS EDW2    
                ON EDW1.item_category_code = EDW2.corp_fml_prod_service_code;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_plg_dn_03.ins.sql
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
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t03 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t03;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.corporate_product_line FOR ACCESS LOCK TABLE ${EEDDWWDDBB2}.product_group FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t03(      product_line_nbr,      product_group_nbr,      prod_line_cat_code,      product_line_name,      product_group_name)SELECT      EDW1.product_line_nbr,      EDW1.product_group_nbr,      EDW1.prod_line_cat_code,      EDW1.product_line_name,      EDW2.product_group_nameFROM  ${EEDDWWDDBB2}.corporate_product_line EDW1,      ${EEDDWWDDBB2}.product_group EDW2WHERE EDW1.product_group_nbr = EDW2.product_group_nbr

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t03           SELECT
            EDW1.product_line_nbr,
            EDW1.product_group_nbr,
            EDW1.prod_line_cat_code,
            EDW1.product_line_name,
            EDW2.product_group_name  
        FROM
            ${EEDDWWDDBB2}.corporate_product_line    AS EDW1   ,
            ${EEDDWWDDBB2}.product_group    AS EDW2   
        WHERE
            EDW1.product_group_nbr = EDW2.product_group_nbr;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_plg_dn_04.ins.sql
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
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04;

--Original Query:
  ----LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02 FOR ACCESSLOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t03 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04(      instance_id,      inventory_item_id,      inventory_source_code,      category_id,      category_set_id,      source_country_code,      item_category_code,      item_category_desc,      product_line_nbr,      corp_product_type_code,      product_group_nbr,      prod_line_cat_code,      product_line_name,      product_group_name,      best_fit_tran_code)SELECT      EDW1.instance_id,      EDW1.inventory_item_id,      EDW1.inventory_source_code,      EDW1.category_id,      EDW1.category_set_id,      EDW1.source_country_code,      EDW1.item_category_code,      EDW1.item_category_desc,      EDW1.product_line_nbr,      EDW1.corp_product_type_code,      EDW2.product_group_nbr,      EDW2.prod_line_cat_code,      EDW2.product_line_name,      EDW2.product_group_name,      EDW1.best_fit_tran_codeFROM  ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02 EDW1,      ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t03 EDW2WHERE EDW1.product_line_nbr = EDW2.product_line_nbr

--Translated Query: SUCCESS
--DA:query from d8_install_base_plg_dn_04.ins and d8_install_base_plg_dn_05.ins merged. HEnce commented
    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04           SELECT
            EDW1.instance_id,
            EDW1.inventory_item_id,
            EDW1.inventory_source_code,
            EDW1.category_id,
            EDW1.category_set_id,
            EDW1.source_country_code,
            EDW1.item_category_code,
            EDW1.item_category_desc,
            EDW1.product_line_nbr,
            EDW1.corp_product_type_code,
            EDW2.product_group_nbr,
            EDW2.prod_line_cat_code,
            EDW2.product_line_name,
            EDW2.product_group_name,
            EDW1.best_fit_tran_code  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02    AS EDW1   ,
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t03    AS EDW2   
        WHERE
            EDW1.product_line_nbr = EDW2.product_line_nbr;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_plg_dn_05.ins.sql
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
  ----COLLECT STATISTICS ON ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04 COLUMN corp_product_type_code


--DA:query from d8_install_base_plg_dn_04.ins and d8_install_base_plg_dn_05.ins merged. hence commented
--    ANALYZE TABLE ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04 COMPUTE STATISTICS  FOR COLUMNS corp_product_type_code;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05;

--Original Query:
  ----LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04 FOR ACCESSLOCK TABLE ${EEDDWWDDBB2}.product_type FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05(      instance_id,      inventory_item_id,      inventory_source_code,      category_id,      category_set_id,      source_country_code,      item_category_code,      item_category_desc,      product_line_nbr,      corp_product_type_code,      product_type_name,      product_group_nbr,      prod_line_cat_code,      product_line_name,      product_group_name,      best_fit_tran_code)SELECT      EDW1.instance_id,      EDW1.inventory_item_id,      EDW1.inventory_source_code,      EDW1.category_id,      EDW1.category_set_id,      EDW1.source_country_code,      EDW1.item_category_code,      EDW1.item_category_desc,      EDW1.product_line_nbr,      EDW1.corp_product_type_code,      COALESCE(EDW2.product_type_name,' '),      EDW1.product_group_nbr,      EDW1.prod_line_cat_code,      EDW1.product_line_name,      EDW1.product_group_name,      EDW1.best_fit_tran_codeFROM  ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04 EDW1LEFT OUTER JOIN      ${EEDDWWDDBB2}.product_type EDW2ON    EDW1.corp_product_type_code = EDW2.product_type_code

--DA:query from d8_install_base_plg_dn_04.ins and d8_install_base_plg_dn_05.ins merged. hence commented
    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05           SELECT
            EDW1.instance_id,
            EDW1.inventory_item_id,
            EDW1.inventory_source_code,
            EDW1.category_id,
            EDW1.category_set_id,
            EDW1.source_country_code,
            EDW1.item_category_code,
            EDW1.item_category_desc,
            EDW1.product_line_nbr,
            EDW1.corp_product_type_code,
            COALESCE( EDW2.product_type_name ,
            ' ' ) AS auto_c010,
            EDW1.product_group_nbr,
            EDW1.prod_line_cat_code,
            EDW1.product_line_name,
            EDW1.product_group_name,
            EDW1.best_fit_tran_code  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04    AS EDW1   
        LEFT OUTER  JOIN
            ${EEDDWWDDBB2}.product_type    AS EDW2    
                ON EDW1.corp_product_type_code = EDW2.product_type_code;

--DA:query from d8_install_base_plg_dn_04.ins and d8_install_base_plg_dn_05.ins merged. It is a merged query
--Translated Query: Manual
--- INSERT 
--- INTO
---     TABLE
---     ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05           SELECT
---         EDW1.instance_id,
---         EDW1.inventory_item_id,
---         EDW1.inventory_source_code,
---         EDW1.category_id,
---         EDW1.category_set_id,
---         EDW1.source_country_code,
---         EDW1.item_category_code,
---         EDW1.item_category_desc,
---         EDW1.product_line_nbr,
---         EDW1.corp_product_type_code,
---         COALESCE( EDW3.product_type_name ,
---         ' ' ) AS auto_c010,
---         EDW2.product_group_nbr,
---         EDW2.prod_line_cat_code,
---         EDW2.product_line_name,
---         EDW2.product_group_name,
---         EDW1.best_fit_tran_code  
---     FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02    AS EDW1   ,
---         ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t03    AS EDW2   
---     LEFT OUTER  JOIN
---         ${EEDDWWDDBB2}.product_type    AS EDW3    
---             ON EDW1.corp_product_type_code = EDW3.product_type_code
---     WHERE
---            EDW1.product_line_nbr = EDW2.product_line_nbr;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_plg_dn_06.ins.sql
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
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06;

--Original Query:
  ----LOCK TABLE ${EEDDWWDDBB2}.mtl_item_category_xref FOR ACCESSLOCK TABLE ${EEDDWWDDBB2}.mtl_item_category FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06(      instance_id,      inventory_item_id,      inventory_source_code,      category_id,      category_set_id,      source_country_code,      item_category_code,      item_category_desc,      best_fit_tran_code)SELECT      EDW1.instance_id,      EDW1.inventory_item_id,      EDW1.inventory_source_code,      EDW1.category_id,      EDW1.category_set_id,      EDW1.source_country_code,      EDW2.item_category_code,      EDW2.item_category_desc,     (CASE      WHEN EDW1.tran_code = 'D'      OR   EDW2.tran_code = 'D'      THEN 'D'      ELSE 'U'      END)FROM  ${EEDDWWDDBB2}.mtl_item_category_xref EDW1,      ${EEDDWWDDBB2}.mtl_item_category EDW2WHERE EDW1.instance_id = EDW2.instance_idAND   EDW1.category_id = EDW2.category_idAND   EDW1.category_set_id = 7

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06           SELECT
            EDW1.instance_id,
            EDW1.inventory_item_id,
            EDW1.inventory_source_code,
            EDW1.category_id,
            EDW1.category_set_id,
            EDW1.source_country_code,
            EDW2.item_category_code,
            EDW2.item_category_desc,
            CASE 
                WHEN upper(trim(EDW1.tran_code)) = 'D'  
                OR upper(trim(EDW2.tran_code)) = 'D'   THEN 'D'  
                ELSE 'U'  
            END AS auto_c08  
        FROM
            ${EEDDWWDDBB2}.mtl_item_category_xref    AS EDW1   ,
            ${EEDDWWDDBB2}.mtl_item_category    AS EDW2   
        WHERE
            EDW1.instance_id = EDW2.instance_id 
            AND EDW1.category_id = EDW2.category_id   
            AND EDW1.category_set_id = 7;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01 ALL

--Translated Query: SUCCESS

    ---TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t01;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02 ALL

--Translated Query: SUCCESS

    ---TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t02;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t03 ALL

--Translated Query: SUCCESS

   --- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t03;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_plg_dn_07a.ins.sql
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
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t07 ALL

--Translated Query: SUCCESS
---TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t07;
    TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t07_b;
 TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05_b;

 TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06_b;

INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05_b SELECT * FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05;
INSERT INTO ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06_b SELECT * FROM ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05 FOR ACCESS LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t07(      instance_id,      inventory_item_id,      inventory_source_code,      category_id,      category_set_id,      source_country_code,      item_category_code,      item_category_desc,      product_line_nbr,      product_type_code,      product_type_name,      product_group_nbr,      prod_line_cat_code,      product_line_name,      product_group_name,      pl_tran_code,      product_category_code,      product_category_desc,      cg_tran_code)SELECT      EDW1.instance_id,      EDW1.inventory_item_id,      EDW1.inventory_source_code,      EDW1.category_id,      EDW1.category_set_id,      EDW1.source_country_code,      EDW1.item_category_code,      EDW1.item_category_desc,      EDW1.product_line_nbr,      EDW1.corp_product_type_code,      EDW1.product_type_name,      EDW1.product_group_nbr,      EDW1.prod_line_cat_code,      EDW1.product_line_name,      EDW1.product_group_name,      EDW1.best_fit_tran_code,      COALESCE(EDW2.item_category_code,' '),      COALESCE(EDW2.item_category_desc,' '),      COALESCE(EDW2.best_fit_tran_code,' ')FROM  ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05 EDW1LEFT OUTER JOIN      ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06 EDW2ON    EDW1.instance_id = EDW2.instance_idAND   EDW1.inventory_item_id = EDW2.inventory_item_idAND   EDW1.inventory_source_code = EDW2.inventory_source_code

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t07_b           SELECT
            EDW1.instance_id,
            EDW1.inventory_item_id,
            EDW1.inventory_source_code,
            EDW1.category_id,
            EDW1.category_set_id,
            EDW1.source_country_code,
            EDW1.item_category_code,
            EDW1.item_category_desc,
            EDW1.product_line_nbr,
            EDW1.corp_product_type_code,
            EDW1.product_type_name,
            EDW1.product_group_nbr,
            EDW1.prod_line_cat_code,
            EDW1.product_line_name,
            EDW1.product_group_name,
            EDW1.best_fit_tran_code,
            COALESCE( EDW2.item_category_code ,
            ' ' ) AS auto_c016,
            COALESCE( EDW2.item_category_desc ,
            ' ' ) AS auto_c017,
            COALESCE( EDW2.best_fit_tran_code ,
            ' ' ) AS auto_c018  
        FROM
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05_b    AS EDW1   
        LEFT OUTER  JOIN
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06_b    AS EDW2    
                ON EDW1.instance_id = EDW2.instance_id  
                AND EDW1.inventory_item_id = EDW2.inventory_item_id   
                AND EDW1.inventory_source_code = EDW2.inventory_source_code;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_plg_dn_07b.ins.sql
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
  ----LOCK TABLE ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06 FOR ACCESS INSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t07(      instance_id,      inventory_item_id,      inventory_source_code,      category_id,      category_set_id,      source_country_code,      item_category_code,      item_category_desc,      product_line_nbr,      product_type_code,      product_type_name,      product_group_nbr,      prod_line_cat_code,      product_line_name,      product_group_name,      pl_tran_code,      product_category_code,      product_category_desc,      cg_tran_code)SELECT      EDW1.instance_id,      EDW1.inventory_item_id,      EDW1.inventory_source_code,      <WM_COMMENT_START> category_id <WM_COMMENT_END>      0,      <WM_COMMENT_START> category_set_id <WM_COMMENT_END>      0,      <WM_COMMENT_START> source_country_code <WM_COMMENT_END>      ' ',      <WM_COMMENT_START> item_category_code <WM_COMMENT_END>      ' ',      <WM_COMMENT_START> item_category_desc <WM_COMMENT_END>      ' ',      <WM_COMMENT_START> product_line_nbr <WM_COMMENT_END>      0,      <WM_COMMENT_START> corp_product_type_code <WM_COMMENT_END>      ' ',      <WM_COMMENT_START> product_type_name <WM_COMMENT_END>      ' ',      <WM_COMMENT_START> product_group_nbr <WM_COMMENT_END>      0,      <WM_COMMENT_START> prod_line_cat_code <WM_COMMENT_END>      ' ',      <WM_COMMENT_START> product_line_name <WM_COMMENT_END>      ' ',      <WM_COMMENT_START> product_group_name <WM_COMMENT_END>      ' ',      <WM_COMMENT_START> best_fit_tran_code <WM_COMMENT_END>      ' ',      EDW1.item_category_code,      EDW1.item_category_desc,      EDW1.best_fit_tran_codeFROM  ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06 EDW1WHERE (      EDW1.instance_id,      EDW1.inventory_item_id,      EDW1.inventory_source_code) NOT IN (SELECT      instance_id,      inventory_item_id,      inventory_source_codeFROM  ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t07)

--Translated Query: SUCCESS

 INSERT 
        INTO
            TABLE
            ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t07_b   
   SELECT
        EDW1.instance_id,
        EDW1.inventory_item_id,
        EDW1.inventory_source_code,
        0 AS auto_c03,
        0 AS auto_c04,
        ' ' AS auto_c05,
        ' ' AS auto_c06,
        ' ' AS auto_c07,
        0 AS auto_c08,
        ' ' AS auto_c09,
        ' ' AS auto_c010,
        0 AS auto_c011,
        ' ' AS auto_c012,
        ' ' AS auto_c013,
        ' ' AS auto_c014,
        ' ' AS auto_c015,
        EDW1.item_category_code,
        EDW1.item_category_desc,
        EDW1.best_fit_tran_code  
    FROM
        ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06_b    AS EDW1 
    LEFT OUTER JOIN
         ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t07_b  AS autoAlias_88 
            ON ( EDW1.instance_id = autoAlias_88.instance_id 
            AND EDW1.inventory_item_id = autoAlias_88.inventory_item_id 
            AND EDW1.inventory_source_code = autoAlias_88.inventory_source_code ) 
    WHERE
        autoAlias_88.instance_id IS  NULL  
        AND autoAlias_88.inventory_item_id IS  NULL  
        AND autoAlias_88.inventory_source_code IS  NULL    ;
--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04 ALL

--Translated Query: SUCCESS

   --- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t04;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05 ALL

--Translated Query: SUCCESS

   ---- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t05;

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06 ALL

--Translated Query: SUCCESS

   ---- TRUNCATE TABLE   ${TTMMPPDDBB}.${AAPPLLIIDD1EENNVV}_install_base_plg_dn_t06;