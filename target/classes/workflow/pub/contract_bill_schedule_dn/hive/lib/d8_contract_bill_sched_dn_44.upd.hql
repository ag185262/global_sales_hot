----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:13:53 
--Script Name: d8_contract_bill_sched_dn_44.upd.sql
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
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 FOR ACCESS  UPDATE ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 SET level_element_amt_ent =0 WHERE (  bill_on_date ,contract_header_id ,instance_id ,interface_date ,invoice_type_code ,level_sequence_nbr ,period_end_date ,period_start_date ,sequence_nbr ,service_line_id ,service_subline_id )  IN (SELECT  bill_on_date ,contract_header_id ,instance_id ,interface_date ,invoice_type_code ,level_sequence_nbr ,period_end_date ,period_start_date ,sequence_nbr ,service_line_id ,service_subline_id FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 WHERE invoice_type_code = 'INV' GROUP BY 1,2,3,4,5,6,7,8,9,10,11 HAVING COUNT(*)>1  ) AND (  bill_on_date ,contract_header_id ,instance_id ,interface_date ,invoice_type_code ,level_sequence_nbr ,period_end_date ,period_start_date ,sequence_nbr ,service_line_id ,service_subline_id ,level_element_amt_ent ) NOT IN ( SELECT  bill_on_date ,contract_header_id ,instance_id ,interface_date ,invoice_type_code ,level_sequence_nbr ,period_end_date ,period_start_date ,sequence_nbr ,service_line_id ,service_subline_id ,MIN(level_element_amt_ent ) FROM ${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 WHERE invoice_type_code = 'INV' GROUP BY 1,2,3,4,5,6,7,8,9,10,11 HAVING COUNT(*)>1 )  

--Translated Query: STATUS SUCCESS

    -- INSERT OVERWRITE
        -- TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 SELECT
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contract_header_id AS contract_header_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.instance_id AS instance_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_line_id AS service_line_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_subline_id AS service_subline_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sline_line_style_id AS sline_line_style_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sline_covered_lvl_name AS sline_covered_lvl_name ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sequence_nbr AS sequence_nbr ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.billing_stream_end_date AS billing_stream_end_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.billing_stream_start_date AS billing_stream_start_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contr_bill_stream_id AS contr_bill_stream_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_offset_day_nbr AS interface_offset_day_nbr ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_uom_cnt AS period_uom_cnt ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_uom_code AS period_uom_code ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_discount_amt_ent AS sl_discount_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_surcharge_amt_ent AS sl_surcharge_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_adj_prc_amt_ent AS sl_top_lvl_adj_prc_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_oprnd_prc_amt_ent AS sl_top_lvl_oprnd_prc_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_pricing_uom_qty AS sl_top_lvl_pricing_uom_qty ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_unbilled_amt_ent AS sl_unbilled_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.credit_amt_ent AS credit_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_date AS interface_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_sequence_nbr AS level_sequence_nbr ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_end_date AS period_end_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_start_date AS period_start_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_on_date AS bill_on_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_type_code AS invoice_type_code ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_amt_ent AS bill_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.completed_date AS completed_date ,
            -- CASE 
                -- WHEN autoAlias_37.auto_c00 IS not null 
                -- AND autoAlias_37.auto_c00 IS null THEN 0 
                -- ELSE AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_element_amt_ent 
            -- END AS level_element_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_currency_code AS invoice_currency_code ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_date AS invoice_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_nbr AS invoice_nbr 
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 AS AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44 
        -- Left Outer Join
            -- (
                -- SELECT
                    -- bill_on_date AS auto_c00,
                    -- contract_header_id AS auto_c01,
                    -- instance_id AS auto_c02,
                    -- interface_date AS auto_c03,
                    -- invoice_type_code AS auto_c04,
                    -- level_sequence_nbr AS auto_c05,
                    -- period_end_date AS auto_c06,
                    -- period_start_date AS auto_c07,
                    -- sequence_nbr AS auto_c08,
                    -- service_line_id AS auto_c09,
                    -- service_subline_id AS auto_c010,
                    -- MIN( level_element_amt_ent ) AS auto_c011  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44     
                -- WHERE
                    -- UPPER(TRIM(INVOICE_TYPE_CODE)) = UPPER(TRIM('INV'))  
                -- GROUP BY
                    -- bill_on_date ,
                    -- contract_header_id ,
                    -- instance_id ,
                    -- interface_date ,
                    -- invoice_type_code ,
                    -- level_sequence_nbr ,
                    -- period_end_date ,
                    -- period_start_date ,
                    -- sequence_nbr ,
                    -- service_line_id ,
                    -- service_subline_id 
                -- HAVING
                    -- COUNT(*) > 1
            -- )autoAlias_37 
                -- ON (
                    -- autoAlias_37.auto_c00 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_on_date 
                    -- AND autoAlias_37.auto_c01 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contract_header_id 
                    -- AND autoAlias_37.auto_c02 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.instance_id 
                    -- AND autoAlias_37.auto_c03 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_date 
                    -- AND autoAlias_37.auto_c04 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_type_code 
                    -- AND autoAlias_37.auto_c05 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_sequence_nbr 
                    -- AND autoAlias_37.auto_c06 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_end_date 
                    -- AND autoAlias_37.auto_c07 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_start_date 
                    -- AND autoAlias_37.auto_c08 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sequence_nbr 
                    -- AND autoAlias_37.auto_c09 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_line_id 
                    -- AND autoAlias_37.auto_c010 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_subline_id 
                    -- AND autoAlias_37.auto_c011 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_element_amt_ent
                -- );

--Translated Query: (Further modified, Comments: The CASE Statement generated was incorrectly translated, hence included a few more not null checks and a NULL check in CASE statement

 -- INSERT OVERWRITE
        -- TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 
-- SELECT
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contract_header_id AS contract_header_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.instance_id AS instance_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_line_id AS service_line_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_subline_id AS service_subline_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sline_line_style_id AS sline_line_style_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sline_covered_lvl_name AS sline_covered_lvl_name ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sequence_nbr AS sequence_nbr ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.billing_stream_end_date AS billing_stream_end_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.billing_stream_start_date AS billing_stream_start_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contr_bill_stream_id AS contr_bill_stream_id ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_offset_day_nbr AS interface_offset_day_nbr ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_uom_cnt AS period_uom_cnt ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_uom_code AS period_uom_code ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_discount_amt_ent AS sl_discount_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_surcharge_amt_ent AS sl_surcharge_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_adj_prc_amt_ent AS sl_top_lvl_adj_prc_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_oprnd_prc_amt_ent AS sl_top_lvl_oprnd_prc_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_pricing_uom_qty AS sl_top_lvl_pricing_uom_qty ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_unbilled_amt_ent AS sl_unbilled_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.credit_amt_ent AS credit_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_date AS interface_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_sequence_nbr AS level_sequence_nbr ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_end_date AS period_end_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_start_date AS period_start_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_on_date AS bill_on_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_type_code AS invoice_type_code ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_amt_ent AS bill_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.completed_date AS completed_date ,
            -- CASE 
                -- WHEN autoAlias_37.auto_c00 IS not null and autoAlias_37.auto_c01 IS not null and autoAlias_37.auto_c02 IS not null and autoAlias_37.auto_c03 IS not null and autoAlias_37.auto_c04 IS not null and autoAlias_37.auto_c05 IS not null and autoAlias_37.auto_c06 IS not null and autoAlias_37.auto_c07 IS not null and autoAlias_37.auto_c08 IS not null and autoAlias_37.auto_c09 IS not null and autoAlias_37.auto_c010 IS not null and autoAlias_37.auto_c011 is null THEN 0 
                -- ELSE AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_element_amt_ent 
            -- END AS level_element_amt_ent ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_currency_code AS invoice_currency_code ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_date AS invoice_date ,
            -- AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_nbr AS invoice_nbr 
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 AS AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44 
        -- Left Outer Join
            -- (
                -- SELECT
                    -- bill_on_date AS auto_c00,
                    -- contract_header_id AS auto_c01,
                    -- instance_id AS auto_c02,
                    -- interface_date AS auto_c03,
                    -- invoice_type_code AS auto_c04,
                    -- level_sequence_nbr AS auto_c05,
                    -- period_end_date AS auto_c06,
                    -- period_start_date AS auto_c07,
                    -- sequence_nbr AS auto_c08,
                    -- service_line_id AS auto_c09,
                    -- service_subline_id AS auto_c010,
                    -- MIN( level_element_amt_ent ) AS auto_c011  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44     
                -- WHERE
                    -- UPPER(TRIM(INVOICE_TYPE_CODE)) = UPPER(TRIM('INV'))  
                -- GROUP BY
                    -- bill_on_date ,
                    -- contract_header_id ,
                    -- instance_id ,
                    -- interface_date ,
                    -- invoice_type_code ,
                    -- level_sequence_nbr ,
                    -- period_end_date ,
                    -- period_start_date ,
                    -- sequence_nbr ,
                    -- service_line_id ,
                    -- service_subline_id 
                -- HAVING
                    -- COUNT(*) > 1
            -- )autoAlias_37 
                -- ON (
                    -- autoAlias_37.auto_c00 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_on_date 
                    -- AND autoAlias_37.auto_c01 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contract_header_id 
                    -- AND autoAlias_37.auto_c02 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.instance_id 
                    -- AND autoAlias_37.auto_c03 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_date 
                    -- AND autoAlias_37.auto_c04 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_type_code 
                    -- AND autoAlias_37.auto_c05 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_sequence_nbr 
                    -- AND autoAlias_37.auto_c06 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_end_date 
                    -- AND autoAlias_37.auto_c07 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_start_date 
                    -- AND autoAlias_37.auto_c08 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sequence_nbr 
                    -- AND autoAlias_37.auto_c09 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_line_id 
                    -- AND autoAlias_37.auto_c010 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_subline_id 
                    -- AND autoAlias_37.auto_c011 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_element_amt_ent
                -- );
				
--Corrected Query:IDW Tool generated transformed query logic is incorrect .which gives only Inner Join result.we will achieve the expected result by adding additional condition				
INSERT OVERWRITE
        TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 
SELECT
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contract_header_id AS contract_header_id ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.instance_id AS instance_id ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_line_id AS service_line_id ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_subline_id AS service_subline_id ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sline_line_style_id AS sline_line_style_id ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sline_covered_lvl_name AS sline_covered_lvl_name ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sequence_nbr AS sequence_nbr ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.billing_stream_end_date AS billing_stream_end_date ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.billing_stream_start_date AS billing_stream_start_date ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contr_bill_stream_id AS contr_bill_stream_id ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_offset_day_nbr AS interface_offset_day_nbr ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_uom_cnt AS period_uom_cnt ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_uom_code AS period_uom_code ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_discount_amt_ent AS sl_discount_amt_ent ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_surcharge_amt_ent AS sl_surcharge_amt_ent ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_adj_prc_amt_ent AS sl_top_lvl_adj_prc_amt_ent ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_oprnd_prc_amt_ent AS sl_top_lvl_oprnd_prc_amt_ent ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_pricing_uom_qty AS sl_top_lvl_pricing_uom_qty ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_unbilled_amt_ent AS sl_unbilled_amt_ent ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.credit_amt_ent AS credit_amt_ent ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_date AS interface_date ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_sequence_nbr AS level_sequence_nbr ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_end_date AS period_end_date ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_start_date AS period_start_date ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_on_date AS bill_on_date ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_type_code AS invoice_type_code ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_amt_ent AS bill_amt_ent ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.completed_date AS completed_date ,
            CASE 
                WHEN autoAlias_37.auto_c00 IS not null and autoAlias_37.auto_c01 IS not null and autoAlias_37.auto_c02 IS not null and autoAlias_37.auto_c03 IS not null and autoAlias_37.auto_c04 IS not null and autoAlias_37.auto_c05 IS not null and autoAlias_37.auto_c06 IS not null and autoAlias_37.auto_c07 IS not null and autoAlias_37.auto_c08 IS not null and autoAlias_37.auto_c09 IS not null and autoAlias_37.auto_c010 IS not null and autoAlias_37.auto_c011 is null THEN 0 
                ELSE AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_element_amt_ent 
            END AS level_element_amt_ent ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_currency_code AS invoice_currency_code ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_date AS invoice_date ,
            AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_nbr AS invoice_nbr 
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44 AS AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44 
        Left Outer Join
            (
                SELECT
                    bill_on_date AS auto_c00,
                    contract_header_id AS auto_c01,
                    instance_id AS auto_c02,
                    interface_date AS auto_c03,
                    invoice_type_code AS auto_c04,
                    level_sequence_nbr AS auto_c05,
                    period_end_date AS auto_c06,
                    period_start_date AS auto_c07,
                    sequence_nbr AS auto_c08,
                    service_line_id AS auto_c09,
                    service_subline_id AS auto_c010,
                    MIN( level_element_amt_ent ) AS auto_c011  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_bill_sched_dn_t44     
                WHERE
                    UPPER(TRIM(INVOICE_TYPE_CODE)) = UPPER(TRIM('INV'))  
                GROUP BY
                    bill_on_date ,
                    contract_header_id ,
                    instance_id ,
                    interface_date ,
                    invoice_type_code ,
                    level_sequence_nbr ,
                    period_end_date ,
                    period_start_date ,
                    sequence_nbr ,
                    service_line_id ,
                    service_subline_id 
                HAVING
                    COUNT(*) > 1
            )autoAlias_37 
                ON (
                    autoAlias_37.auto_c00 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_on_date 
                    AND UPPER(TRIM(autoAlias_37.auto_c01)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contract_header_id)) 
                    AND UPPER(TRIM(autoAlias_37.auto_c02)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.instance_id ))
                    AND autoAlias_37.auto_c03= AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_date 
                    AND UPPER(TRIM(autoAlias_37.auto_c04)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_type_code)) 
                    AND UPPER(TRIM(autoAlias_37.auto_c05)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_sequence_nbr)) 
                    AND autoAlias_37.auto_c06 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_end_date 
                    AND autoAlias_37.auto_c07 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_start_date 
                    AND autoAlias_37.auto_c08 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sequence_nbr 
                    AND UPPER(TRIM(autoAlias_37.auto_c09)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_line_id)) 
                    AND UPPER(TRIM(autoAlias_37.auto_c010)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_subline_id)) 
                    AND autoAlias_37.auto_c011 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_element_amt_ent
                )
		Where autoAlias_37.auto_c00  Is Null				
		  AND autoAlias_37.auto_c01  Is Null
	      AND autoAlias_37.auto_c02  Is Null
          AND autoAlias_37.auto_c03  Is Null
          AND autoAlias_37.auto_c04  Is Null
          AND autoAlias_37.auto_c05  Is Null
          AND autoAlias_37.auto_c06  Is Null
          AND autoAlias_37.auto_c07  Is Null
          AND autoAlias_37.auto_c08  Is Null
          AND autoAlias_37.auto_c09  Is Null
          AND autoAlias_37.auto_c010 Is Null
          AND autoAlias_37.auto_c011 Is Null;
