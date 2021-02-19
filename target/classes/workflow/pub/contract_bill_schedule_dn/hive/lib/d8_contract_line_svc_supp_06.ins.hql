----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:20 
--Script Name: d8_contract_line_svc_supp_06.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1;

--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1 (      contract_line_supp_id  ,instance_id    ,acctg_rule_id  ,allow_before_tax_disc_flag     ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,bill_sched_lvl_type_code   ,contract_line_id   ,cover_lvl_ext_amt_ent  ,cover_lvl_list_prc_amt_ent     ,cover_lvl_qty  ,cover_lvl_uom_code     ,coverage_type_code     ,created_by_name    ,creation_date_time     ,credit_amt_ent     ,cust_po_nbr    ,cust_po_nbr_rqrd_flag  ,full_credit_flag   ,incident_severity_id   ,invoice_print_flag     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,offset_duration    ,offset_period_code     ,override_amt_ent   ,pricing_uom_code   ,process_defn_id    ,product_price_amt_ent  ,product_upgrade_flag   ,react_active_flag  ,service_price_amt_ent  ,suppressed_credit_amt_ent  ,sync_date_installed_flag   ,tax_amt_ent    ,tax_exempt_status_code     ,tax_exemption_id   ,vat_tax_id     ,tax_inclusive_flag     ,top_lvl_adj_prc_amt_ent    ,top_lvl_operator_code  ,top_lvl_oprnd_prc_amt_ent  ,top_lvl_pricing_uom_code   ,top_lvl_pricing_uom_qty    ,top_lvl_qty    ,tran_code  ,transfer_option_code   ,unbilled_amt_ent   ,work_thru_flag ) SELECT     contract_line_supp_id  ,instance_id    ,acctg_rule_id  ,allow_before_tax_disc_flag     ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,bill_sched_lvl_type_code   ,contract_line_id   ,cover_lvl_ext_amt_ent  ,cover_lvl_list_prc_amt_ent     ,cover_lvl_qty  ,cover_lvl_uom_code     ,coverage_type_code     ,created_by_name    ,creation_date_time     ,credit_amt_ent     ,cust_po_nbr    ,cust_po_nbr_rqrd_flag  ,full_credit_flag   ,incident_severity_id   ,invoice_print_flag     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,offset_duration    ,offset_period_code     ,override_amt_ent   ,pricing_uom_code   ,process_defn_id    ,product_price_amt_ent  ,product_upgrade_flag   ,react_active_flag  ,service_price_amt_ent  ,suppressed_credit_amt_ent  ,sync_date_installed_flag   ,tax_amt_ent    ,tax_exempt_status_code     ,tax_exemption_id   ,vat_tax_id     ,tax_inclusive_flag     ,top_lvl_adj_prc_amt_ent    ,top_lvl_operator_code  ,top_lvl_oprnd_prc_amt_ent  ,top_lvl_pricing_uom_code   ,top_lvl_pricing_uom_qty    ,top_lvl_qty    ,tran_code  ,transfer_option_code   ,unbilled_amt_ent   ,work_thru_flag FROM    ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml WHERE    (CAST(as_of_date_time AS CHAR(26))||batch_nbr   ,contract_line_supp_id  ,instance_id)   IN  (SELECT  MAX(CAST(as_of_date_time AS CHAR(26))||batch_nbr)      ,contract_line_supp_id      ,instance_id    FROM ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml     WHERE tran_code = 'D'   GROUP BY 2,3) AND   tran_code = 'D'  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1           SELECT
            -- contract_line_supp_id,
            -- instance_id,
            -- acctg_rule_id,
            -- allow_before_tax_disc_flag,
            -- alt_contract_header_id,
            -- as_of_date_time,
            -- batch_nbr,
            -- bill_sched_lvl_type_code,
            -- contract_line_id,
            -- cover_lvl_ext_amt_ent,
            -- cover_lvl_list_prc_amt_ent,
            -- cover_lvl_qty,
            -- cover_lvl_uom_code,
            -- coverage_type_code,
            -- created_by_name,
            -- creation_date_time,
            -- credit_amt_ent,
            -- cust_po_nbr,
            -- cust_po_nbr_rqrd_flag,
            -- full_credit_flag,
            -- incident_severity_id,
            -- invoice_print_flag,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- offset_duration,
            -- offset_period_code,
            -- override_amt_ent,
            -- pricing_uom_code,
            -- process_defn_id,
            -- product_price_amt_ent,
            -- product_upgrade_flag,
            -- react_active_flag,
            -- service_price_amt_ent,
            -- suppressed_credit_amt_ent,
            -- sync_date_installed_flag,
            -- tax_amt_ent,
            -- tax_exempt_status_code,
            -- tax_exemption_id,
            -- vat_tax_id,
            -- tax_inclusive_flag,
            -- top_lvl_adj_prc_amt_ent,
            -- top_lvl_operator_code,
            -- top_lvl_oprnd_prc_amt_ent,
            -- top_lvl_pricing_uom_code,
            -- top_lvl_pricing_uom_qty,
            -- top_lvl_qty,
            -- tran_code,
            -- transfer_option_code,
            -- unbilled_amt_ent,
            -- work_thru_flag  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- contract_line_supp_id AS auto_c01,
                    -- instance_id AS auto_c02  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml     
                -- WHERE
                    -- upper(trim(tran_code)) = upper(trim('D'))  
                -- GROUP BY
                    -- contract_line_supp_id ,
                    -- instance_id 
            -- ) AS autoAlias_177 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_177.AUTO_C00 
                -- AND upper(trim(contract_line_supp_id)) = upper(trim(autoAlias_177.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_177.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) = upper(trim('D'));
                
--Corrected Query: Transformed Query build with self join for max values.by using rank() function we can achieve the same result    
      with qq1 as (
SELECT
                contract_line_supp_id,
                instance_id,
                acctg_rule_id,
                allow_before_tax_disc_flag,
                alt_contract_header_id,
                as_of_date_time,
                batch_nbr,
                bill_sched_lvl_type_code,
                contract_line_id,
                cover_lvl_ext_amt_ent,
                cover_lvl_list_prc_amt_ent,
                cover_lvl_qty,
                cover_lvl_uom_code,
                coverage_type_code,
                created_by_name,
                creation_date_time,
                credit_amt_ent,
                cust_po_nbr,
                cust_po_nbr_rqrd_flag,
                full_credit_flag,
                incident_severity_id,
                invoice_print_flag,
                last_update_date_time,
                last_update_login_name,
                last_updated_by_name,
                offset_duration,
                offset_period_code,
                override_amt_ent,
                pricing_uom_code,
                process_defn_id,
                product_price_amt_ent,
                product_upgrade_flag,
                react_active_flag,
                service_price_amt_ent,
                suppressed_credit_amt_ent,
                sync_date_installed_flag,
                tax_amt_ent,
                tax_exempt_status_code,
                tax_exemption_id,
                vat_tax_id,
                tax_inclusive_flag,
                top_lvl_adj_prc_amt_ent,
                top_lvl_operator_code,
                top_lvl_oprnd_prc_amt_ent,
                top_lvl_pricing_uom_code,
                top_lvl_pricing_uom_qty,
                top_lvl_qty,
                tran_code,
                transfer_option_code,
                unbilled_amt_ent,
                work_thru_flag,
                ROW_NUMBER() over (PARTITION BY contract_line_supp_id , instance_id 
            ORDER BY
                CONCAT (CAST (as_of_date_time AS CHAR(26))  , batch_nbr) DESC) As RNK 
            FROM
                ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml 
            WHERE
                upper(trim(tran_code)) = upper(trim('D')) ) 
INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1 
SELECT
            EDW2.contract_line_supp_id,
            EDW2.instance_id,
            EDW2.acctg_rule_id,
            EDW2.allow_before_tax_disc_flag,
            EDW2.alt_contract_header_id,
            EDW2.as_of_date_time,
            EDW2.batch_nbr,
            EDW2.bill_sched_lvl_type_code,
            EDW2.contract_line_id,
            EDW2.cover_lvl_ext_amt_ent,
            EDW2.cover_lvl_list_prc_amt_ent,
            EDW2.cover_lvl_qty,
            EDW2.cover_lvl_uom_code,
            EDW2.coverage_type_code,
            EDW2.created_by_name,
            EDW2.creation_date_time,
            EDW2.credit_amt_ent,
            EDW2.cust_po_nbr,
            EDW2.cust_po_nbr_rqrd_flag,
            EDW2.full_credit_flag,
            EDW2.incident_severity_id,
            EDW2.invoice_print_flag,
            EDW2.last_update_date_time,
            EDW2.last_update_login_name,
            EDW2.last_updated_by_name,
            EDW2.offset_duration,
            EDW2.offset_period_code,
            EDW2.override_amt_ent,
            EDW2.pricing_uom_code,
            EDW2.process_defn_id,
            EDW2.product_price_amt_ent,
            EDW2.product_upgrade_flag,
            EDW2.react_active_flag,
            EDW2.service_price_amt_ent,
            EDW2.suppressed_credit_amt_ent,
            EDW2.sync_date_installed_flag,
            EDW2.tax_amt_ent,
            EDW2.tax_exempt_status_code,
            EDW2.tax_exemption_id,
            EDW2.vat_tax_id,
            EDW2.tax_inclusive_flag,
            EDW2.top_lvl_adj_prc_amt_ent,
            EDW2.top_lvl_operator_code,
            EDW2.top_lvl_oprnd_prc_amt_ent,
            EDW2.top_lvl_pricing_uom_code,
            EDW2.top_lvl_pricing_uom_qty,
            EDW2.top_lvl_qty,
            EDW2.tran_code,
            EDW2.transfer_option_code,
            EDW2.unbilled_amt_ent,
            EDW2.work_thru_flag 
FROM      ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_ml 
 EDW2 
  INNER JOIN (
    SELECT 
            contract_line_supp_id,
            instance_id,
            acctg_rule_id,
            allow_before_tax_disc_flag,
            alt_contract_header_id,
            as_of_date_time,
            batch_nbr,
            bill_sched_lvl_type_code,
            contract_line_id,
            cover_lvl_ext_amt_ent,
            cover_lvl_list_prc_amt_ent,
            cover_lvl_qty,
            cover_lvl_uom_code,
            coverage_type_code,
            created_by_name,
            creation_date_time,
            credit_amt_ent,
            cust_po_nbr,
            cust_po_nbr_rqrd_flag,
            full_credit_flag,
            incident_severity_id,
            invoice_print_flag,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            offset_duration,
            offset_period_code,
            override_amt_ent,
            pricing_uom_code,
            process_defn_id,
            product_price_amt_ent,
            product_upgrade_flag,
            react_active_flag,
            service_price_amt_ent,
            suppressed_credit_amt_ent,
            sync_date_installed_flag,
            tax_amt_ent,
            tax_exempt_status_code,
            tax_exemption_id,
            vat_tax_id,
            tax_inclusive_flag,
            top_lvl_adj_prc_amt_ent,
            top_lvl_operator_code,
            top_lvl_oprnd_prc_amt_ent,
            top_lvl_pricing_uom_code,
            top_lvl_pricing_uom_qty,
            top_lvl_qty,
            tran_code,
            transfer_option_code,
            unbilled_amt_ent,
            work_thru_flag , 
            RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND EDW2.contract_line_supp_id =IBVL.contract_line_supp_id 
  AND upper(trim(EDW2.tran_code)) = upper(trim('D'));

--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2;

--(Nikhil: Replaced the Inequality condition with CTE block)
--Original Query:
  --LOCK TABLE ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2 (   contract_line_supp_id  ,instance_id    ,acctg_rule_id  ,allow_before_tax_disc_flag     ,alt_as_of_date_time    ,alt_batch_nbr  ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,bill_sched_lvl_type_code   ,contract_line_id   ,cover_lvl_ext_amt_ent  ,cover_lvl_list_prc_amt_ent     ,cover_lvl_qty  ,cover_lvl_uom_code     ,coverage_type_code     ,created_by_name    ,creation_date_time     ,credit_amt_ent     ,cust_po_nbr    ,cust_po_nbr_rqrd_flag  ,full_credit_flag   ,incident_severity_id   ,invoice_print_flag     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,offset_duration    ,offset_period_code     ,override_amt_ent   ,pricing_uom_code   ,process_defn_id    ,product_price_amt_ent  ,product_upgrade_flag   ,react_active_flag  ,service_price_amt_ent  ,suppressed_credit_amt_ent  ,sync_date_installed_flag   ,tax_amt_ent    ,tax_exempt_status_code     ,tax_exemption_id   ,vat_tax_id     ,tax_inclusive_flag     ,top_lvl_adj_prc_amt_ent    ,top_lvl_operator_code  ,top_lvl_oprnd_prc_amt_ent  ,top_lvl_pricing_uom_code   ,top_lvl_pricing_uom_qty    ,top_lvl_qty    ,tran_code  ,transfer_option_code   ,unbilled_amt_ent   ,work_thru_flag ) SELECT     l.contract_line_supp_id    ,l.instance_id  ,l.acctg_rule_id    ,l.allow_before_tax_disc_flag   ,t.as_of_date_time  ,t.batch_nbr    ,l.alt_contract_header_id   ,l.as_of_date_time  ,l.batch_nbr    ,l.bill_sched_lvl_type_code     ,l.contract_line_id     ,l.cover_lvl_ext_amt_ent    ,l.cover_lvl_list_prc_amt_ent   ,l.cover_lvl_qty    ,l.cover_lvl_uom_code   ,l.coverage_type_code   ,l.created_by_name  ,l.creation_date_time   ,l.credit_amt_ent   ,l.cust_po_nbr  ,l.cust_po_nbr_rqrd_flag    ,l.full_credit_flag     ,l.incident_severity_id     ,l.invoice_print_flag   ,l.last_update_date_time    ,l.last_update_login_name   ,l.last_updated_by_name     ,l.offset_duration  ,l.offset_period_code   ,l.override_amt_ent     ,l.pricing_uom_code     ,l.process_defn_id  ,l.product_price_amt_ent    ,l.product_upgrade_flag     ,l.react_active_flag    ,l.service_price_amt_ent    ,l.suppressed_credit_amt_ent    ,l.sync_date_installed_flag     ,l.tax_amt_ent  ,l.tax_exempt_status_code   ,l.tax_exemption_id     ,l.vat_tax_id   ,l.tax_inclusive_flag   ,l.top_lvl_adj_prc_amt_ent  ,l.top_lvl_operator_code    ,l.top_lvl_oprnd_prc_amt_ent    ,l.top_lvl_pricing_uom_code     ,l.top_lvl_pricing_uom_qty  ,l.top_lvl_qty  ,t.tran_code    ,l.transfer_option_code     ,l.unbilled_amt_ent     ,l.work_thru_flag FROM  ${EEDDWWDDBB1}.contract_line_svc_supp_ld l,     ${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1 t WHERE  l.contract_line_supp_id = t.contract_line_supp_id AND   l.instance_id = t.instance_id AND   CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <     CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2           SELECT
            l.contract_line_supp_id,
            l.instance_id,
            l.acctg_rule_id,
            l.allow_before_tax_disc_flag,
            t.as_of_date_time,
            t.batch_nbr,
            l.alt_contract_header_id,
            l.as_of_date_time,
            l.batch_nbr,
            l.bill_sched_lvl_type_code,
            l.contract_line_id,
            l.cover_lvl_ext_amt_ent,
            l.cover_lvl_list_prc_amt_ent,
            l.cover_lvl_qty,
            l.cover_lvl_uom_code,
            l.coverage_type_code,
            l.created_by_name,
            l.creation_date_time,
            l.credit_amt_ent,
            null,
            l.cust_po_nbr,
            l.cust_po_nbr_rqrd_flag,
            l.full_credit_flag,
            l.incident_severity_id,
            l.invoice_print_flag,
            l.last_update_date_time,
            l.last_update_login_name,
            l.last_updated_by_name,
            l.offset_duration,
            l.offset_period_code,
            l.override_amt_ent,
            l.pricing_uom_code,
            l.process_defn_id,
            l.product_price_amt_ent,
            l.product_upgrade_flag,
            l.react_active_flag,
            l.service_price_amt_ent,
            l.suppressed_credit_amt_ent,
            l.sync_date_installed_flag,
            l.tax_amt_ent,
            l.tax_exempt_status_code,
            l.tax_exemption_id,
            l.vat_tax_id,
            l.tax_inclusive_flag,
            l.top_lvl_adj_prc_amt_ent,
            l.top_lvl_operator_code,
            l.top_lvl_oprnd_prc_amt_ent,
            l.top_lvl_pricing_uom_code,
            l.top_lvl_pricing_uom_qty,
            l.top_lvl_qty,
            t.tran_code,
            l.transfer_option_code,
            l.unbilled_amt_ent,
            l.work_thru_flag  
        FROM
            ${EEDDWWDDBB1}.contract_line_svc_supp_ld    AS l   ,
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1    AS t   
        WHERE
            trim(upper(l.contract_line_supp_id)) = trim(upper(t.contract_line_supp_id))  
            AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))   
            AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);
--Translated Query:(Corrected Manually)
--Translated Query:(Corrected Manually)
--(Nikhil: Replaced the Inequality condition with CTE block)
WITH CTE AS 
(
SELECT t.contract_line_supp_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
 CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat
from  ${EEDDWWDDBB1}.contract_line_svc_supp_ld    AS l
INNER JOIN 
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t1    AS t   
ON        trim(upper(l.contract_line_supp_id)) = trim(upper(t.contract_line_supp_id))  
            AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))
            
            WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
)
INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2           SELECT
            l.contract_line_supp_id,
            l.instance_id,
            l.acctg_rule_id,
            l.allow_before_tax_disc_flag,
            t.as_of_date_time,
            t.batch_nbr,
            l.alt_contract_header_id,
            l.as_of_date_time,
            l.batch_nbr,
            l.bill_sched_lvl_type_code,
            l.contract_line_id,
            l.cover_lvl_ext_amt_ent,
            l.cover_lvl_list_prc_amt_ent,
            l.cover_lvl_qty,
            l.cover_lvl_uom_code,
            l.coverage_type_code,
            l.created_by_name,
            l.creation_date_time,
            l.credit_amt_ent,
            null,
            l.cust_po_nbr,
            l.cust_po_nbr_rqrd_flag,
            l.full_credit_flag,
            l.incident_severity_id,
            l.invoice_print_flag,
            l.last_update_date_time,
            l.last_update_login_name,
            l.last_updated_by_name,
            l.offset_duration,
            l.offset_period_code,
            l.override_amt_ent,
            l.pricing_uom_code,
            l.process_defn_id,
            l.product_price_amt_ent,
            l.product_upgrade_flag,
            l.react_active_flag,
            l.service_price_amt_ent,
            l.suppressed_credit_amt_ent,
            l.sync_date_installed_flag,
            l.tax_amt_ent,
            l.tax_exempt_status_code,
            l.tax_exemption_id,
            l.vat_tax_id,
            l.tax_inclusive_flag,
            l.top_lvl_adj_prc_amt_ent,
            l.top_lvl_operator_code,
            l.top_lvl_oprnd_prc_amt_ent,
            l.top_lvl_pricing_uom_code,
            l.top_lvl_pricing_uom_qty,
            l.top_lvl_qty,
            t.tran_code,
            l.transfer_option_code,
            l.unbilled_amt_ent,
            l.work_thru_flag  
        FROM
            ${EEDDWWDDBB1}.contract_line_svc_supp_ld    AS l   
            INNER JOIN CTE AS t 
            ON
            trim(upper(l.contract_line_supp_id)) = trim(upper(t.contract_line_supp_id))  
            AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))   
            WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat;
