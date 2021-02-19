----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:21 
--Script Name: d8_contract_line_svc_supp_ld_1.ins.sql
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
--(Bala: Corrected the translated query,The table used in join was skipped, added that and also kept only limited columns in corrected query )
--Original Query:
  --DELETE FROM contract_line_svc_supp_ld WHERE  (contract_line_supp_id, instance_id) IN    (SELECT contract_line_supp_id, instance_id       FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2)  

--Translated Query: STATUS SUCCESS
   -- /*
    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_line_svc_supp_ld SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.contract_line_svc_supp_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${EEDDWWDDBB1}.contract_line_svc_supp_ld      
            -- ) AS Q2 
                -- ON COALESCE( Q1.contract_line_supp_id ,
            -- '1' ) = COALESCE( Q2.contract_line_supp_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.instance_id ,
            -- '1' ) = COALESCE( Q2.instance_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.acctg_rule_id ,
            -- 1) = COALESCE( Q2.acctg_rule_id ,
            -- 1) 
            -- AND COALESCE( Q1.allow_before_tax_disc_flag ,
            -- '1' ) = COALESCE( Q2.allow_before_tax_disc_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.alt_batch_nbr ,
            -- 1) = COALESCE( Q2.alt_batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.alt_contract_header_id ,
            -- '1' ) = COALESCE( Q2.alt_contract_header_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.batch_nbr ,
            -- 1) = COALESCE( Q2.batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.bill_sched_lvl_type_code ,
            -- '1' ) = COALESCE( Q2.bill_sched_lvl_type_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.contract_line_id ,
            -- '1' ) = COALESCE( Q2.contract_line_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.cover_lvl_ext_amt_ent ,
            -- 1) = COALESCE( Q2.cover_lvl_ext_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.cover_lvl_list_prc_amt_ent ,
            -- 1) = COALESCE( Q2.cover_lvl_list_prc_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.cover_lvl_qty ,
            -- 1) = COALESCE( Q2.cover_lvl_qty ,
            -- 1) 
            -- AND COALESCE( Q1.cover_lvl_uom_code ,
            -- '1' ) = COALESCE( Q2.cover_lvl_uom_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.coverage_type_code ,
            -- '1' ) = COALESCE( Q2.coverage_type_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.created_by_name ,
            -- '1' ) = COALESCE( Q2.created_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.credit_amt_ent ,
            -- 1) = COALESCE( Q2.credit_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.cust_po_nbr ,
            -- '1' ) = COALESCE( Q2.cust_po_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.cust_po_nbr_rqrd_flag ,
            -- '1' ) = COALESCE( Q2.cust_po_nbr_rqrd_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.full_credit_flag ,
            -- '1' ) = COALESCE( Q2.full_credit_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.incident_severity_id ,
            -- 1) = COALESCE( Q2.incident_severity_id ,
            -- 1) 
            -- AND COALESCE( Q1.invoice_print_flag ,
            -- '1' ) = COALESCE( Q2.invoice_print_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_update_login_name ,
            -- '1' ) = COALESCE( Q2.last_update_login_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_updated_by_name ,
            -- '1' ) = COALESCE( Q2.last_updated_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.offset_duration ,
            -- 1) = COALESCE( Q2.offset_duration ,
            -- 1) 
            -- AND COALESCE( Q1.offset_period_code ,
            -- '1' ) = COALESCE( Q2.offset_period_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.override_amt_ent ,
            -- 1) = COALESCE( Q2.override_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.pricing_uom_code ,
            -- '1' ) = COALESCE( Q2.pricing_uom_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.process_defn_id ,
            -- '1' ) = COALESCE( Q2.process_defn_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.product_price_amt_ent ,
            -- 1) = COALESCE( Q2.product_price_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.product_upgrade_flag ,
            -- '1' ) = COALESCE( Q2.product_upgrade_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.react_active_flag ,
            -- '1' ) = COALESCE( Q2.react_active_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.service_price_amt_ent ,
            -- 1) = COALESCE( Q2.service_price_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.suppressed_credit_amt_ent ,
            -- 1) = COALESCE( Q2.suppressed_credit_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.sync_date_installed_flag ,
            -- '1' ) = COALESCE( Q2.sync_date_installed_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.tax_amt_ent ,
            -- 1) = COALESCE( Q2.tax_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.tax_exempt_status_code ,
            -- '1' ) = COALESCE( Q2.tax_exempt_status_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.tax_exemption_id ,
            -- 1) = COALESCE( Q2.tax_exemption_id ,
            -- 1) 
            -- AND COALESCE( Q1.vat_tax_id ,
            -- 1) = COALESCE( Q2.vat_tax_id ,
            -- 1) 
            -- AND COALESCE( Q1.tax_inclusive_flag ,
            -- '1' ) = COALESCE( Q2.tax_inclusive_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.top_lvl_adj_prc_amt_ent ,
            -- 1) = COALESCE( Q2.top_lvl_adj_prc_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.top_lvl_operator_code ,
            -- '1' ) = COALESCE( Q2.top_lvl_operator_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.top_lvl_oprnd_prc_amt_ent ,
            -- 1) = COALESCE( Q2.top_lvl_oprnd_prc_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.top_lvl_pricing_uom_code ,
            -- '1' ) = COALESCE( Q2.top_lvl_pricing_uom_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.top_lvl_pricing_uom_qty ,
            -- 1) = COALESCE( Q2.top_lvl_pricing_uom_qty ,
            -- 1) 
            -- AND COALESCE( Q1.top_lvl_qty ,
            -- 1) = COALESCE( Q2.top_lvl_qty ,
            -- 1) 
            -- AND COALESCE( Q1.tran_code ,
            -- '1' ) = COALESCE( Q2.tran_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.transfer_option_code ,
            -- '1' ) = COALESCE( Q2.transfer_option_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.unbilled_amt_ent ,
            -- 1) = COALESCE( Q2.unbilled_amt_ent ,
            -- 1) 
            -- AND COALESCE( Q1.work_thru_flag ,
            -- '1' ) = COALESCE( Q2.work_thru_flag ,
            -- '1' ) 
        -- WHERE
            -- Q2.contract_line_supp_id IS NULL 
            -- AND Q2.instance_id IS NULL 
            -- AND Q2.acctg_rule_id IS NULL 
            -- AND Q2.allow_before_tax_disc_flag IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.alt_contract_header_id IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.bill_sched_lvl_type_code IS NULL 
            -- AND Q2.contract_line_id IS NULL 
            -- AND Q2.cover_lvl_ext_amt_ent IS NULL 
            -- AND Q2.cover_lvl_list_prc_amt_ent IS NULL 
            -- AND Q2.cover_lvl_qty IS NULL 
            -- AND Q2.cover_lvl_uom_code IS NULL 
            -- AND Q2.coverage_type_code IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.credit_amt_ent IS NULL 
            -- AND Q2.cust_po_nbr IS NULL 
            -- AND Q2.cust_po_nbr_rqrd_flag IS NULL 
            -- AND Q2.full_credit_flag IS NULL 
            -- AND Q2.incident_severity_id IS NULL 
            -- AND Q2.invoice_print_flag IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_update_login_name IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.offset_duration IS NULL 
            -- AND Q2.offset_period_code IS NULL 
            -- AND Q2.override_amt_ent IS NULL 
            -- AND Q2.pricing_uom_code IS NULL 
            -- AND Q2.process_defn_id IS NULL 
            -- AND Q2.product_price_amt_ent IS NULL 
            -- AND Q2.product_upgrade_flag IS NULL 
            -- AND Q2.react_active_flag IS NULL 
            -- AND Q2.service_price_amt_ent IS NULL 
            -- AND Q2.suppressed_credit_amt_ent IS NULL 
            -- AND Q2.sync_date_installed_flag IS NULL 
            -- AND Q2.tax_amt_ent IS NULL 
            -- AND Q2.tax_exempt_status_code IS NULL 
            -- AND Q2.tax_exemption_id IS NULL 
            -- AND Q2.vat_tax_id IS NULL 
            -- AND Q2.tax_inclusive_flag IS NULL 
            -- AND Q2.top_lvl_adj_prc_amt_ent IS NULL 
            -- AND Q2.top_lvl_operator_code IS NULL 
            -- AND Q2.top_lvl_oprnd_prc_amt_ent IS NULL 
            -- AND Q2.top_lvl_pricing_uom_code IS NULL 
            -- AND Q2.top_lvl_pricing_uom_qty IS NULL 
            -- AND Q2.top_lvl_qty IS NULL 
            -- AND Q2.tran_code IS NULL 
            -- AND Q2.transfer_option_code IS NULL 
            -- AND Q2.unbilled_amt_ent IS NULL 
            -- AND Q2.work_thru_flag IS NULL;*/
--Corrected Query:
INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.contract_line_svc_supp_ld
select
   Q1.* 
From
   ${EEDDWWDDBB1}.contract_line_svc_supp_ld Q1 
   Left Join
      (
         SELECT
            contract_line_supp_id,
            instance_id 
         FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2
         
      )Q2
       On trim(upper(Q1.contract_line_supp_id)) = trim(upper(Q2.contract_line_supp_id)) 
      And trim(upper(Q1.instance_id)) = trim(upper(Q2.instance_id)) 
   Where
   Q2.contract_line_supp_id Is NULL 
   And Q2.instance_id Is Null ;
           

--Original Query:
  --LOCK TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2 FOR ACCESS  INSERT INTO contract_line_svc_supp_ld (     contract_line_supp_id  ,instance_id    ,acctg_rule_id  ,allow_before_tax_disc_flag     ,alt_as_of_date_time    ,alt_batch_nbr  ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,bill_sched_lvl_type_code   ,contract_line_id   ,cover_lvl_ext_amt_ent  ,cover_lvl_list_prc_amt_ent     ,cover_lvl_qty  ,cover_lvl_uom_code     ,coverage_type_code     ,created_by_name    ,creation_date_time     ,credit_amt_ent     ,cust_po_nbr    ,cust_po_nbr_rqrd_flag  ,full_credit_flag   ,incident_severity_id   ,invoice_print_flag     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,offset_duration    ,offset_period_code     ,override_amt_ent   ,pricing_uom_code   ,process_defn_id    ,product_price_amt_ent  ,product_upgrade_flag   ,react_active_flag  ,service_price_amt_ent  ,suppressed_credit_amt_ent  ,sync_date_installed_flag   ,tax_amt_ent    ,tax_exempt_status_code     ,tax_exemption_id   ,vat_tax_id     ,tax_inclusive_flag     ,top_lvl_adj_prc_amt_ent    ,top_lvl_operator_code  ,top_lvl_oprnd_prc_amt_ent  ,top_lvl_pricing_uom_code   ,top_lvl_pricing_uom_qty    ,top_lvl_qty    ,tran_code  ,transfer_option_code   ,unbilled_amt_ent   ,work_thru_flag ) SELECT     contract_line_supp_id  ,instance_id    ,acctg_rule_id  ,allow_before_tax_disc_flag     ,alt_as_of_date_time    ,alt_batch_nbr  ,alt_contract_header_id     ,as_of_date_time    ,batch_nbr  ,bill_sched_lvl_type_code   ,contract_line_id   ,cover_lvl_ext_amt_ent  ,cover_lvl_list_prc_amt_ent     ,cover_lvl_qty  ,cover_lvl_uom_code     ,coverage_type_code     ,created_by_name    ,creation_date_time     ,credit_amt_ent     ,cust_po_nbr    ,cust_po_nbr_rqrd_flag  ,full_credit_flag   ,incident_severity_id   ,invoice_print_flag     ,last_update_date_time  ,last_update_login_name     ,last_updated_by_name   ,offset_duration    ,offset_period_code     ,override_amt_ent   ,pricing_uom_code   ,process_defn_id    ,product_price_amt_ent  ,product_upgrade_flag   ,react_active_flag  ,service_price_amt_ent  ,suppressed_credit_amt_ent  ,sync_date_installed_flag   ,tax_amt_ent    ,tax_exempt_status_code     ,tax_exemption_id   ,vat_tax_id     ,tax_inclusive_flag     ,top_lvl_adj_prc_amt_ent    ,top_lvl_operator_code  ,top_lvl_oprnd_prc_amt_ent  ,top_lvl_pricing_uom_code   ,top_lvl_pricing_uom_qty    ,top_lvl_qty    ,tran_code  ,transfer_option_code   ,unbilled_amt_ent   ,work_thru_flag FROM    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_line_svc_supp_ld           SELECT
            contract_line_supp_id,
            instance_id,
            acctg_rule_id,
            allow_before_tax_disc_flag,
            alt_as_of_date_time,
            alt_batch_nbr,
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
            work_thru_flag  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contract_line_svc_supp_t2;
