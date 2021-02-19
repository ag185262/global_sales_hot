----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:08 
--Script Name: d8_contract_header_svc_supp_05.ins.sql
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
  --DELETE FROM contract_header_svc_supp_ld  WHERE (contract_header_supp_id,instance_id) IN (SELECT contract_header_supp_id,instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1     WHERE (contract_header_supp_id, instance_id) NOT IN   (SELECT contract_header_supp_id,instance_id     FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2   WHERE current_record_ind = 'D'  ) )  

--Translated Query: STATUS SUCCESS

    -- INSERT OVERWRITE
        -- TABLE ${EEDDWWDDBB1}.contract_header_svc_supp_ld SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.contract_header_svc_supp_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${EEDDWWDDBB1}.contract_header_svc_supp_ld      
            -- ) AS Q2 
                -- ON COALESCE( Q1.contract_header_supp_id ,
            -- '1' ) = COALESCE( Q2.contract_header_supp_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.instance_id ,
            -- '1' ) = COALESCE( Q2.instance_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.acctg_rule_id ,
            -- 1) = COALESCE( Q2.acctg_rule_id ,
            -- 1) 
            -- AND COALESCE( Q1.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.alt_as_of_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.alt_batch_nbr ,
            -- 1) = COALESCE( Q2.alt_batch_nbr ,
            -- 1) 
            -- AND COALESCE( Q1.ar_interface_flag ,
            -- '1' ) = COALESCE( Q2.ar_interface_flag ,
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
            -- AND COALESCE( Q1.contract_header_id ,
            -- '1' ) = COALESCE( Q2.contract_header_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.created_by_name ,
            -- '1' ) = COALESCE( Q2.created_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.cust_trx_type_id ,
            -- '1' ) = COALESCE( Q2.cust_trx_type_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.est_revn_date ,
            -- CURRENT_DATE() ) = COALESCE( Q2.est_revn_date ,
            -- CURRENT_DATE() ) 
            -- AND COALESCE( Q1.est_revn_period_used_code ,
            -- '1' ) = COALESCE( Q2.est_revn_period_used_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.est_revn_pct ,
            -- 1) = COALESCE( Q2.est_revn_pct ,
            -- 1) 
            -- AND COALESCE( Q1.est_revn_used_duration ,
            -- 1) = COALESCE( Q2.est_revn_used_duration ,
            -- 1) 
            -- AND COALESCE( Q1.est_revn_used_pct ,
            -- 1) = COALESCE( Q2.est_revn_used_pct ,
            -- 1) 
            -- AND COALESCE( Q1.grace_duration ,
            -- 1) = COALESCE( Q2.grace_duration ,
            -- 1) 
            -- AND COALESCE( Q1.grace_period_code ,
            -- '1' ) = COALESCE( Q2.grace_period_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.hold_bill_flag ,
            -- '1' ) = COALESCE( Q2.hold_bill_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.invc_print_profile_flag ,
            -- '1' ) = COALESCE( Q2.invc_print_profile_flag ,
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
            -- AND COALESCE( Q1.payment_type_code ,
            -- '1' ) = COALESCE( Q2.payment_type_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.renewal_po_nbr ,
            -- '1' ) = COALESCE( Q2.renewal_po_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.renewal_po_required_flag ,
            -- '1' ) = COALESCE( Q2.renewal_po_required_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.renewal_po_used_flag ,
            -- '1' ) = COALESCE( Q2.renewal_po_used_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.rnw_est_revenue_duration ,
            -- 1) = COALESCE( Q2.rnw_est_revenue_duration ,
            -- 1) 
            -- AND COALESCE( Q1.rnw_est_revenue_pct ,
            -- 1) = COALESCE( Q2.rnw_est_revenue_pct ,
            -- 1) 
            -- AND COALESCE( Q1.rnw_est_revn_period_code ,
            -- '1' ) = COALESCE( Q2.rnw_est_revn_period_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.rnw_grace_period_used_code ,
            -- '1' ) = COALESCE( Q2.rnw_grace_period_used_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.rnw_grace_used_duration ,
            -- 1) = COALESCE( Q2.rnw_grace_used_duration ,
            -- 1) 
            -- AND COALESCE( Q1.rnw_markup_pct ,
            -- 1) = COALESCE( Q2.rnw_markup_pct ,
            -- 1) 
            -- AND COALESCE( Q1.rnw_markup_used_pct ,
            -- 1) = COALESCE( Q2.rnw_markup_used_pct ,
            -- 1) 
            -- AND COALESCE( Q1.rnw_notification_to_id ,
            -- 1) = COALESCE( Q2.rnw_notification_to_id ,
            -- 1) 
            -- AND COALESCE( Q1.rnw_notification_to_name ,
            -- '1' ) = COALESCE( Q2.rnw_notification_to_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.rnw_price_list_id ,
            -- 1) = COALESCE( Q2.rnw_price_list_id ,
            -- 1) 
            -- AND COALESCE( Q1.rnw_price_list_used_id ,
            -- 1) = COALESCE( Q2.rnw_price_list_used_id ,
            -- 1) 
            -- AND COALESCE( Q1.rnw_pricing_type_code ,
            -- '1' ) = COALESCE( Q2.rnw_pricing_type_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.rnw_pricing_used_type_code ,
            -- '1' ) = COALESCE( Q2.rnw_pricing_used_type_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.rnw_used_type_code ,
            -- '1' ) = COALESCE( Q2.rnw_used_type_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.service_po_nbr ,
            -- '1' ) = COALESCE( Q2.service_po_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.service_po_required_flag ,
            -- '1' ) = COALESCE( Q2.service_po_required_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.summary_trx_flag ,
            -- '1' ) = COALESCE( Q2.summary_trx_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.tax_exempt_status_code ,
            -- '1' ) = COALESCE( Q2.tax_exempt_status_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.tax_exemption_id ,
            -- 1) = COALESCE( Q2.tax_exemption_id ,
            -- 1) 
            -- AND COALESCE( Q1.tran_code ,
            -- '1' ) = COALESCE( Q2.tran_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.renewal_approval_type_used ,
            -- '1' ) = COALESCE( Q2.renewal_approval_type_used ,
            -- '1' ) 
            -- AND COALESCE( Q1.evn_thershhold_amt ,
            -- 1) = COALESCE( Q2.evn_thershhold_amt ,
            -- 1) 
            -- AND COALESCE( Q1.ern_thershhold_amt ,
            -- 1) = COALESCE( Q2.ern_thershhold_amt ,
            -- 1) 
        -- WHERE
            -- Q2.contract_header_supp_id IS NULL 
            -- AND Q2.instance_id IS NULL 
            -- AND Q2.acctg_rule_id IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.ar_interface_flag IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.bill_sched_lvl_type_code IS NULL 
            -- AND Q2.contract_header_id IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.cust_trx_type_id IS NULL 
            -- AND Q2.est_revn_date IS NULL 
            -- AND Q2.est_revn_period_used_code IS NULL 
            -- AND Q2.est_revn_pct IS NULL 
            -- AND Q2.est_revn_used_duration IS NULL 
            -- AND Q2.est_revn_used_pct IS NULL 
            -- AND Q2.grace_duration IS NULL 
            -- AND Q2.grace_period_code IS NULL 
            -- AND Q2.hold_bill_flag IS NULL 
            -- AND Q2.invc_print_profile_flag IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_update_login_name IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.payment_type_code IS NULL 
            -- AND Q2.renewal_po_nbr IS NULL 
            -- AND Q2.renewal_po_required_flag IS NULL 
            -- AND Q2.renewal_po_used_flag IS NULL 
            -- AND Q2.rnw_est_revenue_duration IS NULL 
            -- AND Q2.rnw_est_revenue_pct IS NULL 
            -- AND Q2.rnw_est_revn_period_code IS NULL 
            -- AND Q2.rnw_grace_period_used_code IS NULL 
            -- AND Q2.rnw_grace_used_duration IS NULL 
            -- AND Q2.rnw_markup_pct IS NULL 
            -- AND Q2.rnw_markup_used_pct IS NULL 
            -- AND Q2.rnw_notification_to_id IS NULL 
            -- AND Q2.rnw_notification_to_name IS NULL 
            -- AND Q2.rnw_price_list_id IS NULL 
            -- AND Q2.rnw_price_list_used_id IS NULL 
            -- AND Q2.rnw_pricing_type_code IS NULL 
            -- AND Q2.rnw_pricing_used_type_code IS NULL 
            -- AND Q2.rnw_used_type_code IS NULL 
            -- AND Q2.service_po_nbr IS NULL 
            -- AND Q2.service_po_required_flag IS NULL 
            -- AND Q2.summary_trx_flag IS NULL 
            -- AND Q2.tax_exempt_status_code IS NULL 
            -- AND Q2.tax_exemption_id IS NULL 
            -- AND Q2.tran_code IS NULL 
            -- AND Q2.renewal_approval_type_used IS NULL 
            -- AND Q2.evn_thershhold_amt IS NULL 
            -- AND Q2.ern_thershhold_amt IS NULL;

--Corrected Query:Transformed Query logic is incorrect.In below query resolved.             
        INSERT OVERWRITE
            TABLE ${EEDDWWDDBB1}.contract_header_svc_supp_ld
        select
        Q1.* 
        From
        ${EEDDWWDDBB1}.contract_header_svc_supp_ld Q1 
        Left Join
            (
                select
                    A.contract_header_supp_id,
                    A.instance_id 
                From
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 As A 
                    Left Join
                    (
                        SELECT
                            contract_header_supp_id,
                            instance_id 
                        FROM
                            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2
                        WHERE
                            current_record_ind = 'D' 
                    )
                    As B 
                    On A.contract_header_supp_id = b.contract_header_supp_id 
                    And A.instance_id = B.instance_id 
                Where
                    B.contract_header_supp_id Is NULL 
                    And B.instance_id Is Null 
            )
            As Q2 
            On upper(trim( Q1.contract_header_supp_id)) = upper(trim( Q2.contract_header_supp_id ))
            And upper(trim( Q1.instance_id)) = upper(trim( Q2.instance_id ))
        Where
        Q2.contract_header_supp_id Is NULL 
        And Q2.instance_id Is Null ;
        
--Original Query:
  --LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 FOR ACCESS  INSERT INTO contract_header_svc_supp_ld (  contract_header_supp_id,    instance_id,    acctg_rule_id,  alt_as_of_date_time,    alt_batch_nbr,  ar_interface_flag,  as_of_date_time,    batch_nbr,  bill_sched_lvl_type_code,   contract_header_id,     created_by_name,    creation_date_time,     cust_trx_type_id,   est_revn_date,  est_revn_pct,   est_revn_period_used_code,  est_revn_used_duration,     est_revn_used_pct,  grace_duration,     grace_period_code,  hold_bill_flag,     invc_print_profile_flag,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   payment_type_code,  renewal_po_nbr,     renewal_po_required_flag,   renewal_po_used_flag,   rnw_est_revenue_duration,   rnw_est_revenue_pct,    rnw_est_revn_period_code,   rnw_grace_period_used_code,     rnw_grace_used_duration,    rnw_markup_pct,     rnw_markup_used_pct,    rnw_notification_to_id,     rnw_notification_to_name,   rnw_price_list_id,  rnw_price_list_used_id,     rnw_pricing_type_code,  rnw_pricing_used_type_code,     rnw_used_type_code,     service_po_nbr,     service_po_required_flag,   summary_trx_flag,   tax_exempt_status_code,     tax_exemption_id,   tran_code           ,renewal_approval_type_used     ,evn_thershhold_amt     ,ern_thershhold_amt ) SELECT    contract_header_supp_id,    instance_id,    acctg_rule_id,  as_of_date_time,    batch_nbr,  ar_interface_flag,  as_of_date_time,    batch_nbr,  bill_sched_lvl_type_code,   contract_header_id,     created_by_name,    creation_date_time,     cust_trx_type_id,   est_revn_date,  est_revn_pct,   est_revn_period_used_code,  est_revn_used_duration,     est_revn_used_pct,  grace_duration,     grace_period_code,  hold_bill_flag,     invc_print_profile_flag,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   payment_type_code,  renewal_po_nbr,     renewal_po_required_flag,   renewal_po_used_flag,   rnw_est_revenue_duration,   rnw_est_revenue_pct,    rnw_est_revn_period_code,   rnw_grace_period_used_code,     rnw_grace_used_duration,    rnw_markup_pct,     rnw_markup_used_pct,    rnw_notification_to_id,     rnw_notification_to_name,   rnw_price_list_id,  rnw_price_list_used_id,     rnw_pricing_type_code,  rnw_pricing_used_type_code,     rnw_used_type_code,     service_po_nbr,     service_po_required_flag,   summary_trx_flag,   tax_exempt_status_code,     tax_exemption_id,   tran_code               ,renewal_approval_type_used     ,evn_thershhold_amt     ,ern_thershhold_amt FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 WHERE ( contract_header_supp_id, instance_id ) NOT IN (SELECT  contract_header_supp_id, instance_id   FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2   WHERE current_record_ind = 'D'      )  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.contract_header_svc_supp_ld           SELECT
            contract_header_supp_id,
            instance_id,
            acctg_rule_id,
            as_of_date_time,
            batch_nbr,
            ar_interface_flag,
            as_of_date_time,
            batch_nbr,
            bill_sched_lvl_type_code,
            contract_header_id,
            created_by_name,
            creation_date_time,
            cust_trx_type_id,
            est_revn_date,
            est_revn_period_used_code,
            est_revn_pct,
            est_revn_used_duration,
            est_revn_used_pct,
            grace_duration,
            grace_period_code,
            hold_bill_flag,
            invc_print_profile_flag,
            last_update_date_time,
            last_update_login_name,
            last_updated_by_name,
            payment_type_code,
            renewal_po_nbr,
            renewal_po_required_flag,
            renewal_po_used_flag,
            rnw_est_revenue_duration,
            rnw_est_revenue_pct,
            rnw_est_revn_period_code,
            rnw_grace_period_used_code,
            rnw_grace_used_duration,
            rnw_markup_pct,
            rnw_markup_used_pct,
            rnw_notification_to_id,
            rnw_notification_to_name,
            rnw_price_list_id,
            rnw_price_list_used_id,
            rnw_pricing_type_code,
            rnw_pricing_used_type_code,
            rnw_used_type_code,
            service_po_nbr,
            service_po_required_flag,
            summary_trx_flag,
            tax_exempt_status_code,
            tax_exemption_id,
            tran_code,
            renewal_approval_type_used,
            evn_thershhold_amt,
            ern_thershhold_amt  
        FROM
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 
        LEFT OUTER JOIN
            (
                SELECT
                    DISTINCT contract_header_supp_id AS auto_c00,
                    instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2     
                WHERE
                    upper(trim(current_record_ind)) = upper(trim('D')) 
            ) AS autoAlias_105 
                ON (
                    upper(trim(contract_header_supp_id)) = upper(trim(autoAlias_105.AUTO_C00)) 
                    AND upper(trim(instance_id)) = upper(trim(autoAlias_105.AUTO_C01)) 
                ) 
        WHERE
            autoAlias_105.AUTO_C00 IS  NULL  
            AND autoAlias_105.AUTO_C01 IS  NULL;
