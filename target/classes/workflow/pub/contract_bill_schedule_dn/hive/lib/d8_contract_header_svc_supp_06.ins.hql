----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:09 
--Script Name: d8_contract_header_svc_supp_06.ins.sql
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
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 ALL 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1;
--(Nikhil:Replaced the Max function with Row_number function and also removed Inner Join as it is not required)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 (   contract_header_supp_id,    instance_id,    acctg_rule_id,  ar_interface_flag,  as_of_date_time,    batch_nbr,  bill_sched_lvl_type_code,   contract_header_id,     created_by_name,    creation_date_time,     cust_trx_type_id,   est_revn_date,  est_revn_pct,   est_revn_period_used_code,  est_revn_used_duration,     est_revn_used_pct,  grace_duration,     grace_period_code,  hold_bill_flag,     invc_print_profile_flag,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   payment_type_code,  renewal_po_nbr,     renewal_po_required_flag,   renewal_po_used_flag,   rnw_est_revenue_duration,   rnw_est_revenue_pct,    rnw_est_revn_period_code,   rnw_grace_period_used_code,     rnw_grace_used_duration,    rnw_markup_pct,     rnw_markup_used_pct,    rnw_notification_to_id,     rnw_notification_to_name,   rnw_price_list_id,  rnw_price_list_used_id,     rnw_pricing_type_code,  rnw_pricing_used_type_code,     rnw_used_type_code,     service_po_nbr,     service_po_required_flag,   summary_trx_flag,   tax_exempt_status_code,     tax_exemption_id,   tran_code   ,renewal_approval_type_used     ,evn_thershhold_amt     ,ern_thershhold_amt ) SELECT    contract_header_supp_id,    instance_id,    acctg_rule_id,  ar_interface_flag,  as_of_date_time,    batch_nbr,  bill_sched_lvl_type_code,   contract_header_id,     created_by_name,    creation_date_time,     cust_trx_type_id,   est_revn_date,  est_revn_pct,   est_revn_period_used_code,  est_revn_used_duration,     est_revn_used_pct,  grace_duration,     grace_period_code,  hold_bill_flag,     invc_print_profile_flag,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   payment_type_code,  renewal_po_nbr,     renewal_po_required_flag,   renewal_po_used_flag,   rnw_est_revenue_duration,   rnw_est_revenue_pct,    rnw_est_revn_period_code,   rnw_grace_period_used_code,     rnw_grace_used_duration,    rnw_markup_pct,     rnw_markup_used_pct,    rnw_notification_to_id,     rnw_notification_to_name,   rnw_price_list_id,  rnw_price_list_used_id,     rnw_pricing_type_code,  rnw_pricing_used_type_code,     rnw_used_type_code,     service_po_nbr,     service_po_required_flag,   summary_trx_flag,   tax_exempt_status_code,     tax_exemption_id,   tran_code       ,renewal_approval_type_used     ,evn_thershhold_amt     ,ern_thershhold_amt FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml WHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,     contract_header_supp_id,    instance_id ) IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),    contract_header_supp_id,    instance_id     FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml     WHERE tran_code = 'D'     GROUP BY 2,3 ) AND tran_code = 'D'  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1           SELECT
            -- contract_header_supp_id,
            -- instance_id,
            -- acctg_rule_id,
            -- ar_interface_flag,
            -- as_of_date_time,
            -- batch_nbr,
            -- bill_sched_lvl_type_code,
            -- contract_header_id,
            -- created_by_name,
            -- creation_date_time,
            -- cust_trx_type_id,
            -- est_revn_date,
            -- est_revn_pct,
            -- est_revn_period_used_code,
            -- est_revn_used_duration,
            -- est_revn_used_pct,
            -- grace_duration,
            -- grace_period_code,
            -- hold_bill_flag,
            -- invc_print_profile_flag,
            -- last_update_date_time,
            -- last_update_login_name,
            -- last_updated_by_name,
            -- payment_type_code,
            -- renewal_po_nbr,
            -- renewal_po_required_flag,
            -- renewal_po_used_flag,
            -- rnw_est_revenue_duration,
            -- rnw_est_revenue_pct,
            -- rnw_est_revn_period_code,
            -- rnw_grace_period_used_code,
            -- rnw_grace_used_duration,
            -- rnw_markup_pct,
            -- rnw_markup_used_pct,
            -- rnw_notification_to_id,
            -- rnw_notification_to_name,
            -- rnw_price_list_id,
            -- rnw_price_list_used_id,
            -- rnw_pricing_type_code,
            -- rnw_pricing_used_type_code,
            -- rnw_used_type_code,
            -- service_po_nbr,
            -- service_po_required_flag,
            -- summary_trx_flag,
            -- tax_exempt_status_code,
            -- tax_exemption_id,
            -- tran_code,
            -- renewal_approval_type_used,
            -- evn_thershhold_amt,
            -- ern_thershhold_amt  
        -- FROM
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml 
        -- INNER JOIN
            -- (
                -- SELECT
                    -- MAX(  CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                    -- batch_nbr) ) AS auto_c00,
                    -- contract_header_supp_id AS auto_c01,
                    -- instance_id AS auto_c02  
                -- FROM
                    -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml     
                -- WHERE
                    -- upper(trim(tran_code)) = upper(trim('D'))  
                -- GROUP BY
                    -- contract_header_supp_id ,
                    -- instance_id 
            -- ) AS autoAlias_107 
                -- ON (
                    -- CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
                -- batch_nbr) = autoAlias_107.AUTO_C00 
                -- AND upper(trim(contract_header_supp_id)) = upper(trim(autoAlias_107.AUTO_C01)) 
                -- AND upper(trim(instance_id)) = upper(trim(autoAlias_107.AUTO_C02)) ) 
            -- WHERE
                -- upper(trim(tran_code)) = upper(trim('D'));

--Translated Query:(Corrected Manuallly)                
with qq1 as (
SELECT 
            contract_header_supp_id,
            instance_id,
            acctg_rule_id,
            ar_interface_flag,
            as_of_date_time,
            batch_nbr,
            bill_sched_lvl_type_code,
            contract_header_id,
            created_by_name,
            creation_date_time,
            cust_trx_type_id,
            est_revn_date,
            est_revn_pct,
            est_revn_period_used_code,
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
            ern_thershhold_amt,
            ROW_NUMBER() OVER( PARTITION BY contract_header_supp_id,instance_id ORDER BY CONCAT (CAST (as_of_date_time AS CHAR(26)) ,
            batch_nbr) DESC)AS RNK
            FROM 
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml 
            WHERE upper(trim(tran_code)) = upper(trim('D')) 
                ) 
INSERT INTO ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 
SELECT
            EDW2.contract_header_supp_id,
    EDW2.instance_id,
    EDW2.acctg_rule_id,
    EDW2.ar_interface_flag,
    EDW2.as_of_date_time,
    EDW2.batch_nbr,
    EDW2.bill_sched_lvl_type_code,
    EDW2.contract_header_id,
    EDW2.created_by_name,
    EDW2.creation_date_time,
    EDW2.cust_trx_type_id,
    EDW2.est_revn_date,
    EDW2.est_revn_pct,
    EDW2.est_revn_period_used_code,
    EDW2.est_revn_used_duration,
    EDW2.est_revn_used_pct,
    EDW2.grace_duration,
    EDW2.grace_period_code,
    EDW2.hold_bill_flag,
    EDW2.invc_print_profile_flag,
    EDW2.last_update_date_time,
    EDW2.last_update_login_name,
    EDW2.last_updated_by_name,
    EDW2.payment_type_code,
    EDW2.renewal_po_nbr,
    EDW2.renewal_po_required_flag,
    EDW2.renewal_po_used_flag,
    EDW2.rnw_est_revenue_duration,
    EDW2.rnw_est_revenue_pct,
    EDW2.rnw_est_revn_period_code,
    EDW2.rnw_grace_period_used_code,
    EDW2.rnw_grace_used_duration,
    EDW2.rnw_markup_pct,
    EDW2.rnw_markup_used_pct,
    EDW2.rnw_notification_to_id,
    EDW2.rnw_notification_to_name,
    EDW2.rnw_price_list_id,
    EDW2.rnw_price_list_used_id,
    EDW2.rnw_pricing_type_code,
    EDW2.rnw_pricing_used_type_code,
    EDW2.rnw_used_type_code,
    EDW2.service_po_nbr,
    EDW2.service_po_required_flag,
    EDW2.summary_trx_flag,
    EDW2.tax_exempt_status_code,
    EDW2.tax_exemption_id,
    EDW2.tran_code,
    EDW2.renewal_approval_type_used,
    EDW2.evn_thershhold_amt,
    EDW2.ern_thershhold_amt 
FROM   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_ml
 EDW2 
  INNER JOIN (
    SELECT 
       contract_header_supp_id,
    instance_id,
    acctg_rule_id,
    ar_interface_flag,
    as_of_date_time,
    batch_nbr,
    bill_sched_lvl_type_code,
    contract_header_id,
    created_by_name,
    creation_date_time,
    cust_trx_type_id,
    est_revn_date,
    est_revn_pct,
    est_revn_period_used_code,
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
    ern_thershhold_amt, 
            RNK 
    FROM 
      qq1 
    where 
      RNK = 1
  ) IBVL 
  on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
   AND upper(trim(EDW2.contract_header_supp_id)) =upper(trim(IBVL.contract_header_supp_id ))
  AND upper(trim(EDW2.tran_code)) = upper(trim('D'));
        
--Original Query:
  --DELETE FROM ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2 

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2;

--(Nikhil: Replaced the inequality Condition with CTE block)
--Original Query:
  --LOCK TABLE  ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 FOR ACCESS  INSERT INTO ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2 (   contract_header_supp_id,    instance_id,    acctg_rule_id,  alt_as_of_date_time,    alt_batch_nbr,  ar_interface_flag,  as_of_date_time,    batch_nbr,  bill_sched_lvl_type_code,   contract_header_id,     created_by_name,    creation_date_time,     cust_trx_type_id,   est_revn_date,  est_revn_pct,   est_revn_period_used_code,  est_revn_used_duration,     est_revn_used_pct,  grace_duration,     grace_period_code,  hold_bill_flag,     invc_print_profile_flag,    last_update_date_time,  last_update_login_name,     last_updated_by_name,   payment_type_code,  renewal_po_nbr,     renewal_po_required_flag,   renewal_po_used_flag,   rnw_est_revenue_duration,   rnw_est_revenue_pct,    rnw_est_revn_period_code,   rnw_grace_period_used_code,     rnw_grace_used_duration,    rnw_markup_pct,     rnw_markup_used_pct,    rnw_notification_to_id,     rnw_notification_to_name,   rnw_price_list_id,  rnw_price_list_used_id,     rnw_pricing_type_code,  rnw_pricing_used_type_code,     rnw_used_type_code,     service_po_nbr,     service_po_required_flag,   summary_trx_flag,   tax_exempt_status_code,     tax_exemption_id,   tran_code        ) SELECT   l.contract_header_supp_id,  l.instance_id,  l.acctg_rule_id,    l.as_of_date_time,  l.batch_nbr,    l.ar_interface_flag,    t.as_of_date_time,  t.batch_nbr,    l.bill_sched_lvl_type_code,     l.contract_header_id,   l.created_by_name,  l.creation_date_time,   l.cust_trx_type_id,     l.est_revn_date,    l.est_revn_pct,     l.est_revn_period_used_code,    l.est_revn_used_duration,   l.est_revn_used_pct,    l.grace_duration,   l.grace_period_code,    l.hold_bill_flag,   l.invc_print_profile_flag,  l.last_update_date_time,    l.last_update_login_name,   l.last_updated_by_name,     l.payment_type_code,    l.renewal_po_nbr,   l.renewal_po_required_flag,     l.renewal_po_used_flag,     l.rnw_est_revenue_duration,     l.rnw_est_revenue_pct,  l.rnw_est_revn_period_code,     l.rnw_grace_period_used_code,   l.rnw_grace_used_duration,  l.rnw_markup_pct,   l.rnw_markup_used_pct,  l.rnw_notification_to_id,   l.rnw_notification_to_name,     l.rnw_price_list_id,    l.rnw_price_list_used_id,   l.rnw_pricing_type_code,    l.rnw_pricing_used_type_code,   l.rnw_used_type_code,   l.service_po_nbr,   l.service_po_required_flag,     l.summary_trx_flag,     l.tax_exempt_status_code,   l.tax_exemption_id,     t.tran_code FROM ${EEDDWWDDBB1}.contract_header_svc_supp_ld l,       ${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1 t WHERE  l.contract_header_supp_id = t.contract_header_supp_id AND    l.instance_id = t.instance_id  AND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <      CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr  

--Translated Query: STATUS SUCCESS

    -- INSERT 
    -- INTO
        -- TABLE
        -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2           SELECT
            -- l.contract_header_supp_id,
            -- l.instance_id,
            -- l.acctg_rule_id,
            -- l.as_of_date_time,
            -- l.batch_nbr,
            -- l.ar_interface_flag,
            -- t.as_of_date_time,
            -- t.batch_nbr,
            -- l.bill_sched_lvl_type_code,
            -- l.contract_header_id,
            -- l.created_by_name,
            -- l.creation_date_time,
            -- null,
            -- l.cust_trx_type_id,
            -- l.est_revn_date,
            -- l.est_revn_pct,
            -- l.est_revn_period_used_code,
            -- l.est_revn_used_duration,
            -- l.est_revn_used_pct,
            -- l.grace_duration,
            -- l.grace_period_code,
            -- l.hold_bill_flag,
            -- l.invc_print_profile_flag,
            -- l.last_update_date_time,
            -- l.last_update_login_name,
            -- l.last_updated_by_name,
            -- l.payment_type_code,
            -- l.renewal_po_nbr,
            -- l.renewal_po_required_flag,
            -- l.renewal_po_used_flag,
            -- l.rnw_est_revenue_duration,
            -- l.rnw_est_revenue_pct,
            -- l.rnw_est_revn_period_code,
            -- l.rnw_grace_period_used_code,
            -- l.rnw_grace_used_duration,
            -- l.rnw_markup_pct,
            -- l.rnw_markup_used_pct,
            -- l.rnw_notification_to_id,
            -- l.rnw_notification_to_name,
            -- l.rnw_price_list_id,
            -- l.rnw_price_list_used_id,
            -- l.rnw_pricing_type_code,
            -- l.rnw_pricing_used_type_code,
            -- l.rnw_used_type_code,
            -- l.service_po_nbr,
            -- l.service_po_required_flag,
            -- l.summary_trx_flag,
            -- l.tax_exempt_status_code,
            -- l.tax_exemption_id,
            -- t.tran_code,
            -- null,
            -- null,
            -- null  
        -- FROM
            -- ${EEDDWWDDBB1}.contract_header_svc_supp_ld    AS l   ,
            -- ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1    AS t   
        -- WHERE
            -- l.contract_header_supp_id = t.contract_header_supp_id  
            -- AND l.instance_id = t.instance_id   
            -- AND  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);

 --Translated Query:(Corrected Manually)
WITH CTE AS
(SELECT t.contract_header_supp_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code, CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) AS iCat
From ${EEDDWWDDBB1}.contract_header_svc_supp_ld    AS l
INNER JOIN
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t1    AS t 
ON upper(trim(t.contract_header_supp_id))=upper(trim(l.contract_header_supp_id))
AND upper(trim(t.instance_id))=upper(trim(l.instance_id))
WHERE CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
)

INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_contr_header_svc_supp_t2           SELECT
            l.contract_header_supp_id,
            l.instance_id,
            l.acctg_rule_id,
            l.as_of_date_time,
            l.batch_nbr,
            l.ar_interface_flag,
            t.as_of_date_time,
            t.batch_nbr,
            l.bill_sched_lvl_type_code,
            l.contract_header_id,
            l.created_by_name,
            l.creation_date_time,
            null,
            l.cust_trx_type_id,
            l.est_revn_date,
            l.est_revn_pct,
            l.est_revn_period_used_code,
            l.est_revn_used_duration,
            l.est_revn_used_pct,
            l.grace_duration,
            l.grace_period_code,
            l.hold_bill_flag,
            l.invc_print_profile_flag,
            l.last_update_date_time,
            l.last_update_login_name,
            l.last_updated_by_name,
            l.payment_type_code,
            l.renewal_po_nbr,
            l.renewal_po_required_flag,
            l.renewal_po_used_flag,
            l.rnw_est_revenue_duration,
            l.rnw_est_revenue_pct,
            l.rnw_est_revn_period_code,
            l.rnw_grace_period_used_code,
            l.rnw_grace_used_duration,
            l.rnw_markup_pct,
            l.rnw_markup_used_pct,
            l.rnw_notification_to_id,
            l.rnw_notification_to_name,
            l.rnw_price_list_id,
            l.rnw_price_list_used_id,
            l.rnw_pricing_type_code,
            l.rnw_pricing_used_type_code,
            l.rnw_used_type_code,
            l.service_po_nbr,
            l.service_po_required_flag,
            l.summary_trx_flag,
            l.tax_exempt_status_code,
            l.tax_exemption_id,
            t.tran_code,
            null,
            null,
            null  
        FROM
            ${EEDDWWDDBB1}.contract_header_svc_supp_ld    AS l
            INNER JOIN CTE t
            ON upper(trim(t.contract_header_supp_id))=upper(trim(l.contract_header_supp_id))
            AND upper(trim(t.instance_id))=upper(trim(l.instance_id))
            WHERE CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat;
