from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractheadersvcsupp01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_header_svc_supp_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t1"""
        print("Job: 'd8_contract_header_svc_supp_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   with qq1 as (
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
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_ml 
                    WHERE upper(trim(tran_code)) <> upper(trim('D')) 
                        ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t1 
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
        FROM   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_ml EDW2 
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
           AND upper(trim(EDW2.contract_header_supp_id)) =upper(trim(IBVL.contract_header_supp_id)) 
          AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_header_svc_supp_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_header_svc_supp_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2"""
        print("Job: 'd8_contract_header_svc_supp_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2           SELECT
                    contract_header_supp_id,
                    instance_id,
                    acctg_rule_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    ar_interface_flag,
                    as_of_date_time,
                    batch_nbr,
                    bill_sched_lvl_type_code,
                    contract_header_id,
                    created_by_name,
                    creation_date_time,
                    'Y',
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
                    ern_thershhold_amt  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_header_supp_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t1 
                    ) AS autoAlias_97 
                        ON (
                            upper(trim(contract_header_supp_id)) = upper(trim(autoAlias_97.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_97.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_header_svc_supp_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t3"""
        print("Job: 'd8_contract_header_svc_supp_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t3           SELECT
                    contract_header_supp_id,
                    instance_id,
                    acctg_rule_id,
                    ar_interface_flag,
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
                    null,
                    null,
                    null  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_header_supp_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t1 
                    ) AS autoAlias_99 
                        ON (
                            upper(trim(contract_header_supp_id)) = upper(trim(autoAlias_99.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_99.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_header_svc_supp_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_header_svc_supp_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t3           SELECT
                    contract_header_supp_id,
                    instance_id,
                    acctg_rule_id,
                    ar_interface_flag,
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
                    ern_thershhold_amt  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_header_supp_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2 
                    ) AS autoAlias_387 
                        ON (
                            upper(trim(contract_header_supp_id)) = upper(trim(autoAlias_387.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_387.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_header_svc_supp_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_header_svc_supp_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2 SELECT
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.contract_header_supp_id AS contract_header_supp_id ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.acctg_rule_id AS acctg_rule_id ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.ar_interface_flag AS ar_interface_flag ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.bill_sched_lvl_type_code AS bill_sched_lvl_type_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.contract_header_id AS contract_header_id ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_101.auto_c00 IS not null and autoAlias_101.auto_c01 IS not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.cust_trx_type_id AS cust_trx_type_id ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.est_revn_date AS est_revn_date ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.est_revn_pct AS est_revn_pct ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.est_revn_period_used_code AS est_revn_period_used_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.est_revn_used_duration AS est_revn_used_duration ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.est_revn_used_pct AS est_revn_used_pct ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.grace_duration AS grace_duration ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.grace_period_code AS grace_period_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.hold_bill_flag AS hold_bill_flag ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.invc_print_profile_flag AS invc_print_profile_flag ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.payment_type_code AS payment_type_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.renewal_po_nbr AS renewal_po_nbr ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.renewal_po_required_flag AS renewal_po_required_flag ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.renewal_po_used_flag AS renewal_po_used_flag ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_est_revenue_duration AS rnw_est_revenue_duration ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_est_revenue_pct AS rnw_est_revenue_pct ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_est_revn_period_code AS rnw_est_revn_period_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_grace_period_used_code AS rnw_grace_period_used_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_grace_used_duration AS rnw_grace_used_duration ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_markup_pct AS rnw_markup_pct ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_markup_used_pct AS rnw_markup_used_pct ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_notification_to_id AS rnw_notification_to_id ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_notification_to_name AS rnw_notification_to_name ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_price_list_id AS rnw_price_list_id ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_price_list_used_id AS rnw_price_list_used_id ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_pricing_type_code AS rnw_pricing_type_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_pricing_used_type_code AS rnw_pricing_used_type_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.rnw_used_type_code AS rnw_used_type_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.service_po_nbr AS service_po_nbr ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.service_po_required_flag AS service_po_required_flag ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.summary_trx_flag AS summary_trx_flag ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.tax_exempt_status_code AS tax_exempt_status_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.tax_exemption_id AS tax_exemption_id ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.tran_code AS tran_code ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.renewal_approval_type_used AS renewal_approval_type_used ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.evn_thershhold_amt AS evn_thershhold_amt ,
                    AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.ern_thershhold_amt AS ern_thershhold_amt 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2 AS AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2 
                Left Outer Join
                    (
                        SELECT
                            contract_header_supp_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t3      
                        GROUP BY
                            contract_header_supp_id,
                            instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_101 
                        ON (
                            upper(trim( autoAlias_101.auto_c00)) = upper(trim( AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.contract_header_supp_id)) 
                            AND upper(trim( autoAlias_101.auto_c01)) = upper(trim( AAPPLLIIDD1EENNVV_contr_header_svc_supp_t2.instance_id))
                        )"""
        print("Job: 'd8_contract_header_svc_supp_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_header_svc_supp_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE
                    TABLE """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld
                select
                Q1.* 
                From
                """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld Q1 
                Left Join
                    (
                        select
                            A.contract_header_supp_id,
                            A.instance_id 
                        From
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t1 As A 
                            Left Join
                            (
                                SELECT
                                    contract_header_supp_id,
                                    instance_id 
                                FROM
                                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2
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
                And Q2.instance_id Is Null """
        print("Job: 'd8_contract_header_svc_supp_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld           SELECT
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
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT contract_header_supp_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_105 
                        ON (
                            upper(trim(contract_header_supp_id)) = upper(trim(autoAlias_105.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_105.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_105.AUTO_C00 IS  NULL  
                    AND autoAlias_105.AUTO_C01 IS  NULL"""
        print("Job: 'd8_contract_header_svc_supp_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_header_svc_supp_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t1"""
        print("Job: 'd8_contract_header_svc_supp_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """with qq1 as (
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
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_ml 
                    WHERE upper(trim(tran_code)) = upper(trim('D')) 
                        ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t1 
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
        FROM   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_ml
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
          AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_header_svc_supp_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2"""
        print("Job: 'd8_contract_header_svc_supp_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """WITH CTE AS
        (SELECT t.contract_header_supp_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code, CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) AS iCat
        From """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld    AS l
        INNER JOIN
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t1    AS t 
        ON upper(trim(t.contract_header_supp_id))=upper(trim(l.contract_header_supp_id))
        AND upper(trim(t.instance_id))=upper(trim(l.instance_id))
        WHERE CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
        )
        INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2           SELECT
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
                    """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld    AS l
                    INNER JOIN CTE t
                    ON upper(trim(t.contract_header_supp_id))=upper(trim(l.contract_header_supp_id))
                    AND upper(trim(t.instance_id))=upper(trim(l.instance_id))
                    WHERE CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat"""
        print("Job: 'd8_contract_header_svc_supp_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_header_svc_supp_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        INSERT OVERWRITE
                    TABLE  """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld
                SELECT
                Q1.* 
                FROM
                """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld AS Q1
                Left Join
                    (
                        SELECT
                            contract_header_supp_id,
                            instance_id 
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2  
                    )
                    As Q2 
                    On upper(trim( Q1.contract_header_supp_id ))= upper(trim( Q2.contract_header_supp_id ))
                    And upper(trim( Q1.instance_id )) = upper(trim( Q2.instance_id ))
                Where
                Q2.contract_header_supp_id Is NULL 
                And Q2.instance_id Is Null """
        print("Job: 'd8_contract_header_svc_supp_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld           SELECT
                    contract_header_supp_id,
                    instance_id,
                    acctg_rule_id,
                    alt_as_of_date_time,
                    alt_batch_nbr,
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
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_header_svc_supp_t2"""
        print("Job: 'd8_contract_header_svc_supp_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractheadersvcsupp01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractheadersvcsupp01inssql").enableHiveSupport().getOrCreate()
    main()
