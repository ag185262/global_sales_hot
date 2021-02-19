from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractlinesvcsupp01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_svc_supp_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t1"""
        print("Job: 'd8_contract_line_svc_supp_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """        with qq1 as (
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
                        CONCAT (CAST (as_of_date_time AS CHAR(26)) , tran_code , batch_nbr) DESC) As RNK 
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_ml 
                    WHERE
                        upper(trim(tran_code)) <> upper(trim('D')) ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t1 
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
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_ml 
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
          AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_line_svc_supp_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_svc_supp_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2"""
        print("Job: 'd8_contract_line_svc_supp_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2           SELECT
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
                    'Y',
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
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_supp_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t1 
                    ) AS autoAlias_165 
                        ON (
                            upper(trim(contract_line_supp_id)) = upper(trim(autoAlias_165.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_165.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_line_svc_supp_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t3"""
        print("Job: 'd8_contract_line_svc_supp_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t3           SELECT
                    contract_line_supp_id,
                    instance_id,
                    acctg_rule_id,
                    allow_before_tax_disc_flag,
                    alt_contract_header_id,
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
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_supp_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t1 
                    ) AS autoAlias_167 
                        ON (
                            upper(trim(contract_line_supp_id)) = upper(trim(autoAlias_167.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_167.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_line_svc_supp_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_svc_supp_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t3           SELECT
                    contract_line_supp_id,
                    instance_id,
                    acctg_rule_id,
                    allow_before_tax_disc_flag,
                    alt_contract_header_id,
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
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t1 
                INNER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_supp_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2 
                    ) AS autoAlias_169 
                        ON (
                            upper(trim(contract_line_supp_id)) = upper(trim(autoAlias_169.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_169.AUTO_C01)) 
                        )"""
        print("Job: 'd8_contract_line_svc_supp_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_svc_supp_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2 SELECT
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.contract_line_supp_id AS contract_line_supp_id ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.acctg_rule_id AS acctg_rule_id ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.allow_before_tax_disc_flag AS allow_before_tax_disc_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.alt_contract_header_id AS alt_contract_header_id ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.bill_sched_lvl_type_code AS bill_sched_lvl_type_code ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.contract_line_id AS contract_line_id ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cover_lvl_ext_amt_ent AS cover_lvl_ext_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cover_lvl_list_prc_amt_ent AS cover_lvl_list_prc_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cover_lvl_qty AS cover_lvl_qty ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cover_lvl_uom_code AS cover_lvl_uom_code ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.coverage_type_code AS coverage_type_code ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.creation_date_time AS creation_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.credit_amt_ent AS credit_amt_ent ,
                    CASE 
        				WHEN autoAlias_171.auto_c00 IS not null and autoAlias_171.auto_c01 is not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cust_po_nbr AS cust_po_nbr ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.cust_po_nbr_rqrd_flag AS cust_po_nbr_rqrd_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.full_credit_flag AS full_credit_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.incident_severity_id AS incident_severity_id ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.invoice_print_flag AS invoice_print_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.offset_duration AS offset_duration ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.offset_period_code AS offset_period_code ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.override_amt_ent AS override_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.pricing_uom_code AS pricing_uom_code ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.process_defn_id AS process_defn_id ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.product_price_amt_ent AS product_price_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.product_upgrade_flag AS product_upgrade_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.react_active_flag AS react_active_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.service_price_amt_ent AS service_price_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.suppressed_credit_amt_ent AS suppressed_credit_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.sync_date_installed_flag AS sync_date_installed_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.tax_amt_ent AS tax_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.tax_exempt_status_code AS tax_exempt_status_code ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.tax_exemption_id AS tax_exemption_id ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.vat_tax_id AS vat_tax_id ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.tax_inclusive_flag AS tax_inclusive_flag ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_adj_prc_amt_ent AS top_lvl_adj_prc_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_operator_code AS top_lvl_operator_code ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_oprnd_prc_amt_ent AS top_lvl_oprnd_prc_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_pricing_uom_code AS top_lvl_pricing_uom_code ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_pricing_uom_qty AS top_lvl_pricing_uom_qty ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.top_lvl_qty AS top_lvl_qty ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.tran_code AS tran_code ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.transfer_option_code AS transfer_option_code ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.unbilled_amt_ent AS unbilled_amt_ent ,
                    AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.work_thru_flag AS work_thru_flag 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2 AS AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2 
                Left Outer Join
                    (
                        SELECT
                            contract_line_supp_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t3      
                        GROUP BY
                            contract_line_supp_id,
                            instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_171 
                        ON (
                            trim(upper(autoAlias_171.auto_c00)) = trim(upper(AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.contract_line_supp_id))
                            AND trim(upper(autoAlias_171.auto_c01)) = trim(upper(AAPPLLIIDD1EENNVV_contract_line_svc_supp_t2.instance_id))
                        )"""
        print("Job: 'd8_contract_line_svc_supp_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_svc_supp_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """   INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld 
                select
                Q1.* 
                From
                """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld Q1 
                Left Join
                    (
                        select
                            A.contract_line_supp_id,
                            A.instance_id 
                        From
                       """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t1  As A 
                            Left Join
                            (
                                SELECT
                                    contract_line_supp_id,
                                    instance_id 
                                FROM
                                   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2
                                WHERE
                                    current_record_ind = 'D' 
                            )
                            As B 
                            On trim(upper(A.contract_line_supp_id)) = trim(upper(b.contract_line_supp_id)) 
                            And trim(upper(A.instance_id)) = trim(upper(B.instance_id)) 
                        Where
                            B.contract_line_supp_id Is NULL 
                            And B.instance_id Is Null 
                    )
                    As Q2 
                    On trim(upper(Q1.contract_line_supp_id)) = trim(upper(Q2.contract_line_supp_id)) 
                    And trim(upper(Q1.instance_id)) = trim(upper(Q2.instance_id)) 
                Where
                Q2.contract_line_supp_id Is NULL 
                And Q2.instance_id Is Null"""
        print("Job: 'd8_contract_line_svc_supp_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld           SELECT
                    contract_line_supp_id,
                    instance_id,
                    acctg_rule_id,
                    allow_before_tax_disc_flag,
                    as_of_date_time,
                    batch_nbr,
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
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            DISTINCT contract_line_supp_id AS auto_c00,
                            instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2     
                        WHERE
                            upper(trim(current_record_ind)) = upper(trim('D')) 
                    ) AS autoAlias_175 
                        ON (
                            upper(trim(contract_line_supp_id)) = upper(trim(autoAlias_175.AUTO_C00)) 
                            AND upper(trim(instance_id)) = upper(trim(autoAlias_175.AUTO_C01)) 
                        ) 
                WHERE
                    autoAlias_175.AUTO_C00 IS  NULL  
                    AND autoAlias_175.AUTO_C01 IS  NULL"""
        print("Job: 'd8_contract_line_svc_supp_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_line_svc_supp_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t1"""
        print("Job: 'd8_contract_line_svc_supp_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """      with qq1 as (
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
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_ml 
                    WHERE
                        upper(trim(tran_code)) = upper(trim('D')) ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t1 
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
        FROM      """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_ml 
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
          AND upper(trim(EDW2.tran_code)) = upper(trim('D'))"""
        print("Job: 'd8_contract_line_svc_supp_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2"""
        print("Job: 'd8_contract_line_svc_supp_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        
        query = """WITH CTE AS 
        (
        SELECT t.contract_line_supp_id,t.instance_id,t.as_of_date_time,t.batch_nbr,t.tran_code,
         CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) as iCat
        from  """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld    AS l
        INNER JOIN 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t1    AS t   
        ON        trim(upper(l.contract_line_supp_id)) = trim(upper(t.contract_line_supp_id))  
                    AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) <  CONCAT (CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)
        )
        INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2           SELECT
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
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld    AS l   
                    INNER JOIN CTE AS t 
                    ON
                    trim(upper(l.contract_line_supp_id)) = trim(upper(t.contract_line_supp_id))  
                    AND trim(upper(l.instance_id)) = trim(upper(t.instance_id))   
                    WHERE  CONCAT (CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr)=t.iCat"""
        print("Job: 'd8_contract_line_svc_supp_06'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_svc_supp_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld
        select
           Q1.* 
        From
           """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld Q1 
           Left Join
              (
                 SELECT
                    contract_line_supp_id,
                    instance_id 
                 FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2
              )Q2
               On trim(upper(Q1.contract_line_supp_id)) = trim(upper(Q2.contract_line_supp_id)) 
              And trim(upper(Q1.instance_id)) = trim(upper(Q2.instance_id)) 
           Where
           Q2.contract_line_supp_id Is NULL 
           And Q2.instance_id Is Null """
        print("Job: 'd8_contract_line_svc_supp_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld           SELECT
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
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_line_svc_supp_t2"""
        print("Job: 'd8_contract_line_svc_supp_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractlinesvcsupp01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractlinesvcsupp01inssql").enableHiveSupport().getOrCreate()
    main()
