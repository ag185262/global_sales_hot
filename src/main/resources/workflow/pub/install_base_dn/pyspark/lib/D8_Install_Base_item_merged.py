from pyspark.sql import SparkSession
from datetime import datetime 
import sys 
import db_params

class D8InstallBaseitemmergedpy:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld          SELECT
                    A.*  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_item A CLUSTER BY A.instance_id,A.item_instance_id """
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    use """ + db_params.TTMMPPDDBB1 + """"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t1"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t1           SELECT
                    instance_id,
                    item_instance_id,
                    accounting_class_code,
                    active_end_date_time,
                    active_start_date_time,
                    actual_return_date_time,
                    as_of_date_time,
                    batch_nbr,
                    complete_flag,
                    contract_reference_nbr,
                    created_by_name,
                    creation_complete_flag,
                    creation_date_time,
                    customer_view_flag,
                    ecf_nbr,
                    external_reference_nbr,
                    in_transit_order_line_id,
                    install_date_time,
                    install_location_id,
                    install_location_type_code,
                    inventory_item_id,
                    inventory_locator_id,
                    inventory_master_org_id,
                    inventory_org_id,
                    inventory_revision_code,
                    invtry_subinventory_name,
                    item_instance_condition_id,
                    item_instance_nbr,
                    item_instance_status_id,
                    item_instance_type_code,
                    item_instance_usage_code,
                    item_qty,
                    last_oe_agreement_id,
                    last_oe_po_nbr,
                    last_order_line_id,
                    last_pa_project_id,
                    last_pa_task_id,
                    last_po_line_id,
                    last_rma_line_id,
                    last_tran_line_detail_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    last_valid_invtry_org_id,
                    last_wip_entity_id,
                    location_id,
                    location_type_code,
                    lot_nbr,
                    manually_created_flag,
                    merchant_view_flag,
                    migrated_flag,
                    mnfctr_serial_nbr_flag,
                    owner_cust_acct_id,
                    owner_party_id,
                    owner_party_table_name,
                    pa_project_id,
                    pa_project_task_id,
                    prev_install_at_location_id,
                    product_reference_nbr,
                    purchase_order_line_id,
                    return_by_date_time,
                    sellable_flag,
                    serial_nbr,
                    system_id,
                    tran_code,
                    uom_code,
                    warranty_code,
                    warranty_eff_date,
                    warranty_exp_date,
                    wip_entity_id,
                    wot_quote_nbr,
                    sot_order_nbr,
                    prev_serial_nbr,
                    future_move_concat_value,
                    om_invoice_date,
                    previous_prod_id,
                    previous_prod_id_formatted,
                    security_risk_code,
                    prev_cust_nbr,
                    prev_site_nbr,
                    service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date,
                cit_vendor_code,        
                mdm_product_identifier, 
                mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_ml 
                    WHERE
                        upper(trim(tran_code)) <> 'D' CLUSTER BY INSTANCE_ID,ITEM_INSTANCE_ID"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2           SELECT
                    instance_id,
                    item_instance_id,
                    accounting_class_code,
                    active_end_date_time,
                    active_start_date_time,
                    actual_return_date_time,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    complete_flag,
                    contract_reference_nbr,
                    created_by_name,
                    creation_complete_flag,
                    creation_date_time,
                    'Y',
                    customer_view_flag,
                    ecf_nbr,
                    external_reference_nbr,
                    in_transit_order_line_id,
                    install_date_time,
                    install_location_id,
                    install_location_type_code,
                    inventory_item_id,
                    inventory_locator_id,
                    inventory_master_org_id,
                    inventory_org_id,
                    inventory_revision_code,
                    invtry_subinventory_name,
                    item_instance_condition_id,
                    item_instance_nbr,
                    item_instance_status_id,
                    item_instance_type_code,
                    item_instance_usage_code,
                    item_qty,
                    last_oe_agreement_id,
                    last_oe_po_nbr,
                    last_order_line_id,
                    last_pa_project_id,
                    last_pa_task_id,
                    last_po_line_id,
                    last_rma_line_id,
                    last_tran_line_detail_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    last_valid_invtry_org_id,
                    last_wip_entity_id,
                    location_id,
                    location_type_code,
                    lot_nbr,
                    manually_created_flag,
                    merchant_view_flag,
                    migrated_flag,
                    mnfctr_serial_nbr_flag,
                    owner_cust_acct_id,
                    owner_party_id,
                    owner_party_table_name,
                    pa_project_id,
                    pa_project_task_id,
                    prev_install_at_location_id,
                    product_reference_nbr,
                    purchase_order_line_id,
                    return_by_date_time,
                    sellable_flag,
                    serial_nbr,
                    system_id,
                    tran_code,
                    uom_code,
                    warranty_code,
                    warranty_eff_date,
                    warranty_exp_date,
                    wip_entity_id,
                    wot_quote_nbr,
                    sot_order_nbr,
                    prev_serial_nbr,
                    future_move_concat_value,
                    om_invoice_date,
                    previous_prod_id,
                    previous_prod_id_formatted,
                    security_risk_code,
                    prev_cust_nbr,
                    prev_site_nbr,
                    service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date,
                    cit_vendor_code,        
                mdm_product_identifier, 
                mdm_solution_identifier  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld 
                INNER JOIN
                    (
                        SELECT
                            instance_id AS auto_c00,
                            item_instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t1 
                    ) AS autoAlias_54 
                        ON (
                            instance_id = autoAlias_54.AUTO_C00 
                            AND item_instance_id = autoAlias_54.AUTO_C01 
                        )"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t3"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t3           SELECT
                    instance_id,
                    item_instance_id,
                    accounting_class_code,
                    active_end_date_time,
                    active_start_date_time,
                    actual_return_date_time,
                    complete_flag,
                    contract_reference_nbr,
                    created_by_name,
                    creation_complete_flag,
                    creation_date_time,
                    customer_view_flag,
                    ecf_nbr,
                    external_reference_nbr,
                    in_transit_order_line_id,
                    install_date_time,
                    install_location_id,
                    install_location_type_code,
                    inventory_item_id,
                    inventory_locator_id,
                    inventory_master_org_id,
                    inventory_org_id,
                    inventory_revision_code,
                    invtry_subinventory_name,
                    item_instance_condition_id,
                    item_instance_nbr,
                    item_instance_status_id,
                    item_instance_type_code,
                    item_instance_usage_code,
                    item_qty,
                    last_oe_agreement_id,
                    last_oe_po_nbr,
                    last_order_line_id,
                    last_pa_project_id,
                    last_pa_task_id,
                    last_po_line_id,
                    last_rma_line_id,
                    last_tran_line_detail_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    last_valid_invtry_org_id,
                    last_wip_entity_id,
                    location_id,
                    location_type_code,
                    lot_nbr,
                    manually_created_flag,
                    merchant_view_flag,
                    migrated_flag,
                    mnfctr_serial_nbr_flag,
                    owner_cust_acct_id,
                    owner_party_id,
                    owner_party_table_name,
                    pa_project_id,
                    pa_project_task_id,
                    prev_install_at_location_id,
                    product_reference_nbr,
                    purchase_order_line_id,
                    return_by_date_time,
                    sellable_flag,
                    serial_nbr,
                    system_id,
                    tran_code,
                    uom_code,
                    warranty_code,
                    warranty_eff_date,
                    warranty_exp_date,
                    wip_entity_id,
                    wot_quote_nbr,
                    sot_order_nbr,
                    prev_serial_nbr,
                    future_move_concat_value,
                    om_invoice_date,
                    previous_prod_id,
                    previous_prod_id_formatted,
                    security_risk_code,
                    prev_site_nbr,
                    prev_cust_nbr,
                    service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date,
                    cit_vendor_code,        
                mdm_product_identifier, 
                mdm_solution_identifier  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2 CLUSTER BY INSTANCE_ID,ITEM_INSTANCE_ID"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t3           SELECT
                    instance_id,
                    item_instance_id,
                    accounting_class_code,
                    active_end_date_time,
                    active_start_date_time,
                    actual_return_date_time,
                    complete_flag,
                    contract_reference_nbr,
                    created_by_name,
                    creation_complete_flag,
                    creation_date_time,
                    customer_view_flag,
                    ecf_nbr,
                    external_reference_nbr,
                    in_transit_order_line_id,
                    install_date_time,
                    install_location_id,
                    install_location_type_code,
                    inventory_item_id,
                    inventory_locator_id,
                    inventory_master_org_id,
                    inventory_org_id,
                    inventory_revision_code,
                    invtry_subinventory_name,
                    item_instance_condition_id,
                    item_instance_nbr,
                    item_instance_status_id,
                    item_instance_type_code,
                    item_instance_usage_code,
                    item_qty,
                    last_oe_agreement_id,
                    last_oe_po_nbr,
                    last_order_line_id,
                    last_pa_project_id,
                    last_pa_task_id,
                    last_po_line_id,
                    last_rma_line_id,
                    last_tran_line_detail_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    last_valid_invtry_org_id,
                    last_wip_entity_id,
                    location_id,
                    location_type_code,
                    lot_nbr,
                    manually_created_flag,
                    merchant_view_flag,
                    migrated_flag,
                    mnfctr_serial_nbr_flag,
                    owner_cust_acct_id,
                    owner_party_id,
                    owner_party_table_name,
                    pa_project_id,
                    pa_project_task_id,
                    prev_install_at_location_id,
                    product_reference_nbr,
                    purchase_order_line_id,
                    return_by_date_time,
                    sellable_flag,
                    serial_nbr,
                    system_id,
                    tran_code,
                    uom_code,
                    warranty_code,
                    warranty_eff_date,
                    warranty_exp_date,
                    wip_entity_id,
                    wot_quote_nbr,
                    sot_order_nbr,
                    prev_serial_nbr,
                    future_move_concat_value,
                    om_invoice_date,
                    previous_prod_id,
                    previous_prod_id_formatted,
                    security_risk_code,
                    prev_site_nbr,
                    prev_cust_nbr,
                    service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date,
                cit_vendor_code,        
                mdm_product_identifier, 
                mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t1 
                INNER JOIN
                    (
                        SELECT
                            instance_id AS auto_c00,
                            item_instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2 
                    ) AS autoAlias_58 
                        ON (
                            instance_id = autoAlias_58.AUTO_C00
                            AND item_instance_id = autoAlias_58.AUTO_C01 
                        )"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
           INSERT overwrite table """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t3    SELECT distinct * from """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t3"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            overwrite
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2
                  SELECT
                    AAPPLLIIDD1EENNVV_install_base_item_t2.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.item_instance_id AS item_instance_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.accounting_class_code AS accounting_class_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.active_end_date_time AS active_end_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.active_start_date_time AS active_start_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.actual_return_date_time AS actual_return_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.alt_as_of_date_time AS alt_as_of_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.alt_batch_nbr AS alt_batch_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.as_of_date_time AS as_of_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.batch_nbr AS batch_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.complete_flag AS complete_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.contract_reference_nbr AS contract_reference_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.created_by_name AS created_by_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.creation_complete_flag AS creation_complete_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.creation_date_time AS creation_date_time ,
                    CASE 
                        WHEN autoAlias_60.auto_c00 IS not null THEN 'D' 
                        ELSE AAPPLLIIDD1EENNVV_install_base_item_t2.current_record_ind 
                    END AS current_record_ind ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.customer_view_flag AS customer_view_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.ecf_nbr AS ecf_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.external_reference_nbr AS external_reference_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.in_transit_order_line_id AS in_transit_order_line_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.install_date_time AS install_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.install_location_id AS install_location_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.install_location_type_code AS install_location_type_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.inventory_item_id AS inventory_item_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.inventory_locator_id AS inventory_locator_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.inventory_master_org_id AS inventory_master_org_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.inventory_org_id AS inventory_org_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.inventory_revision_code AS inventory_revision_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.invtry_subinventory_name AS invtry_subinventory_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.item_instance_condition_id AS item_instance_condition_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.item_instance_nbr AS item_instance_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.item_instance_status_id AS item_instance_status_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.item_instance_type_code AS item_instance_type_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.item_instance_usage_code AS item_instance_usage_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.item_qty AS item_qty ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_oe_agreement_id AS last_oe_agreement_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_oe_po_nbr AS last_oe_po_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_order_line_id AS last_order_line_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_pa_project_id AS last_pa_project_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_pa_task_id AS last_pa_task_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_po_line_id AS last_po_line_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_rma_line_id AS last_rma_line_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_tran_line_detail_id AS last_tran_line_detail_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_update_date_time AS last_update_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_update_login_name AS last_update_login_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_updated_by_name AS last_updated_by_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_valid_invtry_org_id AS last_valid_invtry_org_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.last_wip_entity_id AS last_wip_entity_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.location_id AS location_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.location_type_code AS location_type_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.lot_nbr AS lot_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.manually_created_flag AS manually_created_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.merchant_view_flag AS merchant_view_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.migrated_flag AS migrated_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.mnfctr_serial_nbr_flag AS mnfctr_serial_nbr_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.owner_cust_acct_id AS owner_cust_acct_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.owner_party_id AS owner_party_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.owner_party_table_name AS owner_party_table_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.pa_project_id AS pa_project_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.pa_project_task_id AS pa_project_task_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.prev_install_at_location_id AS prev_install_at_location_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.product_reference_nbr AS product_reference_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.purchase_order_line_id AS purchase_order_line_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.return_by_date_time AS return_by_date_time ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.sellable_flag AS sellable_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.serial_nbr AS serial_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.system_id AS system_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.tran_code AS tran_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.uom_code AS uom_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.warranty_code AS warranty_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.warranty_eff_date AS warranty_eff_date ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.warranty_exp_date AS warranty_exp_date ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.wip_entity_id AS wip_entity_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.wot_quote_nbr AS wot_quote_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.sot_order_nbr AS sot_order_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.prev_serial_nbr AS prev_serial_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.future_move_concat_value AS future_move_concat_value ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.om_invoice_date AS om_invoice_date ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.previous_prod_id AS previous_prod_id ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.previous_prod_id_formatted AS previous_prod_id_formatted ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.security_risk_code AS security_risk_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.prev_cust_nbr AS prev_cust_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.prev_site_nbr AS prev_site_nbr ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.service_tier_name AS service_tier_name ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.esd_flag AS esd_flag ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.media_type AS media_type ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.license_model AS license_model ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.license_start_date AS license_start_date ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.license_end_date AS license_end_date ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.cit_vendor_code AS cit_vendor_code ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.mdm_product_identifier AS mdm_product_identifier ,
                    AAPPLLIIDD1EENNVV_install_base_item_t2.mdm_solution_identifier AS mdm_solution_identifier 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2 AS AAPPLLIIDD1EENNVV_install_base_item_t2 
                Left Outer Join
                    (
                        SELECT 
                            instance_id AS auto_c00,
                            item_instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t3      
                        GROUP BY
                            instance_id,
                            item_instance_id 
                        HAVING
                            COUNT(*) = 1
                    )autoAlias_60 
                        ON (
                            autoAlias_60.auto_c00 = AAPPLLIIDD1EENNVV_install_base_item_t2.instance_id 
                            AND autoAlias_60.auto_c01 = AAPPLLIIDD1EENNVV_install_base_item_t2.item_instance_id
                        )"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    WITH CTE AS 
            (SELECT   T1.instance_id ,T1.item_instance_id FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t1 T1
                LEFT JOIN
                (SELECT  instance_id,item_instance_id,current_record_ind FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2)T2
                ON T1.instance_id=T2.instance_id
                AND T1.item_instance_id=T2.item_instance_id
                AND T2.current_record_ind = 'D'
                WHERE T2.instance_id IS NULL
                AND T2.item_instance_id IS NULL
            )
            INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld
            SELECT A.* from  """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld A
            LEFT OUTER JOIN
            CTE B
            ON  A.instance_id=B.instance_id
            AND A.item_instance_id=B.item_instance_id
            WHERE B.instance_id IS  NULL
            AND B.item_instance_id IS  NULL"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld           SELECT
                    instance_id,
                    item_instance_id,
                    accounting_class_code,
                    active_end_date_time,
                    active_start_date_time,
                    actual_return_date_time,
                    as_of_date_time,
                    batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    complete_flag,
                    contract_reference_nbr,
                    created_by_name,
                    creation_complete_flag,
                    creation_date_time,
                    customer_view_flag,
                    ecf_nbr,
                    external_reference_nbr,
                    in_transit_order_line_id,
                    install_date_time,
                    install_location_id,
                    install_location_type_code,
                    inventory_item_id,
                    inventory_locator_id,
                    inventory_master_org_id,
                    inventory_org_id,
                    inventory_revision_code,
                    invtry_subinventory_name,
                    item_instance_condition_id,
                    item_instance_nbr,
                    item_instance_status_id,
                    item_instance_type_code,
                    item_instance_usage_code,
                    item_qty,
                    last_oe_agreement_id,
                    last_oe_po_nbr,
                    last_order_line_id,
                    last_pa_project_id,
                    last_pa_task_id,
                    last_po_line_id,
                    last_rma_line_id,
                    last_tran_line_detail_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    last_valid_invtry_org_id,
                    last_wip_entity_id,
                    location_id,
                    location_type_code,
                    lot_nbr,
                    manually_created_flag,
                    merchant_view_flag,
                    migrated_flag,
                    mnfctr_serial_nbr_flag,
                    owner_cust_acct_id,
                    owner_party_id,
                    owner_party_table_name,
                    pa_project_id,
                    pa_project_task_id,
                    prev_install_at_location_id,
                    product_reference_nbr,
                    purchase_order_line_id,
                    return_by_date_time,
                    sellable_flag,
                    serial_nbr,
                    system_id,
                    tran_code,
                    uom_code,
                    warranty_code,
                    warranty_eff_date,
                    warranty_exp_date,
                    wip_entity_id,
                    wot_quote_nbr,
                    sot_order_nbr,
                    prev_serial_nbr,
                    future_move_concat_value,
                    om_invoice_date,
                    previous_prod_id,
                    previous_prod_id_formatted,
                    security_risk_code,
                    prev_cust_nbr,
                    prev_site_nbr,
                    service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date,
                cit_vendor_code,        
                mdm_product_identifier, 
                mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t1 
                LEFT OUTER JOIN
                    (
                        SELECT
                            instance_id AS auto_c00,
                            item_instance_id AS auto_c01  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2     
                        WHERE
                            upper(trim(current_record_ind)) = 'D'
                    ) AS autoAlias_64 
                        ON (
                            upper(trim(instance_id)) = upper(trim(autoAlias_64.AUTO_C00)) 
                            AND item_instance_id = autoAlias_64.AUTO_C01 
                        ) 
                WHERE
                    autoAlias_64.AUTO_C00 IS  NULL  
                    AND autoAlias_64.AUTO_C01 IS  NULL"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t1"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t1           SELECT
                    instance_id,
                    item_instance_id,
                    accounting_class_code,
                    active_end_date_time,
                    active_start_date_time,
                    actual_return_date_time,
                    as_of_date_time,
                    batch_nbr,
                    complete_flag,
                    contract_reference_nbr,
                    created_by_name,
                    creation_complete_flag,
                    creation_date_time,
                    customer_view_flag,
                    ecf_nbr,
                    external_reference_nbr,
                    in_transit_order_line_id,
                    install_date_time,
                    install_location_id,
                    install_location_type_code,
                    inventory_item_id,
                    inventory_locator_id,
                    inventory_master_org_id,
                    inventory_org_id,
                    inventory_revision_code,
                    invtry_subinventory_name,
                    item_instance_condition_id,
                    item_instance_nbr,
                    item_instance_status_id,
                    item_instance_type_code,
                    item_instance_usage_code,
                    item_qty,
                    last_oe_agreement_id,
                    last_oe_po_nbr,
                    last_order_line_id,
                    last_pa_project_id,
                    last_pa_task_id,
                    last_po_line_id,
                    last_rma_line_id,
                    last_tran_line_detail_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    last_valid_invtry_org_id,
                    last_wip_entity_id,
                    location_id,
                    location_type_code,
                    lot_nbr,
                    manually_created_flag,
                    merchant_view_flag,
                    migrated_flag,
                    mnfctr_serial_nbr_flag,
                    owner_cust_acct_id,
                    owner_party_id,
                    owner_party_table_name,
                    pa_project_id,
                    pa_project_task_id,
                    prev_install_at_location_id,
                    product_reference_nbr,
                    purchase_order_line_id,
                    return_by_date_time,
                    sellable_flag,
                    serial_nbr,
                    system_id,
                    tran_code,
                    uom_code,
                    warranty_code,
                    warranty_eff_date,
                    warranty_exp_date,
                    wip_entity_id,
                    wot_quote_nbr,
                    sot_order_nbr,
                    prev_serial_nbr,
                    future_move_concat_value,
                    om_invoice_date,
                    previous_prod_id,
                    previous_prod_id_formatted,
                    security_risk_code,
                    prev_cust_nbr,
                    prev_site_nbr,
                    service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date,
                    cit_vendor_code,        
                mdm_product_identifier, 
                mdm_solution_identifier  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_ml 
                    WHERE
                        upper(trim(tran_code)) = 'D'"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2           SELECT
                    l.instance_id,
                    l.item_instance_id,
                    l.accounting_class_code,
                    l.active_end_date_time,
                    l.active_start_date_time,
                    l.actual_return_date_time,
                    t.as_of_date_time,
                    t.batch_nbr,
                    l.as_of_date_time,
                    l.batch_nbr,
                    l.complete_flag,
                    l.contract_reference_nbr,
                    l.created_by_name,
                    l.creation_complete_flag,
                    l.creation_date_time,
                    null,
                    l.customer_view_flag,
                    l.ecf_nbr,
                    l.external_reference_nbr,
                    l.in_transit_order_line_id,
                    l.install_date_time,
                    l.install_location_id,
                    l.install_location_type_code,
                    l.inventory_item_id,
                    l.inventory_locator_id,
                    l.inventory_master_org_id,
                    l.inventory_org_id,
                    l.inventory_revision_code,
                    l.invtry_subinventory_name,
                    l.item_instance_condition_id,
                    l.item_instance_nbr,
                    l.item_instance_status_id,
                    l.item_instance_type_code,
                    l.item_instance_usage_code,
                    l.item_qty,
                    l.last_oe_agreement_id,
                    l.last_oe_po_nbr,
                    l.last_order_line_id,
                    l.last_pa_project_id,
                    l.last_pa_task_id,
                    l.last_po_line_id,
                    l.last_rma_line_id,
                    l.last_tran_line_detail_id,
                    l.last_update_date_time,
                    l.last_update_login_name,
                    l.last_updated_by_name,
                    l.last_valid_invtry_org_id,
                    l.last_wip_entity_id,
                    l.location_id,
                    l.location_type_code,
                    l.lot_nbr,
                    l.manually_created_flag,
                    l.merchant_view_flag,
                    l.migrated_flag,
                    l.mnfctr_serial_nbr_flag,
                    l.owner_cust_acct_id,
                    l.owner_party_id,
                    l.owner_party_table_name,
                    l.pa_project_id,
                    l.pa_project_task_id,
                    l.prev_install_at_location_id,
                    l.product_reference_nbr,
                    l.purchase_order_line_id,
                    l.return_by_date_time,
                    l.sellable_flag,
                    l.serial_nbr,
                    l.system_id,
                    t.tran_code,
                    l.uom_code,
                    l.warranty_code,
                    l.warranty_eff_date,
                    l.warranty_exp_date,
                    l.wip_entity_id,
                    l.wot_quote_nbr,
                    l.sot_order_nbr,
                    l.prev_serial_nbr,
                    l.future_move_concat_value,
                    l.om_invoice_date,
                    l.previous_prod_id,
                    l.previous_prod_id_formatted,
                    l.security_risk_code,
                    l.prev_cust_nbr,
                    l.prev_site_nbr,
                    l.service_tier_name,
                    l.esd_flag,
                    l.media_type,
                    l.license_model,
                    l.license_start_date,
                    l.license_end_date,
                l.cit_vendor_code,        
                l.mdm_product_identifier, 
                l.mdm_solution_identifier
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld    AS l   ,
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t1    AS t   
                WHERE
                    l.instance_id = t.instance_id  
                    AND l.item_instance_id = t.item_instance_id   
                    AND CONCAT(CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) < CONCAT(CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr)"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
            INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld SELECT
                    a.*  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld    AS A  
                LEFt OUTER JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_purge_t1    AS B  
                        on  (
                            A.item_instance_id = B.item_instance_id  
                            AND a.instance_id = B.instance_id  
                        )  
                where
                    B.item_instance_id IS NULL  
                    and  B.instance_id IS NULL  """
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld
        SELECT A.* from  """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld A
        LEFT OUTER JOIN
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2 B
        ON  A.instance_id=B.instance_id and
        A.item_instance_id=B.item_instance_id
        WHERE B.instance_id IS NULL
        AND B.item_instance_id IS NULL"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB1 + """.install_base_item_ld           SELECT
                    instance_id,
                    item_instance_id,
                    accounting_class_code,
                    active_end_date_time,
                    active_start_date_time,
                    actual_return_date_time,
                    alt_as_of_date_time,
                    alt_batch_nbr,
                    as_of_date_time,
                    batch_nbr,
                    complete_flag,
                    contract_reference_nbr,
                    created_by_name,
                    creation_complete_flag,
                    creation_date_time,
                    customer_view_flag,
                    ecf_nbr,
                    external_reference_nbr,
                    in_transit_order_line_id,
                    install_date_time,
                    install_location_id,
                    install_location_type_code,
                    inventory_item_id,
                    inventory_locator_id,
                    inventory_master_org_id,
                    inventory_org_id,
                    inventory_revision_code,
                    invtry_subinventory_name,
                    item_instance_condition_id,
                    item_instance_nbr,
                    item_instance_status_id,
                    item_instance_type_code,
                    item_instance_usage_code,
                    item_qty,
                    last_oe_agreement_id,
                    last_oe_po_nbr,
                    last_order_line_id,
                    last_pa_project_id,
                    last_pa_task_id,
                    last_po_line_id,
                    last_rma_line_id,
                    last_tran_line_detail_id,
                    last_update_date_time,
                    last_update_login_name,
                    last_updated_by_name,
                    last_valid_invtry_org_id,
                    last_wip_entity_id,
                    location_id,
                    location_type_code,
                    lot_nbr,
                    manually_created_flag,
                    merchant_view_flag,
                    migrated_flag,
                    mnfctr_serial_nbr_flag,
                    owner_cust_acct_id,
                    owner_party_id,
                    owner_party_table_name,
                    pa_project_id,
                    pa_project_task_id,
                    prev_install_at_location_id,
                    product_reference_nbr,
                    purchase_order_line_id,
                    return_by_date_time,
                    sellable_flag,
                    serial_nbr,
                    system_id,
                    tran_code,
                    uom_code,
                    warranty_code,
                    warranty_eff_date,
                    warranty_exp_date,
                    wip_entity_id,
                    wot_quote_nbr,
                    sot_order_nbr,
                    prev_serial_nbr,
                    future_move_concat_value,
                    om_invoice_date,
                    previous_prod_id,
                    previous_prod_id_formatted,
                    security_risk_code,
                    prev_cust_nbr,
                    prev_site_nbr,
                    service_tier_name,
                    esd_flag,
                    media_type,
                    license_model,
                    license_start_date,
                    license_end_date,
                cit_vendor_code,        
                mdm_product_identifier, 
                mdm_solution_identifier
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_install_base_item_t2"""
        print("Job 'D8_Install_Base_item_merged'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = D8InstallBaseitemmergedpy(sparkSession)
    try:
        m.execute()
    except Exception as e:
        print(e)
        print("Error in D8InstallBaseitemmergedpy")
        sparkSession.stop()
        sys.exit(1)


if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("D8InstallBaseitemmergedpy").enableHiveSupport().getOrCreate()
    main()
    sparkSession.stop()
    sys.exit(0)
