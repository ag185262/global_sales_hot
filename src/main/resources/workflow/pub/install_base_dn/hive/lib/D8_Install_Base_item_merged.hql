--Original Query:
  ----DATABASE ${EEDDWWDDBB1}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB1};

--Original Query:
  ----DELETE FROM EEDDWWDDBB1.install_base_item_ld;

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${EEDDWWDDBB1}.install_base_item_ld;

--Original Query:
  ----INSERT INTO EEDDWWDDBB1.install_base_item_ld
--SELECT *
---FROM EEDDWWDDBB1.install_base_item
--;

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_item_ld          SELECT
            A.*  
        FROM
            ${EEDDWWDDBB1}.install_base_item A;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_01.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB1}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB1};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t1 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_t1(instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr, last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr,future_move_date_time,<WM_COMMENT_END>sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr, last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr, future_move_date_time,<WM_COMMENT_END>    sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date            FROM ${AAPPLLIIDD1EENNVV}_install_base_item_mlWHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,  instance_id,    item_instance_id )IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),    instance_id,    item_instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_ml    WHERE tran_code <> 'D'    GROUP BY 2,3 )AND tran_code <> 'D'

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1           SELECT
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_ml 
        INNER JOIN
            (
                SELECT
                    MAX( CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                    batch_nbr) ) AS auto_c00,
                    instance_id AS auto_c01,
                    item_instance_id AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_ml     
                WHERE
                    upper(trim(tran_code)) <> 'D'  
                GROUP BY
                    instance_id ,
                    item_instance_id 
            ) AS autoAlias_52 
                ON (
                    CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                batch_nbr) = autoAlias_52.AUTO_C00 
                AND instance_id = autoAlias_52.AUTO_C01
                AND item_instance_id = autoAlias_52.AUTO_C02 ) 
            WHERE
                upper(trim(tran_code)) <> 'D';
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_02.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB1}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB1};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t2 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_t1 FOR ACCESSLOCK TABLE  ${EEDDWWDDBB1}.install_base_item_ld FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_t2(instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,current_record_ind,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr, future_move_date_time,<WM_COMMENT_END>    sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,'Y',customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr, future_move_date_time,<WM_COMMENT_END>    sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM ${EEDDWWDDBB1}.install_base_item_ldWHERE (instance_id, item_instance_id)IN (SELECT     instance_id,    item_instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t1    )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2           SELECT
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
            ${EEDDWWDDBB1}.install_base_item_ld 
        INNER JOIN
            (
                SELECT
                    instance_id AS auto_c00,
                    item_instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1 
            ) AS autoAlias_54 
                ON (
                    instance_id = autoAlias_54.AUTO_C00 
                    AND item_instance_id = autoAlias_54.AUTO_C01 
                );

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t3 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t3;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_t1 FOR ACCESSLOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_t2 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_t3(instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr, future_move_date_time,<WM_COMMENT_END>    sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECTinstance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr, future_move_date_time,<WM_COMMENT_END>    sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t2WHERE (instance_id, item_instance_id)IN (SELECT     instance_id,    item_instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t1    )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t3           SELECT
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_03.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB1}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB1};

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_t1 FOR ACCESSLOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_t2 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_t3(instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr, last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr, future_move_date_time,<WM_COMMENT_END>    sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECTinstance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr, last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr, future_move_date_time,<WM_COMMENT_END>    sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t1WHERE (instance_id, item_instance_id)IN (SELECT     instance_id,    item_instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t2    )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t3           SELECT
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1 
        INNER JOIN
            (
                SELECT
                    instance_id AS auto_c00,
                    item_instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2 
            ) AS autoAlias_58 
                ON (
                    instance_id = autoAlias_58.AUTO_C00
                    AND item_instance_id = autoAlias_58.AUTO_C01 
                );
--Translated Query: SUCCESS
   INSERT overwrite table ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t3    SELECT distinct * from ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t3;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_04.upd.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB1}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB1};

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_t3 FOR ACCESSUPDATE ${AAPPLLIIDD1EENNVV}_install_base_item_t2SET current_record_ind = 'D'WHERE (instance_id, item_instance_id)  IN    (SELECT instance_id, item_instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t3    GROUP BY instance_id, item_instance_id    HAVING COUNT(*) = 1    )

--Translated Query: SUCCESS
--Modified Further (Comments: Replaced 5 step approach with insert overwrite and removed distinct with groupBy
--    DROP TABLE IF EXISTS ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2Temp;
--    CREATE TABLE ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2Temp Like ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2;
    INSERT 
    overwrite
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2 AS AAPPLLIIDD1EENNVV_install_base_item_t2 
        Left Outer Join
            (
                SELECT 
                    instance_id AS auto_c00,
                    item_instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t3      
                GROUP BY
                    instance_id,
                    item_instance_id 
                HAVING
                    COUNT(*) = 1
            )autoAlias_60 
                ON (
                    autoAlias_60.auto_c00 = AAPPLLIIDD1EENNVV_install_base_item_t2.instance_id 
                    AND autoAlias_60.auto_c01 = AAPPLLIIDD1EENNVV_install_base_item_t2.item_instance_id
                );
--    DROP table IF EXISTS ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2;
--    Alter table ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2Temp rename to ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2 ;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_05.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${EEDDWWDDBB1}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB1};
--(Changed insert overwrite statement using With block and removed 5 step process)
--Original Query:
  ----DELETE FROM install_base_item_ldWHERE (instance_id, item_instance_id )IN (SELECT instance_id, item_instance_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1    WHERE (instance_id, item_instance_id) NOT IN        (SELECT instance_id, item_instance_id        FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2        WHERE current_record_ind = 'D'        )    )

--Translated Query: SUCCESS

    -- DROP TABLE IF EXISTS  ${EEDDWWDDBB1}.install_base_item_ldTemp;
    -- CREATE TABLE ${EEDDWWDDBB1}.install_base_item_ldTemp LIKE ${EEDDWWDDBB1}.install_base_item_ld;
    -- INSERT 
    -- INTO
        -- TABLE
        -- ${EEDDWWDDBB1}.install_base_item_ldTemp SELECT
            -- Q1.* 
        -- FROM
            -- (SELECT
                -- * 
            -- FROM
                -- ${EEDDWWDDBB1}.install_base_item_ld ) AS  Q1  
        -- LEFT OUTER JOIN
            -- (
                -- SELECT
                    -- * 
                -- FROM
                    -- ${EEDDWWDDBB1}.install_base_item_ld      
            -- ) AS Q2 
                -- ON COALESCE( Q1.instance_id ,
            -- '1' ) = COALESCE( Q2.instance_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.item_instance_id ,
            -- 1) = COALESCE( Q2.item_instance_id ,
            -- 1) 
            -- AND COALESCE( Q1.accounting_class_code ,
            -- '1' ) = COALESCE( Q2.accounting_class_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.active_end_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.active_end_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.active_start_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.active_start_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.actual_return_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.actual_return_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
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
            -- AND COALESCE( Q1.complete_flag ,
            -- '1' ) = COALESCE( Q2.complete_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.contract_reference_nbr ,
            -- '1' ) = COALESCE( Q2.contract_reference_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.created_by_name ,
            -- '1' ) = COALESCE( Q2.created_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_complete_flag ,
            -- '1' ) = COALESCE( Q2.creation_complete_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.creation_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.customer_view_flag ,
            -- '1' ) = COALESCE( Q2.customer_view_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.ecf_nbr ,
            -- '1' ) = COALESCE( Q2.ecf_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.external_reference_nbr ,
            -- '1' ) = COALESCE( Q2.external_reference_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.in_transit_order_line_id ,
            -- 1) = COALESCE( Q2.in_transit_order_line_id ,
            -- 1) 
            -- AND COALESCE( Q1.install_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.install_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.install_location_id ,
            -- 1) = COALESCE( Q2.install_location_id ,
            -- 1) 
            -- AND COALESCE( Q1.install_location_type_code ,
            -- '1' ) = COALESCE( Q2.install_location_type_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.inventory_item_id ,
            -- 1) = COALESCE( Q2.inventory_item_id ,
            -- 1) 
            -- AND COALESCE( Q1.inventory_locator_id ,
            -- 1) = COALESCE( Q2.inventory_locator_id ,
            -- 1) 
            -- AND COALESCE( Q1.inventory_master_org_id ,
            -- 1) = COALESCE( Q2.inventory_master_org_id ,
            -- 1) 
            -- AND COALESCE( Q1.inventory_org_id ,
            -- 1) = COALESCE( Q2.inventory_org_id ,
            -- 1) 
            -- AND COALESCE( Q1.inventory_revision_code ,
            -- '1' ) = COALESCE( Q2.inventory_revision_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.invtry_subinventory_name ,
            -- '1' ) = COALESCE( Q2.invtry_subinventory_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.item_instance_condition_id ,
            -- 1) = COALESCE( Q2.item_instance_condition_id ,
            -- 1) 
            -- AND COALESCE( Q1.item_instance_nbr ,
            -- '1' ) = COALESCE( Q2.item_instance_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.item_instance_status_id ,
            -- 1) = COALESCE( Q2.item_instance_status_id ,
            -- 1) 
            -- AND COALESCE( Q1.item_instance_type_code ,
            -- '1' ) = COALESCE( Q2.item_instance_type_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.item_instance_usage_code ,
            -- '1' ) = COALESCE( Q2.item_instance_usage_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.item_qty ,
            -- 1) = COALESCE( Q2.item_qty ,
            -- 1) 
            -- AND COALESCE( Q1.last_oe_agreement_id ,
            -- 1) = COALESCE( Q2.last_oe_agreement_id ,
            -- 1) 
            -- AND COALESCE( Q1.last_oe_po_nbr ,
            -- '1' ) = COALESCE( Q2.last_oe_po_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_order_line_id ,
            -- 1) = COALESCE( Q2.last_order_line_id ,
            -- 1) 
            -- AND COALESCE( Q1.last_pa_project_id ,
            -- 1) = COALESCE( Q2.last_pa_project_id ,
            -- 1) 
            -- AND COALESCE( Q1.last_pa_task_id ,
            -- 1) = COALESCE( Q2.last_pa_task_id ,
            -- 1) 
            -- AND COALESCE( Q1.last_po_line_id ,
            -- 1) = COALESCE( Q2.last_po_line_id ,
            -- 1) 
            -- AND COALESCE( Q1.last_rma_line_id ,
            -- 1) = COALESCE( Q2.last_rma_line_id ,
            -- 1) 
            -- AND COALESCE( Q1.last_tran_line_detail_id ,
            -- 1) = COALESCE( Q2.last_tran_line_detail_id ,
            -- 1) 
            -- AND COALESCE( Q1.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.last_update_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.last_update_login_name ,
            -- '1' ) = COALESCE( Q2.last_update_login_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_updated_by_name ,
            -- '1' ) = COALESCE( Q2.last_updated_by_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.last_valid_invtry_org_id ,
            -- 1) = COALESCE( Q2.last_valid_invtry_org_id ,
            -- 1) 
            -- AND COALESCE( Q1.last_wip_entity_id ,
            -- 1) = COALESCE( Q2.last_wip_entity_id ,
            -- 1) 
            -- AND COALESCE( Q1.location_id ,
            -- 1) = COALESCE( Q2.location_id ,
            -- 1) 
            -- AND COALESCE( Q1.location_type_code ,
            -- '1' ) = COALESCE( Q2.location_type_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.lot_nbr ,
            -- '1' ) = COALESCE( Q2.lot_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.manually_created_flag ,
            -- '1' ) = COALESCE( Q2.manually_created_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.merchant_view_flag ,
            -- '1' ) = COALESCE( Q2.merchant_view_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.migrated_flag ,
            -- '1' ) = COALESCE( Q2.migrated_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.mnfctr_serial_nbr_flag ,
            -- '1' ) = COALESCE( Q2.mnfctr_serial_nbr_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.owner_cust_acct_id ,
            -- 1) = COALESCE( Q2.owner_cust_acct_id ,
            -- 1) 
            -- AND COALESCE( Q1.owner_party_id ,
            -- 1) = COALESCE( Q2.owner_party_id ,
            -- 1) 
            -- AND COALESCE( Q1.owner_party_table_name ,
            -- '1' ) = COALESCE( Q2.owner_party_table_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.pa_project_id ,
            -- 1) = COALESCE( Q2.pa_project_id ,
            -- 1) 
            -- AND COALESCE( Q1.pa_project_task_id ,
            -- 1) = COALESCE( Q2.pa_project_task_id ,
            -- 1) 
            -- AND COALESCE( Q1.prev_install_at_location_id ,
            -- '1' ) = COALESCE( Q2.prev_install_at_location_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.product_reference_nbr ,
            -- '1' ) = COALESCE( Q2.product_reference_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.purchase_order_line_id ,
            -- 1) = COALESCE( Q2.purchase_order_line_id ,
            -- 1) 
            -- AND COALESCE( Q1.return_by_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.return_by_date_time ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.sellable_flag ,
            -- '1' ) = COALESCE( Q2.sellable_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.serial_nbr ,
            -- '1' ) = COALESCE( Q2.serial_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.system_id ,
            -- 1) = COALESCE( Q2.system_id ,
            -- 1) 
            -- AND COALESCE( Q1.tran_code ,
            -- '1' ) = COALESCE( Q2.tran_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.uom_code ,
            -- '1' ) = COALESCE( Q2.uom_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.warranty_code ,
            -- '1' ) = COALESCE( Q2.warranty_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.warranty_eff_date ,
            -- '1' ) = COALESCE( Q2.warranty_eff_date ,
            -- '1' ) 
            -- AND COALESCE( Q1.warranty_exp_date ,
            -- '1' ) = COALESCE( Q2.warranty_exp_date ,
            -- '1' ) 
            -- AND COALESCE( Q1.wip_entity_id ,
            -- 1) = COALESCE( Q2.wip_entity_id ,
            -- 1) 
            -- AND COALESCE( Q1.wot_quote_nbr ,
            -- '1' ) = COALESCE( Q2.wot_quote_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.sot_order_nbr ,
            -- '1' ) = COALESCE( Q2.sot_order_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.prev_serial_nbr ,
            -- '1' ) = COALESCE( Q2.prev_serial_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.future_move_concat_value ,
            -- '1' ) = COALESCE( Q2.future_move_concat_value ,
            -- '1' ) 
            -- AND COALESCE( Q1.om_invoice_date ,
            -- '1' ) = COALESCE( Q2.om_invoice_date ,
            -- '1' ) 
            -- AND COALESCE( Q1.previous_prod_id ,
            -- '1' ) = COALESCE( Q2.previous_prod_id ,
            -- '1' ) 
            -- AND COALESCE( Q1.previous_prod_id_formatted ,
            -- '1' ) = COALESCE( Q2.previous_prod_id_formatted ,
            -- '1' ) 
            -- AND COALESCE( Q1.security_risk_code ,
            -- '1' ) = COALESCE( Q2.security_risk_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.prev_cust_nbr ,
            -- '1' ) = COALESCE( Q2.prev_cust_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.prev_site_nbr ,
            -- '1' ) = COALESCE( Q2.prev_site_nbr ,
            -- '1' ) 
            -- AND COALESCE( Q1.service_tier_name ,
            -- '1' ) = COALESCE( Q2.service_tier_name ,
            -- '1' ) 
            -- AND COALESCE( Q1.esd_flag ,
            -- '1' ) = COALESCE( Q2.esd_flag ,
            -- '1' ) 
            -- AND COALESCE( Q1.media_type ,
            -- '1' ) = COALESCE( Q2.media_type ,
            -- '1' ) 
            -- AND COALESCE( Q1.license_model ,
            -- '1' ) = COALESCE( Q2.license_model ,
            -- '1' ) 
            -- AND COALESCE( Q1.license_start_date ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.license_start_date ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.license_end_date ,
            -- cast ( CURRENT_DATE() as timestamp) ) = COALESCE( Q2.license_end_date ,
            -- cast ( CURRENT_DATE() as timestamp) ) 
            -- AND COALESCE( Q1.cit_vendor_code ,
            -- '1' ) = COALESCE( Q2.cit_vendor_code ,
            -- '1' ) 
            -- AND COALESCE( Q1.mdm_product_identifier ,
            -- '1' ) = COALESCE( Q2.mdm_product_identifier ,
            -- '1' ) 
            -- AND COALESCE( Q1.mdm_solution_identifier ,
            -- '1' ) = COALESCE( Q2.mdm_solution_identifier ,
            -- '1' ) 
        -- WHERE
            -- Q2.instance_id IS NULL 
            -- AND Q2.item_instance_id IS NULL 
            -- AND Q2.accounting_class_code IS NULL 
            -- AND Q2.active_end_date_time IS NULL 
            -- AND Q2.active_start_date_time IS NULL 
            -- AND Q2.actual_return_date_time IS NULL 
            -- AND Q2.alt_as_of_date_time IS NULL 
            -- AND Q2.alt_batch_nbr IS NULL 
            -- AND Q2.as_of_date_time IS NULL 
            -- AND Q2.batch_nbr IS NULL 
            -- AND Q2.complete_flag IS NULL 
            -- AND Q2.contract_reference_nbr IS NULL 
            -- AND Q2.created_by_name IS NULL 
            -- AND Q2.creation_complete_flag IS NULL 
            -- AND Q2.creation_date_time IS NULL 
            -- AND Q2.customer_view_flag IS NULL 
            -- AND Q2.ecf_nbr IS NULL 
            -- AND Q2.external_reference_nbr IS NULL 
            -- AND Q2.in_transit_order_line_id IS NULL 
            -- AND Q2.install_date_time IS NULL 
            -- AND Q2.install_location_id IS NULL 
            -- AND Q2.install_location_type_code IS NULL 
            -- AND Q2.inventory_item_id IS NULL 
            -- AND Q2.inventory_locator_id IS NULL 
            -- AND Q2.inventory_master_org_id IS NULL 
            -- AND Q2.inventory_org_id IS NULL 
            -- AND Q2.inventory_revision_code IS NULL 
            -- AND Q2.invtry_subinventory_name IS NULL 
            -- AND Q2.item_instance_condition_id IS NULL 
            -- AND Q2.item_instance_nbr IS NULL 
            -- AND Q2.item_instance_status_id IS NULL 
            -- AND Q2.item_instance_type_code IS NULL 
            -- AND Q2.item_instance_usage_code IS NULL 
            -- AND Q2.item_qty IS NULL 
            -- AND Q2.last_oe_agreement_id IS NULL 
            -- AND Q2.last_oe_po_nbr IS NULL 
            -- AND Q2.last_order_line_id IS NULL 
            -- AND Q2.last_pa_project_id IS NULL 
            -- AND Q2.last_pa_task_id IS NULL 
            -- AND Q2.last_po_line_id IS NULL 
            -- AND Q2.last_rma_line_id IS NULL 
            -- AND Q2.last_tran_line_detail_id IS NULL 
            -- AND Q2.last_update_date_time IS NULL 
            -- AND Q2.last_update_login_name IS NULL 
            -- AND Q2.last_updated_by_name IS NULL 
            -- AND Q2.last_valid_invtry_org_id IS NULL 
            -- AND Q2.last_wip_entity_id IS NULL 
            -- AND Q2.location_id IS NULL 
            -- AND Q2.location_type_code IS NULL 
            -- AND Q2.lot_nbr IS NULL 
            -- AND Q2.manually_created_flag IS NULL 
            -- AND Q2.merchant_view_flag IS NULL 
            -- AND Q2.migrated_flag IS NULL 
            -- AND Q2.mnfctr_serial_nbr_flag IS NULL 
            -- AND Q2.owner_cust_acct_id IS NULL 
            -- AND Q2.owner_party_id IS NULL 
            -- AND Q2.owner_party_table_name IS NULL 
            -- AND Q2.pa_project_id IS NULL 
            -- AND Q2.pa_project_task_id IS NULL 
            -- AND Q2.prev_install_at_location_id IS NULL 
            -- AND Q2.product_reference_nbr IS NULL 
            -- AND Q2.purchase_order_line_id IS NULL 
            -- AND Q2.return_by_date_time IS NULL 
            -- AND Q2.sellable_flag IS NULL 
            -- AND Q2.serial_nbr IS NULL 
            -- AND Q2.system_id IS NULL 
            -- AND Q2.tran_code IS NULL 
            -- AND Q2.uom_code IS NULL 
            -- AND Q2.warranty_code IS NULL 
            -- AND Q2.warranty_eff_date IS NULL 
            -- AND Q2.warranty_exp_date IS NULL 
            -- AND Q2.wip_entity_id IS NULL 
            -- AND Q2.wot_quote_nbr IS NULL 
            -- AND Q2.sot_order_nbr IS NULL 
            -- AND Q2.prev_serial_nbr IS NULL 
            -- AND Q2.future_move_concat_value IS NULL 
            -- AND Q2.om_invoice_date IS NULL 
            -- AND Q2.previous_prod_id IS NULL 
            -- AND Q2.previous_prod_id_formatted IS NULL 
            -- AND Q2.security_risk_code IS NULL 
            -- AND Q2.prev_cust_nbr IS NULL 
            -- AND Q2.prev_site_nbr IS NULL 
            -- AND Q2.service_tier_name IS NULL 
            -- AND Q2.esd_flag IS NULL 
            -- AND Q2.media_type IS NULL 
            -- AND Q2.license_model IS NULL 
            -- AND Q2.license_start_date IS NULL 
            -- AND Q2.license_end_date IS NULL 
            -- AND Q2.cit_vendor_code IS NULL 
            -- AND Q2.mdm_product_identifier IS NULL 
            -- AND Q2.mdm_solution_identifier IS NULL;
    -- DROP TABLE IF EXISTS  ${EEDDWWDDBB1}.install_base_item_ld;
    -- ALTER TABLE ${EEDDWWDDBB1}.install_base_item_ldTemp RENAME TO ${EEDDWWDDBB1}.install_base_item_ld;

    WITH CTE AS 
    (SELECT   T1.instance_id ,T1.item_instance_id FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1 T1
        LEFT JOIN
        (SELECT  instance_id,item_instance_id,current_record_ind FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2)T2
        ON T1.instance_id=T2.instance_id
        AND T1.item_instance_id=T2.item_instance_id
        AND T2.current_record_ind = 'D'
        WHERE T2.instance_id IS NULL
        AND T2.item_instance_id IS NULL
    )
        
    INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.install_base_item_ld
    SELECT A.* from  ${EEDDWWDDBB1}.install_base_item_ld A
    LEFT OUTER JOIN
    CTE B
    ON  A.instance_id=B.instance_id
    AND A.item_instance_id=B.item_instance_id
    WHERE B.instance_id IS  NULL
    AND B.item_instance_id IS  NULL;
--Original Query:
  ----LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1 FOR ACCESSINSERT INTO install_base_item_ld(instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr,future_move_date_time,<WM_COMMENT_END>sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECTinstance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,as_of_date_time,batch_nbr,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr,future_move_date_time,<WM_COMMENT_END>sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1WHERE ( instance_id, item_instance_id )NOT IN (SELECT  instance_id, item_instance_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2    WHERE current_record_ind = 'D'    )

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_item_ld           SELECT
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1 
        LEFT OUTER JOIN
            (
                SELECT
                    instance_id AS auto_c00,
                    item_instance_id AS auto_c01  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2     
                WHERE
                    upper(trim(current_record_ind)) = 'D'
            ) AS autoAlias_64 
                ON (
                    upper(trim(instance_id)) = upper(trim(autoAlias_64.AUTO_C00)) 
                    AND item_instance_id = autoAlias_64.AUTO_C01 
                ) 
        WHERE
            autoAlias_64.AUTO_C00 IS  NULL  
            AND autoAlias_64.AUTO_C01 IS  NULL;
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_06.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${TTMMPPDDBB1}

--Translated Query: SUCCESS

    use ${TTMMPPDDBB1};

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t1 ALL

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_ml FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_t1(instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr,future_move_date_time,<WM_COMMENT_END>sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECTinstance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr,future_move_date_time,<WM_COMMENT_END>sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           FROM ${AAPPLLIIDD1EENNVV}_install_base_item_mlWHERE (cast(as_of_date_time AS CHAR(26))||batch_nbr,        instance_id,        item_instance_id)IN (SELECT  MAX(cast(as_of_date_time AS CHAR(26))||batch_nbr),        instance_id,        item_instance_id    FROM ${AAPPLLIIDD1EENNVV}_install_base_item_ml    WHERE tran_code = 'D'    GROUP BY 2,3 )AND tran_code = 'D'

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1           SELECT
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_ml 
        INNER JOIN
            (
                SELECT
                    MAX( CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                    batch_nbr) ) AS auto_c00,
                    instance_id AS auto_c01,
                    item_instance_id AS auto_c02  
                FROM
                    ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_ml     
                WHERE
                    upper(trim(tran_code)) = upper(trim('D'))  
                GROUP BY
                    instance_id ,
                    item_instance_id 
            ) AS autoAlias_66 
                ON (
                    CONCAT(CAST (as_of_date_time AS CHAR(26)) ,
                batch_nbr) = autoAlias_66.AUTO_C00 
                AND instance_id = autoAlias_66.AUTO_C01
                AND item_instance_id = autoAlias_66.AUTO_C02 ) 
            WHERE
                upper(trim(tran_code)) = 'D';

--Original Query:
  ----DELETE FROM ${AAPPLLIIDD1EENNVV}_install_base_item_t2

--Translated Query: SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2;

--Original Query:
  ----LOCK TABLE  ${AAPPLLIIDD1EENNVV}_install_base_item_t1 FOR ACCESSINSERT INTO ${AAPPLLIIDD1EENNVV}_install_base_item_t2(instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr,future_move_date_time,<WM_COMMENT_END>sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECTl.instance_id,l.item_instance_id,l.accounting_class_code,l.active_end_date_time,l.active_start_date_time,l.actual_return_date_time,t.as_of_date_time,t.batch_nbr,l.as_of_date_time,l.batch_nbr,l.complete_flag,l.contract_reference_nbr,l.created_by_name,l.creation_complete_flag,l.creation_date_time,l.customer_view_flag,l.ecf_nbr,l.external_reference_nbr,l.in_transit_order_line_id,l.install_date_time,l.install_location_id,l.install_location_type_code,l.inventory_item_id,l.inventory_locator_id,l.inventory_master_org_id,l.inventory_org_id,l.inventory_revision_code,l.invtry_subinventory_name,l.item_instance_condition_id,l.item_instance_nbr,l.item_instance_status_id,l.item_instance_type_code,l.item_instance_usage_code,l.item_qty,l.last_oe_agreement_id,l.last_oe_po_nbr,l.last_order_line_id,l.last_po_line_id,l.last_pa_task_id,l.last_rma_line_id,l.last_tran_line_detail_id,l.last_update_date_time,l.last_update_login_name,l.last_updated_by_name,l.last_valid_invtry_org_id,l.last_wip_entity_id,l.location_id,l.location_type_code,l.lot_nbr,l.manually_created_flag,l.merchant_view_flag,l.migrated_flag,l.mnfctr_serial_nbr_flag,l.owner_cust_acct_id,l.owner_party_id,l.owner_party_table_name,l.pa_project_id,l.pa_project_task_id,l.prev_install_at_location_id,l.product_reference_nbr,l.purchase_order_line_id,l.return_by_date_time,l.sellable_flag,l.serial_nbr,l.system_id,t.tran_code,l.uom_code,l.warranty_code,l.warranty_eff_date,l.warranty_exp_date,l.wip_entity_id,l.wot_quote_nbr,l.last_pa_project_id,<WM_COMMENT_START>l.future_move_cust_site_nbr,l.future_move_customer_nbr,l.future_move_date_time,<WM_COMMENT_END>l.sot_order_nbr,l.prev_serial_nbr,l.future_move_concat_value,l.om_invoice_date,l.previous_prod_id,l.previous_prod_id_formatted,l.security_risk_code,l.prev_cust_nbr,l.prev_site_nbr,l.service_tier_name,l.esd_flag,                   l.media_type,                 l.license_model,              l.license_start_date,         l.license_end_date           FROM ${EEDDWWDDBB1}.install_base_item_ld l,     ${AAPPLLIIDD1EENNVV}_install_base_item_t1 tWHERE  l.instance_id = t.instance_idAND    l.item_instance_id = t.item_instance_idAND    CAST(l.as_of_date_time AS VARCHAR(26)) || l.batch_nbr <        CAST(t.as_of_date_time AS VARCHAR(26)) || t.batch_nbr

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2           SELECT
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
            ${EEDDWWDDBB1}.install_base_item_ld    AS l   ,
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t1    AS t   
        WHERE
            l.instance_id = t.instance_id  
            AND l.item_instance_id = t.item_instance_id   
            AND CONCAT(CAST (l.as_of_date_time AS VARCHAR(26)) , l.batch_nbr) < CONCAT(CAST (t.as_of_date_time AS VARCHAR(26)) , t.batch_nbr);
----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_07.del.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${EEDDWWDDBB1}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB1};

--Original Query:
  ----DELETE AFROM install_base_item_ld A,     ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_purge_t1 BWHERE 	A.item_instance_id = B.item_instance_id	AND a.instance_id = B.instance_id

--Translated Query: MANUAL

    INSERT OVERWRITE
        TABLE ${EEDDWWDDBB1}.install_base_item_ld SELECT
            a.* 
        FROM
            ${EEDDWWDDBB1}.install_base_item_ld    AS A   
        LEFt OUTER JOIN
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_purge_t1    AS B   
                on  (
                    A.item_instance_id = B.item_instance_id  
                    AND a.instance_id = B.instance_id  
                )  
        where
            B.item_instance_id IS NULL  
            and  B.instance_id IS NULL;

----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:40:25 
--Script Name: d8_install_base_item_07.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  ----DATABASE ${EEDDWWDDBB1}

--Translated Query: SUCCESS

    use ${EEDDWWDDBB1};
--(Changed insert overwrite statement using With block and removed 5 step process)
--Original Query:
  ----DELETE FROM install_base_item_ld WHERE (instance_id, item_instance_id)IN (SELECT instance_id, item_instance_id    FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2    )

--Translated Query: STATUS MANUAL
INSERT OVERWRITE TABLE ${EEDDWWDDBB1}.install_base_item_ld
SELECT A.* from  ${EEDDWWDDBB1}.install_base_item_ld A
LEFT OUTER JOIN
${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2 B
ON  A.instance_id=B.instance_id and
A.item_instance_id=B.item_instance_id
WHERE B.instance_id IS NULL
AND B.item_instance_id IS NULL;

--Original Query:
  ----LOCK TABLE  ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2 FOR ACCESSINSERT INTO install_base_item_ld(instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr, future_move_date_time,<WM_COMMENT_END>    sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date           )SELECT instance_id,item_instance_id,accounting_class_code,active_end_date_time,active_start_date_time,actual_return_date_time,alt_as_of_date_time,alt_batch_nbr,as_of_date_time,batch_nbr,complete_flag,contract_reference_nbr,created_by_name,creation_complete_flag,creation_date_time,customer_view_flag,ecf_nbr,external_reference_nbr,in_transit_order_line_id,install_date_time,install_location_id,install_location_type_code,inventory_item_id,inventory_locator_id,inventory_master_org_id,inventory_org_id,inventory_revision_code,invtry_subinventory_name,item_instance_condition_id,item_instance_nbr,item_instance_status_id,item_instance_type_code,item_instance_usage_code,item_qty,last_oe_agreement_id,last_oe_po_nbr,last_order_line_id,last_po_line_id,last_pa_task_id,last_rma_line_id,last_tran_line_detail_id,last_update_date_time,last_update_login_name,last_updated_by_name,last_valid_invtry_org_id,last_wip_entity_id,location_id,location_type_code,lot_nbr,manually_created_flag,merchant_view_flag,migrated_flag,mnfctr_serial_nbr_flag,owner_cust_acct_id,owner_party_id,owner_party_table_name,pa_project_id,pa_project_task_id,prev_install_at_location_id,product_reference_nbr,purchase_order_line_id,return_by_date_time,sellable_flag,serial_nbr,system_id,tran_code,uom_code,warranty_code,warranty_eff_date,warranty_exp_date,wip_entity_id,wot_quote_nbr,last_pa_project_id,<WM_COMMENT_START>future_move_cust_site_nbr,future_move_customer_nbr, future_move_date_time,<WM_COMMENT_END>    sot_order_nbr,prev_serial_nbr,future_move_concat_value,om_invoice_date,previous_prod_id,previous_prod_id_formatted,security_risk_code,prev_cust_nbr,prev_site_nbr,service_tier_name,esd_flag,                   media_type,                 license_model,              license_start_date,         license_end_date            FROM ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2

--Translated Query: SUCCESS

    INSERT 
    INTO
        TABLE
        ${EEDDWWDDBB1}.install_base_item_ld           SELECT
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
            ${TTMMPPDDBB1}.${AAPPLLIIDD1EENNVV}_install_base_item_t2;