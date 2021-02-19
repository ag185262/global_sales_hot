### src/main/resources/transformation/rdw

This location is used to store RDW Schema DMLs. Modularize by repository name.

Example:
```
src/main/resources/transformation/rdw
├── invoice
│   ├── dim_inv_ar_adjustments_insert.hql
│   ├── dim_inv_ar_credit_profiles_insert.hql
│   ├── dim_inv_ar_notes_insert.hql
│   ├── dim_inv_ar_payment_schedules_insert.hql
│   ├── dim_inv_ar_receivable_application_insert.sql
│   ├── dim_inv_batch_sources_insert.hql
│   ├── dim_inv_batches_insert.hql
│   ├── dim_inv_ebtaxes_insert.hql
│   ├── dim_inv_gl_chart_of_accounts_insert.hql
│   ├── dim_inv_header_insert.hql
│   ├── dim_inv_ledgers_insert.hql
│   ├── dim_inv_ra_rules_insert.hql
│   ├── dim_inv_transaction_types_insert.hql
│   └── fact_invoice_line_insert.hql
└── order
    ├── dim_func_grp_insert.hql
    ├── dim_geography_insert.hql
    ├── dim_industry_insert.hql
    ├── dim_master_customer_insert.hql
    ├── dim_master_pid_insert.hql
    ├── dim_master_product_insert.hql
    ├── dim_master_solution_insert.hql
    ├── dim_order_header_insert.hql
    ├── dim_order_holds_insert.hql
    ├── dim_order_operating_unit_insert.hql
    ├── dim_order_price_list_insert.hql
    ├── dim_order_product_insert.hql
    ├── dim_order_shipping_insert.hql
    ├── dim_order_type_insert.hql
    ├── dim_order_warehouse_insert.hql
    ├── dim_sales_person_insert.hql
    ├── dim_site_use_locations_insert.hql
    ├── dim_translation_rate_hist_insert.hql
    ├── dim_translation_rate_insert.hql
    └── fact_order_line_insert.hql
```