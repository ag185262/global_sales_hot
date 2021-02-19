### src/main/resources/transformation/hub

This location is used to store HUB Schema DMLs. Modularize by repository name.

Example:
```
src/main/resources/transformation/hub
├── order
│   ├── hub_dim_func_grp_insert.hql
│   ├── hub_dim_geography_insert.hql
│   ├── hub_dim_industry_insert.hql
│   ├── hub_dim_master_customer_insert.hql
│   ├── hub_dim_master_pid_insert.hql
│   ├── hub_dim_master_product_insert.hql
│   ├── hub_dim_master_solution_insert.hql
│   ├── hub_dim_order_header_insert.hql
│   ├── hub_dim_order_holds_insert.hql
│   ├── hub_dim_order_operating_unit_insert.hql
│   ├── hub_dim_order_price_list_insert.hql
│   ├── hub_dim_order_product_insert.hql
│   ├── hub_dim_order_shipping_insert.hql
│   ├── hub_dim_order_type_insert.hql
│   ├── hub_dim_order_warehouse_insert.hql
│   ├── hub_dim_sales_person_insert.hql
│   ├── hub_dim_site_use_locations_insert.hql
│   ├── hub_dim_translation_rate_insert.hql
│   └── hub_fact_order_line_insert.hql
└── revenue
```