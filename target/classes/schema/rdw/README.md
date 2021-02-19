### src/main/resources/schema/rdw

This location is used to store RDW Schema DDLs. Modularize by repository name.

Example:
```
src/main/resources/schema/rdw/
├── order
│   ├── derive-tables.hql
│   ├── drop-tables.hql
│   ├── init_order_ref_data.hql
│   └── order_create.hql
└── revenue
```