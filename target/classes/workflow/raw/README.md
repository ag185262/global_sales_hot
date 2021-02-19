### src/main/resources/workflow/raw

This location is used to store RAW Schema oozie workflows. Modularize by repository name.

Example:
```
src/main/resources/workflow/raw
├── custom
├── customer
├── jitcoms
│   ├── ddl
│   │   ├── sop_jitcoms_mfg_common1_Stg2Trans.xml
│   │   └── sop_jitcoms_procurement_hrw2_Stg2Trans.xml
│   └── dml
│       ├── historical
│       └── incremental
├── order
│   ├── ddl
│   └── dml
└── pmdm
```