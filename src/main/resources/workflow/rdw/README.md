### src/main/resources/workflow/rdw

This location is used to store RDW Schema oozie workflows. Modularize by repository name.

Example:
```
src/main/resources/workflow/rdw
├── edl-fdr-load-rdw-workflow.xml
└── order
    ├── custom
    │   ├── edl-fdr-load-rdw-order-fact-order-line.xml
    │   ├── edl-fdr-load-rdw-order-transactional-workflow.xml
    │   └── edl-fdr-load-rdw-workflow.xml
    ├── edl-fdr-load-rdw-order-master-workflow.xml
    ├── edl-fdr-load-rdw-order-ref-workflow.xml
    ├── edl-fdr-load-rdw-order-transactional-workflow.xml
    └── edl-fdr-load-rdw-order-translation-rate-history-workflow.xml
```