### src/main/resources/workflow/hub

This location is used to store HUB Schema oozie workflows. Modularize by repository name.

Example:
```
src/main/resources/workflow/hub
├── edl-fdr-load-hub-workflow.xml
└── order
    ├── custom
    │   ├── edl-fdr-load-hub-order-fact-order-line.xml
    │   └── edl-fdr-load-hub-order-trans-rate.xml
    ├── edl-fdr-load-hub-order-master-workflow.xml
    ├── edl-fdr-load-hub-order-ref-workflow.xml
    └── edl-fdr-load-hub-order-transactional-workflow.xml
```