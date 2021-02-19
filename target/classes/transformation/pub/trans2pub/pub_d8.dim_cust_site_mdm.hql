use ${hivevar:targetSchema};

set hive.tez.container.size=${hivevar:containerSize};

DROP TABLE IF EXISTS dim_cust_site_mdm;
CREATE TABLE IF NOT EXISTS dim_cust_site_mdm         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_mm_tedw.dim_cust_site_mdm        ;
