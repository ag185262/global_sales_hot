use ${hivevar:targetSchema};
DROP TABLE IF EXISTS customer_account;
CREATE TABLE IF NOT EXISTS customer_account          stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_gc_tedw.customer_account          ;

