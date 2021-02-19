use ${hivevar:targetSchema};
DROP TABLE IF EXISTS cust_account_site;
CREATE TABLE IF NOT EXISTS cust_account_site         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_gc_tedw.cust_account_site         ;

