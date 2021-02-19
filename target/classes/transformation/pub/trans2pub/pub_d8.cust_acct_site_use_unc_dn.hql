use ${hivevar:targetSchema};
DROP TABLE IF EXISTS cust_acct_site_use_unc_dn;
CREATE TABLE IF NOT EXISTS cust_acct_site_use_unc_dn         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_gc_tedw.cust_acct_site_use_unc_dn         ;

