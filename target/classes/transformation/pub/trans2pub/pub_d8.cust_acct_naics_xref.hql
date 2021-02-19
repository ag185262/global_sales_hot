
use ${hivevar:targetSchema};
DROP TABLE IF EXISTS cust_acct_naics_xref ;
CREATE TABLE IF NOT EXISTS cust_acct_naics_xref stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_gc_tedw.cust_acct_naics_xref
;

