
use ${hivevar:targetSchema};
DROP TABLE IF EXISTS naics_dn ;
CREATE TABLE IF NOT EXISTS naics_dn stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_gc_tedw.naics_dn 
;

