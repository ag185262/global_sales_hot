
use ${hivevar:targetSchema};
DROP TABLE IF EXISTS customer_industry ;
CREATE TABLE IF NOT EXISTS customer_industry stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.customer_industry 
;

