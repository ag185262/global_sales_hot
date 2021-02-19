use ${hivevar:targetSchema};
DROP TABLE IF EXISTS product_dn;
CREATE TABLE IF NOT EXISTS product_dn          stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_sop_edw_m3_tedw.product_dn
;