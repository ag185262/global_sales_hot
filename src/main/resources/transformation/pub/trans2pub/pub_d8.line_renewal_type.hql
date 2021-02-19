use ${hivevar:targetSchema};
DROP TABLE IF EXISTS line_renewal_type;
CREATE TABLE IF NOT EXISTS line_renewal_type         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_gc_tedw.line_renewal_type
;

