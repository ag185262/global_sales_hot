use ${hivevar:targetSchema};
DROP TABLE IF EXISTS contact_point;
CREATE TABLE IF NOT EXISTS contact_point         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_gc_tedw.contact_point         ;

