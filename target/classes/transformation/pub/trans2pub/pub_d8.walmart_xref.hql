use ${hivevar:targetSchema};
DROP TABLE IF EXISTS walmart_xref;
CREATE TABLE IF NOT EXISTS walmart_xref stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.walmart_xref
;
