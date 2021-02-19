use ${hivevar:targetSchema};
DROP TABLE IF EXISTS geo_rollup_xref;
CREATE TABLE IF NOT EXISTS geo_rollup_xref          stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.geo_rollup_xref
;