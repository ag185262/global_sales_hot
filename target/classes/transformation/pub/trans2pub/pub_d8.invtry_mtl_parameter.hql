use ${hivevar:targetSchema};
DROP TABLE IF EXISTS invtry_mtl_parameter;
CREATE TABLE IF NOT EXISTS invtry_mtl_parameter      stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_sop_edw_s6_tedw.invtry_mtl_parameter               ;

