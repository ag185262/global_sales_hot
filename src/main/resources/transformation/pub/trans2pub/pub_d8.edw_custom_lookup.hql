use ${hivevar:targetSchema};
DROP TABLE IF EXISTS edw_custom_lookup  ;
CREATE TABLE IF NOT EXISTS edw_custom_lookup         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_serviceslob_edw_o1_tedw.edw_custom_lookup          ;

