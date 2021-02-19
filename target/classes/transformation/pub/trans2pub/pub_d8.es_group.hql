use ${hivevar:targetSchema};
DROP TABLE IF EXISTS es_group;
CREATE TABLE IF NOT EXISTS es_group stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_serviceslob_edw_o1_tedw.es_group
;
