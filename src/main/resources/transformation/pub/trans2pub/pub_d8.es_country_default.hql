use ${hivevar:targetSchema};
DROP TABLE IF EXISTS es_country_default;
CREATE TABLE IF NOT EXISTS es_country_default stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_serviceslob_edw_o1_tedw.es_country_default
;
