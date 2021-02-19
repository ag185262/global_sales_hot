use ${hivevar:targetSchema};
DROP TABLE IF EXISTS es_country_default_mig;
CREATE TABLE IF NOT EXISTS es_country_default_mig stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_serviceslob_edw_d8_tedw.es_country_default_mig
;
