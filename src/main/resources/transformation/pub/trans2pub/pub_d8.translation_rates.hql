use ${hivevar:targetSchema};
DROP TABLE IF EXISTS translation_rates;
CREATE TABLE IF NOT EXISTS translation_rates          stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_cfocontroller_edw_e3_tedw.translation_rates
;