use ${hivevar:targetSchema};
DROP TABLE IF EXISTS gl_code_combinations;
CREATE TABLE IF NOT EXISTS gl_code_combinations          stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_cfocontroller_edw_e3_tedw.gl_code_combinations
;
