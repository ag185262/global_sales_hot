use ${hivevar:targetSchema};
DROP TABLE IF EXISTS erp_operating_unit;
CREATE TABLE IF NOT EXISTS erp_operating_unit         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.erp_operating_unit         ;

