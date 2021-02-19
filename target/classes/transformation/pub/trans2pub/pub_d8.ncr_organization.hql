use ${hivevar:targetSchema};
DROP TABLE IF EXISTS ncr_organization;
CREATE TABLE IF NOT EXISTS ncr_organization stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.ncr_organization
;