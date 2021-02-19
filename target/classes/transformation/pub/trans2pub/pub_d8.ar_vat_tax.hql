use ${hivevar:targetSchema};
DROP TABLE IF EXISTS ar_vat_tax;
CREATE TABLE IF NOT EXISTS ar_vat_tax stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_cfocontroller_edw_s3_tedw.ar_vat_tax
;
