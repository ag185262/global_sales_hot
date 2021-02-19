use ${hivevar:targetSchema};
DROP TABLE IF EXISTS tax_exemption;
CREATE TABLE IF NOT EXISTS tax_exemption             stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_cfocontroller_edw_s3_tedw.tax_exemption            ;

