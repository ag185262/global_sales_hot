use ${hivevar:targetSchema};
DROP TABLE IF EXISTS invoice_rule;
CREATE TABLE IF NOT EXISTS invoice_rule              stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_cfocontroller_edw_s3_tedw.invoice_rule             ;

