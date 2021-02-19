use ${hivevar:targetSchema};
DROP TABLE IF EXISTS cs_customer;
CREATE TABLE IF NOT EXISTS cs_customer         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_serviceslob_edw_d2_tedw.cs_customer         ;

