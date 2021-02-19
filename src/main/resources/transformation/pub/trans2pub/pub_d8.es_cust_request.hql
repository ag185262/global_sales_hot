use ${hivevar:targetSchema};
DROP TABLE IF EXISTS es_cust_request;
CREATE TABLE IF NOT EXISTS es_cust_request stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_serviceslob_edw_o1_tedw.es_cust_request
;
