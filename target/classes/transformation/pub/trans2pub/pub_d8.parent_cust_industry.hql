use ${hivevar:targetSchema};
DROP TABLE IF EXISTS parent_cust_industry;
CREATE TABLE IF NOT EXISTS parent_cust_industry stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.parent_cust_industry
;
