use ${hivevar:targetSchema};
DROP TABLE IF EXISTS cust_industry_supp;
CREATE TABLE IF NOT EXISTS cust_industry_supp stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.cust_industry_supp
;
