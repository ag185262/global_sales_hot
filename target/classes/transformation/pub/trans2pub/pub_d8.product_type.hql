use ${hivevar:targetSchema};
DROP TABLE IF EXISTS product_type;
CREATE TABLE IF NOT EXISTS product_type              stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.product_type              ;

