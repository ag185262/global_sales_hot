use ${hivevar:targetSchema};
DROP TABLE IF EXISTS corporate_product_line;
CREATE TABLE IF NOT EXISTS corporate_product_line         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.corporate_product_line         ;

