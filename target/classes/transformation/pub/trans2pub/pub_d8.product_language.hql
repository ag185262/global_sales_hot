use ${hivevar:targetSchema};
DROP TABLE IF EXISTS product_language;
CREATE TABLE IF NOT EXISTS product_language          stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_sop_edw_pa_tedw.product_language                   ;

