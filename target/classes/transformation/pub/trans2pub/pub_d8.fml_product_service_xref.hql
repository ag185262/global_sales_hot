use ${hivevar:targetSchema};
DROP TABLE IF EXISTS fml_product_service_xref;
CREATE TABLE IF NOT EXISTS fml_product_service_xref         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.fml_product_service_xref         ;

