use ${hivevar:targetSchema};
DROP TABLE IF EXISTS product_order_org;
CREATE TABLE IF NOT EXISTS product_order_org         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_sop_edw_pa_tedw.product_order_org                  ;

