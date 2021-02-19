use ${hivevar:targetSchema};
DROP TABLE IF EXISTS mtl_item_category;
CREATE TABLE IF NOT EXISTS mtl_item_category         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_sop_edw_s6_tedw.mtl_item_category                  ;

