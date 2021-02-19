use ${hivevar:targetSchema};
DROP TABLE IF EXISTS sales_order ;
CREATE TABLE IF NOT EXISTS sales_order               stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_orderfulfill_edw_s2_tedw.sales_order               ;

