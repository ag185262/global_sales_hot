use ${hivevar:targetSchema};
DROP TABLE IF EXISTS product;
CREATE TABLE IF NOT EXISTS product                   stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_sop_edw_pa_tedw.product                            ;

