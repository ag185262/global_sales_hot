use ${hivevar:targetSchema};
DROP TABLE IF EXISTS product_adv_solution;
CREATE TABLE IF NOT EXISTS product_adv_solution                   stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_sop_edw_pa_tedw.product_adv_solution                               ;

