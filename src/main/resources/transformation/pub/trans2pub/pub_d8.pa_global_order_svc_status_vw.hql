use ${hivevar:targetSchema};
DROP TABLE IF EXISTS pa_global_order_svc_status_vw;
CREATE TABLE IF NOT EXISTS pa_global_order_svc_status_vw                   stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_sop_edw_pa_tedw.pa_global_order_svc_status_vw                            ;

