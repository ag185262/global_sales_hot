use ${hivevar:targetSchema};
DROP TABLE IF EXISTS svc_adv_solution_xref;
CREATE TABLE IF NOT EXISTS svc_adv_solution_xref stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.svc_adv_solution_xref
;
