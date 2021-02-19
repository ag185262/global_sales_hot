use ${hivevar:targetSchema};
DROP TABLE IF EXISTS spt_coverage;
CREATE TABLE IF NOT EXISTS spt_coverage             stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_orderfulfill_edw_eo_tedw.spt_coverage             ;

