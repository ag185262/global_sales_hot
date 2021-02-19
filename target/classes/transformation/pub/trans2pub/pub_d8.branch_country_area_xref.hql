use ${hivevar:targetSchema};
DROP TABLE IF EXISTS branch_country_area_xref;
CREATE TABLE IF NOT EXISTS branch_country_area_xref  stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_serviceslob_edw_wd_tedw.branch_country_area_xref   ;

