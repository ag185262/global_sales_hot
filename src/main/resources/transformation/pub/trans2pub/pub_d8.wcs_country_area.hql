use ${hivevar:targetSchema};
DROP TABLE IF EXISTS wcs_country_area;
CREATE TABLE IF NOT EXISTS wcs_country_area          stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.wcs_country_area
;