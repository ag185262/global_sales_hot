use ${hivevar:targetSchema};
DROP TABLE IF EXISTS country_instance_xref;
CREATE TABLE IF NOT EXISTS country_instance_xref     stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.country_instance_xref   ;

