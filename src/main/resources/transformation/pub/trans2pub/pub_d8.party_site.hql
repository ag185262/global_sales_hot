use ${hivevar:targetSchema};
DROP TABLE IF EXISTS party_site;
CREATE TABLE IF NOT EXISTS party_site       stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_gc_tedw.party_site_ld
;

