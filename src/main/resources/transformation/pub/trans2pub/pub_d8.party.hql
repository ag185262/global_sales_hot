use ${hivevar:targetSchema};
DROP TABLE IF EXISTS party;
CREATE TABLE IF NOT EXISTS party                     stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_gc_tedw.party                     ;

