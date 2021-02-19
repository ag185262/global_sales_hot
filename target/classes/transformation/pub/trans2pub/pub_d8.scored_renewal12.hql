use ${hivevar:targetSchema};
DROP TABLE IF EXISTS scored_renewal12;
CREATE TABLE IF NOT EXISTS scored_renewal12             stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_orderfulfill_edw_eo_tedw.scored_renewal12             ;

