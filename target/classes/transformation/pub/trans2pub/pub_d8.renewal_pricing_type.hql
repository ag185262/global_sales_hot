use ${hivevar:targetSchema};
DROP TABLE IF EXISTS renewal_pricing_type;
CREATE TABLE IF NOT EXISTS renewal_pricing_type             stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_gc_tedw.renewal_pricing_type             ;

