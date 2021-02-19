use ${hivevar:targetSchema};
DROP TABLE IF EXISTS currency_xref ;
CREATE TABLE IF NOT EXISTS currency_xref             stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.currency_xref             ;

