use ${hivevar:targetSchema};
DROP TABLE IF EXISTS accounting_period_name;
CREATE TABLE IF NOT EXISTS accounting_period_name         stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.accounting_period_name         ;

