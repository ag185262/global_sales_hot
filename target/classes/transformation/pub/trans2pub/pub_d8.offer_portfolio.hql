use ${hivevar:targetSchema};
DROP TABLE IF EXISTS offer_portfolio;
CREATE TABLE IF NOT EXISTS offer_portfolio          stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.offer_portfolio
;