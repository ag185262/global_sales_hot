use ${hivevar:targetSchema};
DROP TABLE IF EXISTS service_offer_category;
CREATE TABLE IF NOT EXISTS service_offer_category stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.service_offer_category
;
