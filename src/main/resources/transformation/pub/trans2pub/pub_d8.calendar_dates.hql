
use ${hivevar:targetSchema};
DROP TABLE IF EXISTS calendar_dates ;
CREATE TABLE IF NOT EXISTS calendar_dates            stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_dc_tedw.calendar_dates            ;

