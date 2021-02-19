use ${hivevar:targetSchema};
DROP TABLE IF EXISTS curr_calc;
CREATE TABLE IF NOT EXISTS curr_calc                 stored  as parquet tblproperties ("parquet.compression"="snappy") AS
SELECT
dtl__capxtimestamp,
dtl__capxaction,
dtl__capxrowid,
acc_date,
trim(translate_from_curr_code) as translate_from_curr_code,
trim(translate_to_curr_code)  as translate_to_curr_code,
us_average_rate,
us_end_of_period_rate,
local_average_rate,
local_end_of_period_rate,
euro_average_rate,
euro_end_of_period_rate,
edl_stg_load_time,
edl_trans_load_time
FROM trans_serviceslob_edw_d2_tedw.curr_calc;



