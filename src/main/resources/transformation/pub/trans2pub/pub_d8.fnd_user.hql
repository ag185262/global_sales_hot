use ${hivevar:targetSchema};
DROP TABLE IF EXISTS fnd_user;
CREATE TABLE IF NOT EXISTS fnd_user stored  as parquet tblproperties ("parquet.compression"="snappy") AS
select
A.dtl__capxtimestamp,
A.dtl__capxaction,
A.dtl__capxrowid,
A.USER_NAME,
A.USER_ID,
A.edl_ingest_channel,
A.edl_ingest_time
from
(SELECT
dtl__capxtimestamp,
dtl__capxaction,
dtl__capxrowid,
USER_NAME,
USER_ID,
edl_ingest_channel,
edl_ingest_time,
row_number() over (partition by USER_ID
                   order by edl_ingest_time desc,edl_record_sequence desc ) as rnk
FROM trans_itoperations_erp_applsys_erpprod.fnd_user) A
WHERE A.rnk = 1;