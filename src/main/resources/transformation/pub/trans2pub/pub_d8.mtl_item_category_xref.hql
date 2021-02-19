use ${hivevar:targetSchema};

set hive.tez.container.size=${hivevar:containerSize};

insert OVERWRITE TABLE mtl_item_category_xref
SELECT
A.dtl__capxtimestamp,
A.dtl__capxaction,
A.dtl__capxrowid,
A.category_id,
A.category_set_id,
A.instance_id,
A.inventory_item_id,
A.inventory_source_code,
A.source_country_code,
A.as_of_date_time,
A.created_by_name,
A.creation_date_time,
A.last_update_date_time,
A.last_update_login_name,
A.last_updated_by_name,
A.batch_nbr,
A.tran_code,
A.edl_stg_load_time,
A.edl_trans_load_time,
A.edl_ingest_channel,
A.edl_ingest_time
FROM(
SELECT
dtl__capxtimestamp,
dtl__capxaction,
dtl__capxrowid,
category_id,
category_set_id,
instance_id,
inventory_item_id,
inventory_source_code,
source_country_code,
as_of_date_time,
created_by_name,
creation_date_time,
last_update_date_time,
last_update_login_name,
last_updated_by_name,
batch_nbr,
tran_code,
edl_stg_load_time,
edl_trans_load_time,
edl_ingest_channel,
edl_ingest_time,
row_number() over (partition by category_set_id ,instance_id,inventory_item_id ,inventory_source_code
                   order by as_of_date_time desc) as rnk
FROM trans_sop_edw_s6_tedw.mtl_item_category_xref) A
WHERE A.rnk = 1;