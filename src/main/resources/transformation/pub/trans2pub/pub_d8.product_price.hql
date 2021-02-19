use ${hivevar:targetSchema};

set hive.tez.container.size=${hivevar:containerSize};

insert OVERWRITE TABLE product_price
SELECT
A.dtl__capxtimestamp,
A.dtl__capxaction,
A.dtl__capxrowid,
A.gsdb_org_code,
A.price_type_desc,
A.product_id,
A.product_price_start_date,
A.as_of_date_time,
A.batch_nbr,
A.currency_code,
A.prod_value_amt_ent,
A.update_date_time,
A.edl_stg_load_time,
A.edl_trans_load_time,
A.edl_ingest_channel,
A.edl_ingest_time
FROM(
select
dtl__capxtimestamp,
dtl__capxaction,
dtl__capxrowid,
gsdb_org_code,
price_type_desc,
product_id,
product_price_start_date,
as_of_date_time,
batch_nbr,
currency_code,
prod_value_amt_ent,
update_date_time,
edl_stg_load_time,
edl_trans_load_time,
edl_ingest_channel,
edl_ingest_time,
row_number() over (partition by gsdb_org_code ,price_type_desc ,product_id ,product_price_start_date
                   order by update_date_time desc) as rnk
FROM trans_sop_edw_pa_tedw.product_price) A
WHERE A.rnk = 1;