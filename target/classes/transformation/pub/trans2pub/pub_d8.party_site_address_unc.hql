use ${hivevar:targetSchema};

set hive.tez.container.size=${hivevar:containerSize};

insert OVERWRITE TABLE party_site_address_unc
SELECT
A.`dtl__capxtimestamp`,
A.`dtl__capxaction`,
A.`dtl__capxrowid`,
A.`party_site_addr_id`,
A.`instance_id`,
A.`address_line_1`,
A.`address_line_2`,
A.`address_line_3`,
A.`address_line_4`,
A.`as_of_date_time`,
A.`batch_nbr`,
A.`city_name`,
A.`country_code`,
A.`county_name`,
A.`created_by_name`,
A.`creation_date_time`,
A.`last_update_date_time`,
A.`last_update_login_name`,
A.`last_updated_by_name`,
A.`postal_code`,
A.`province_name`,
A.`state_name`,
A.`tran_code`,
A.`edl_stg_load_time`,
A.`edl_trans_load_time`,
A.`edl_rdw_load_time`,
A.`edl_record_source`,
A.`edl_rdw_last_applied_cdc_indicator`,
A.`edl_hub_last_applied_cdc_indicator`,
A.`edl_excp_handling_key`,
A.`edl_input_file_name`,
A.`edl_byte_offset`,
A.`edl_record_sequence`,
A.`edl_soft_delete_2`,
A.`edl_run_id`,
A.`edl_rdw_last_update_time`,
A.`edl_buss_key_hash`,
A.`edl_record_hash`,
A.`edl_ingest_channel`,
A.`edl_ingest_time`
FROM(
select
`dtl__capxtimestamp`,
`dtl__capxaction`,
`dtl__capxrowid`,
`party_site_addr_id`,
`instance_id`,
`address_line_1`,
`address_line_2`,
`address_line_3`,
`address_line_4`,
`as_of_date_time`,
`batch_nbr`,
`city_name`,
`country_code`,
`county_name`,
`created_by_name`,
`creation_date_time`,
`last_update_date_time`,
`last_update_login_name`,
`last_updated_by_name`,
`postal_code`,
`province_name`,
`state_name`,
`tran_code`,
`edl_stg_load_time`,
`edl_trans_load_time`,
`edl_rdw_load_time`,
`edl_record_source`,
`edl_rdw_last_applied_cdc_indicator`,
`edl_hub_last_applied_cdc_indicator`,
`edl_excp_handling_key`,
`edl_input_file_name`,
`edl_byte_offset`,
`edl_record_sequence`,
`edl_soft_delete_2`,
`edl_run_id`,
`edl_rdw_last_update_time`,
`edl_buss_key_hash`,
`edl_record_hash`,
`edl_ingest_channel`,
`edl_ingest_time`,
row_number() over (partition by party_site_addr_id ,instance_id
   order by last_update_date_time desc) as rnk
FROM trans_foundational_edw_gc_tedw.party_site_address_unc ) A
WHERE A.rnk = 1;