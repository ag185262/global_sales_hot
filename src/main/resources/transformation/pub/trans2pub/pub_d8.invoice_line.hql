use ${hivevar:targetSchema};

set hive.tez.container.size=${hivevar:containerSize};

insert OVERWRITE TABLE invoice_line
SELECT
A.dtl__capxtimestamp,
A.dtl__capxaction,
A.dtl__capxrowid,
A.invoice_line_id,
A.invoice_nbr,
A.source_country_code,
A.as_of_date,
A.as_of_time,
A.customer_trx_id,
A.etp_discount_percent,
A.fml_product_service_code,
A.inventory_item_id,
A.inventory_source_code,
A.invoice_line_nbr,
A.invoice_line_type,
A.invoiced_qty,
A.order_line_id,
A.order_nbr,
A.ordered_qty,
A.picking_line_id,
A.prev_invoice_line_id,
A.prev_invoice_nbr,
A.previous_customer_trx_id,
A.product_line_nbr,
A.product_type_code,
A.ps_project_nbr,
A.revenue_amt_entered,
A.revenue_amt_euro,
A.revenue_amt_func,
A.revenue_amt_us,
A.sales_order_nbr,
A.tax_exempt_nbr,
A.tax_rate,
A.acctg_rule_name,
A.acctg_rule_occurrences,
A.batch_nbr,
A.contract_nbr,
A.contract_nbr_modifier,
A.creation_date_time,
A.extended_amt_entered,
A.extended_amt_euro,
A.extended_amt_func,
A.extended_amt_us,
A.func_currency_code,
A.func_translation_rate,
A.instance_id,
A.invoice_begin_date,
A.invoice_end_date,
A.last_update_date_time,
A.link_to_invoice_line_id,
A.ncr_unit_id,
A.operating_unit_id,
A.oper_unit_country_code,
A.security_country_code,
A.service_line_nbr,
A.service_sub_line_nbr,
A.tax_code,
A.taxable_amt_entered,
A.taxable_amt_euro,
A.taxable_amt_func,
A.taxable_amt_us,
A.lgcy_invtry_item_id,
A.lgcy_invtry_source_code,
A.acctg_rule_id,
A.bill_instance_nbr,
A.invoice_line_desc,
A.item_desc,
A.service_date,
A.vat_tax_id,
A.list_price_amt_entered,
A.list_price_amt_func,
A.list_price_amt_us,
A.list_price_amt_euro,
A.selling_price_amt_entered,
A.selling_price_amt_func,
A.selling_price_amt_us,
A.selling_price_amt_euro,
A.sales_order_line_nbr,
A.cust_acct_site_id,
A.delivery_id,
A.gbl_attr_text_1,
A.gbl_attr_text_2,
A.gbl_attr_text_3,
A.gbl_attr_text_4,
A.gbl_attr_text_5,
A.gbl_attr_text_6,
A.gbl_attr_text_7,
A.gbl_attr_text_8,
A.gbl_attr_text_9,
A.gbl_attr_text_10,
A.gbl_attr_text_11,
A.gbl_attr_text_12,
A.gbl_attr_text_13,
A.gbl_attr_text_14,
A.gbl_attr_text_15,
A.gbl_attr_text_16,
A.gbl_attr_text_17,
A.gbl_attr_text_18,
A.gbl_attr_text_19,
A.gbl_attr_text_20,
A.gbl_attr_cat_code,
A.data_source_code,
A.for_use_at_site_use_id,
A.ship_to_site_use_id,
A.orig_invoice_nbr,
A.product_mdm_id,
A.solution_mdm_id,
A.package,
A.subscription_quote_nbr,
A.quote_line_number,
A.quote_line_tril_gid,
A.charge_line_number_immutable,
A.urr_flag,
A.interface_line_context,
A.invoice_indicator,
A.edl_stg_load_time,
A.edl_trans_load_time,
A.edl_ingest_channel,
A.edl_ingest_time
FROM(
SELECT
dtl__capxtimestamp,
dtl__capxaction,
dtl__capxrowid,
invoice_line_id,
invoice_nbr,
source_country_code,
as_of_date,
as_of_time,
customer_trx_id,
etp_discount_percent,
fml_product_service_code,
inventory_item_id,
inventory_source_code,
invoice_line_nbr,
invoice_line_type,
invoiced_qty,
order_line_id,
order_nbr,
ordered_qty,
picking_line_id,
prev_invoice_line_id,
prev_invoice_nbr,
previous_customer_trx_id,
product_line_nbr,
product_type_code,
ps_project_nbr,
revenue_amt_entered,
revenue_amt_euro,
revenue_amt_func,
revenue_amt_us,
sales_order_nbr,
tax_exempt_nbr,
tax_rate,
acctg_rule_name,
acctg_rule_occurrences,
batch_nbr,
contract_nbr,
contract_nbr_modifier,
creation_date_time,
extended_amt_entered,
extended_amt_euro,
extended_amt_func,
extended_amt_us,
func_currency_code,
func_translation_rate,
instance_id,
invoice_begin_date,
invoice_end_date,
last_update_date_time,
link_to_invoice_line_id,
ncr_unit_id,
operating_unit_id,
oper_unit_country_code,
security_country_code,
service_line_nbr,
service_sub_line_nbr,
tax_code,
taxable_amt_entered,
taxable_amt_euro,
taxable_amt_func,
taxable_amt_us,
lgcy_invtry_item_id,
lgcy_invtry_source_code,
acctg_rule_id,
bill_instance_nbr,
invoice_line_desc,
item_desc,
service_date,
vat_tax_id,
list_price_amt_entered,
list_price_amt_func,
list_price_amt_us,
list_price_amt_euro,
selling_price_amt_entered,
selling_price_amt_func,
selling_price_amt_us,
selling_price_amt_euro,
sales_order_line_nbr,
cust_acct_site_id,
delivery_id,
gbl_attr_text_1,
gbl_attr_text_2,
gbl_attr_text_3,
gbl_attr_text_4,
gbl_attr_text_5,
gbl_attr_text_6,
gbl_attr_text_7,
gbl_attr_text_8,
gbl_attr_text_9,
gbl_attr_text_10,
gbl_attr_text_11,
gbl_attr_text_12,
gbl_attr_text_13,
gbl_attr_text_14,
gbl_attr_text_15,
gbl_attr_text_16,
gbl_attr_text_17,
gbl_attr_text_18,
gbl_attr_text_19,
gbl_attr_text_20,
gbl_attr_cat_code,
data_source_code,
for_use_at_site_use_id,
ship_to_site_use_id,
orig_invoice_nbr,
product_mdm_id,
solution_mdm_id,
package,
subscription_quote_nbr,
quote_line_number,
quote_line_tril_gid,
charge_line_number_immutable,
urr_flag,
interface_line_context,
invoice_indicator,
edl_stg_load_time,
edl_trans_load_time,
edl_ingest_channel,
edl_ingest_time,
row_number() over (partition by invoice_line_id ,invoice_nbr ,source_country_code
                   order by concat(as_of_date,lpad(as_of_time,6,'0')) desc) as rnk
FROM trans_cfocontroller_edw_s3_tedw.invoice_line) A
WHERE A.rnk = 1;
