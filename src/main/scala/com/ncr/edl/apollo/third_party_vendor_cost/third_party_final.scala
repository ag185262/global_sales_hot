package com.ncr.edl.apollo.third_party_vendor_cost

import com.ncr.edl.apollo.common.ExecutorDao
import java.sql.Timestamp
import java.util.Date

class thirdpartyvendorfinal(executor: ExecutorDao, dbParams: scala.collection.mutable.Map[String, String]) {

  def run(){
    var query = ""

    query = """
            INSERT
               OVERWRITE
                  TABLE
                     """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_final partition (accounting_yr_month)
                    select
abc.acctg_period_name
,abc.source_type
,abc.category
,abc.cust_ind_name
,abc.data_source_cd
,current_timestamp() as edl_load_ts
,abc.enterprise_nbr
,abc.enterprise_name
,abc.fml_acct_cd
,abc.fml_org_cd
,abc.func_group_id
,abc.gl_code_combination_id
,abc.hfm_custom3
,abc.industry
,abc.instance_id  -- null values
,abc.master_cust_nbr
,abc.master_cust_name
,abc.prod_srvc_cd
,abc.rptg_sobp_cd
,abc.oper_unit_country_code
,abc.base_tran_amt_func
,abc.base_tran_amt_us
,abc.gl_adj_amt_us -- null
,abc.gl_adj_amt_func -- null
,abc.hospitality_master_parent_id
,abc.hospitality_master_parent_name
,abc.fin_proj_id
,abc.org_func_cd
,abc.inter_entity_cd
,abc.csub_acct_cd
,abc.currency_cd
,abc.exchange_rate
,abc.mdm_product_family
,abc.product_mdm_id
,abc.product_class
,abc.hfm_adj_cost_amt_us
,abc.down2_dept_name
,abc.down2_dept_code
,abc.down2_dept_code_name
,abc.down3_dept_name
,abc.down3_dept_code
,abc.down3_dept_code_name
,abc.down4_dept_name
,abc.down4_dept_code
,abc.down4_dept_code_name
,abc.down5_dept_name
,abc.down5_dept_code
,abc.down5_dept_code_name
,abc.down6_dept_name
,abc.down6_dept_code
,abc.down6_dept_code_name
,abc.down7_dept_name
,abc.down7_dept_code
,abc.down7_dept_code_name
,abc.currency_cd_entered
,abc.total_cost   --Reverted
,abc.asset_product_mdm_id
,abc.service_product_mdm_id
,abc.alloc_cd
,abc.product_family
,abc.gl_posted_date
,abc.service_mdm_product_family
,abc.service_product_name
,abc.asset_mdm_product_family
,abc.asset_product_name
,abc.dataset
,abc.summarised_cost
,abc.customer_type
--,cost_details.cost_details
,abc.accounting_yr_month
from
(select
cost_details.acctg_period_name
,cost_details.source_type
,cost_details.category
,custmdm.customer_industry  as cust_ind_name
,'ABC_3rd_Party_Vendor_Cost' as data_source_cd
,current_timestamp() as edl_load_ts
,custmdm.enterprise_nbr  as enterprise_nbr
,custmdm.enterprise_name  as enterprise_name
,cost_details.fml_acct_cd
,cost_details.fml_org_cd
,ncrorg.functional_group_id as func_group_id
,cost_details.gl_code_combination_id
,cost_details.hfm_custom3
,cost_details.industry
,cost_details.instance_id  -- null values
,COALESCE(rev.master_cust_nbr,'-3') as master_cust_nbr
,COALESCE(custmdm.customer_name,'Unassigned')  as master_cust_name
,cost_details.prod_srvc_cd
,cost_details.rptg_sobp_cd
,custmdm.bus_oper_unit_country_code as oper_unit_country_code
,rev.cost as base_tran_amt_func
,rev.cost as base_tran_amt_us
,cost_details.gl_adj_amt_us -- null
,cost_details.gl_adj_amt_func -- null
,custmdm.hospitality_master_parent_id as hospitality_master_parent_id
,custmdm.hospitality_master_parent_name as hospitality_master_parent_name
,cost_details.fin_proj_id
,ncrorg.org_function_code as org_func_cd
,cost_details.inter_entity_cd
,cost_details.csub_acct_cd
,cost_details.currency_cd
,cost_details.exchange_rate
,cost_details.fin_product_family as mdm_product_family
,COALESCE(prodmdm.product_mdm_id,'-3') as product_mdm_id
,cost_details.product_class
,'NA' as hfm_adj_cost_amt_us
,cost_details.down2_dept_name
,cost_details.down2_dept_code
,cost_details.down2_dept_code_name
,cost_details.down3_dept_name
,cost_details.down3_dept_code
,cost_details.down3_dept_code_name
,cost_details.down4_dept_name
,cost_details.down4_dept_code
,cost_details.down4_dept_code_name
,cost_details.down5_dept_name
,cost_details.down5_dept_code
,cost_details.down5_dept_code_name
,cost_details.down6_dept_name
,cost_details.down6_dept_code
,cost_details.down6_dept_code_name
,cost_details.down7_dept_name
,cost_details.down7_dept_code
,cost_details.down7_dept_code_name
,'NA' as currency_cd_entered
,rev.cost as total_cost   --Reverted
,COALESCE(prodmdm.product_mdm_id,'-3') as asset_product_mdm_id
,COALESCE(cost_details.service_product_mdm_id,'-3') as service_product_mdm_id
,alloc.alloc_cd as alloc_cd
,cost_details.fin_product_family as product_family
,cost_details.gl_posted_date
,cost_details.service_mdm_product_family
,COALESCE(cost_details.service_product_name,'Unassigned') as service_product_name
,cost_details.fin_product_family as asset_mdm_product_family
,COALESCE(cost_details.prod_name,'Unassigned') as asset_product_name
,cost_details.dataset
,rev.total_cost as summarised_cost
,rev.customer_type as customer_type
--,cost_details.cost_details
,cost_details.accounting_yr_month
from """ + dbParams("EEDDLLDDBB") + """.tmp_in_cpnl_thirdpartyvendor_cost cost_details, (select alloc_cd from """ + dbParams("EEDDLLDDBB") + """.hub_bi_dim_alloc_type where alloc_name = 'CLOUD DI 3rd party vendor cost') alloc
left join """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue4a rev
on cost_details.prod_name=rev.prodname
and rev.prodname NOT IN ('Unassigned')
and cost_details.prod_name NOT IN ('Unassigned')
left join """ + dbParams("EEDDLLDDBB") + """.dim_customer_mdm custmdm
on custmdm.customer_nbr=rev.master_cust_nbr
left join """ + dbParams("EEDDLLDDBB") + """.dim_product_mdm prodmdm
on cost_details.prod_name=prodmdm.product_name
left join """ + dbParams("EEDDLLDDBB") + """.ncr_organization ncrorg
on cost_details.fml_org_cd=ncrorg.fml_organization_code
union
select
cost_details.acctg_period_name
,cost_details.source_type
,cost_details.category
,custmdm.customer_industry  as cust_ind_name  
,'ABC_3rd_Party_Vendor_Cost' as data_source_cd
,current_timestamp() as edl_load_ts
,custmdm.enterprise_nbr  as enterprise_nbr  
,custmdm.enterprise_name  as enterprise_name 
,unassign.fml_acct_cd
,unassign.fml_org_cd
,ncrorg.functional_group_id as func_group_id
,cost_details.gl_code_combination_id
,cost_details.hfm_custom3
,cost_details.industry
,cost_details.instance_id  -- null values
,COALESCE(rev.master_cust_nbr,'-3') as master_cust_nbr                             
,COALESCE(custmdm.customer_name,'Unassigned') as master_cust_name                  
,unassign.prod_srvc_cd
,unassign.rptg_sobp_cd
,custmdm.bus_oper_unit_country_code as oper_unit_country_code   
,rev.cost as base_tran_amt_func                           
,rev.cost as base_tran_amt_us                             
,cost_details.gl_adj_amt_us -- null
,cost_details.gl_adj_amt_func -- null
,custmdm.hospitality_master_parent_id as hospitality_master_parent_id                  
,custmdm.hospitality_master_parent_name as hospitality_master_parent_name            
,unassign.fin_proj_id
,ncrorg.org_function_code as org_func_cd
,unassign.inter_entity_cd
,unassign.csub_acct_cd
,cost_details.currency_cd
,cost_details.exchange_rate
,cost_details.fin_product_family as mdm_product_family
,COALESCE(prodmdm.product_mdm_id,'-3') as product_mdm_id
,cost_details.product_class
,'NA' as hfm_adj_cost_amt_us
,cost_details.down2_dept_name
,cost_details.down2_dept_code
,cost_details.down2_dept_code_name
,cost_details.down3_dept_name
,cost_details.down3_dept_code
,cost_details.down3_dept_code_name
,cost_details.down4_dept_name
,cost_details.down4_dept_code
,cost_details.down4_dept_code_name
,cost_details.down5_dept_name
,cost_details.down5_dept_code
,cost_details.down5_dept_code_name
,cost_details.down6_dept_name
,cost_details.down6_dept_code
,cost_details.down6_dept_code_name
,cost_details.down7_dept_name
,cost_details.down7_dept_code
,cost_details.down7_dept_code_name
,'NA' as currency_cd_entered
,(unassign.total_cost*rev.revenue_ratio) as total_cost   --Reverted
,COALESCE(prodmdm.product_mdm_id,'-3') as asset_product_mdm_id
,COALESCE(cost_details.service_product_mdm_id,'-3') as service_product_mdm_id
,alloc.alloc_cd as alloc_cd
,cost_details.fin_product_family as product_family
,cost_details.gl_posted_date
,cost_details.service_mdm_product_family
,COALESCE(cost_details.service_product_name,'Unassigned') as service_product_name
,cost_details.fin_product_family as asset_mdm_product_family
,COALESCE(cost_details.prod_name,'Unassigned') as asset_product_name
,cost_details.dataset
,unassign.total_cost as summarised_cost
,'NA-Unassigned cost' as customer_type
--,cost_details.cost_details
,unassign.accounting_yr_month
from """ + dbParams("EEDDLLDDBB") + """.tmp_in_cpnl_thirdpartyvendor_cost cost_details, (select alloc_cd from """ + dbParams("EEDDLLDDBB") + """.hub_bi_dim_alloc_type where alloc_name = 'CLOUD DI 3rd party vendor cost') alloc
right join """ + dbParams("EEDDLLDDBB") + """.tmp_unassigned_cost_alloc unassign
on cost_details.prod_name=unassign.prod_name
and cost_details.prod_name NOT IN ('Unassigned')
left join """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue4a rev
on cost_details.prod_name=rev.prodname
and rev.prodname NOT IN ('Unassigned')
left join """ + dbParams("EEDDLLDDBB") + """.dim_customer_mdm custmdm
on custmdm.customer_nbr=rev.master_cust_nbr
left join """ + dbParams("EEDDLLDDBB") + """.dim_product_mdm prodmdm
on cost_details.prod_name=prodmdm.product_name
left join """ + dbParams("EEDDLLDDBB") + """.ncr_organization ncrorg
on cost_details.fml_org_cd=ncrorg.fml_organization_code
)abc """
    executor.executeQuery(query)

  }
}

object thirdpartyvendorfinalObjRunner{

  def main(args: Array[String]){
    if (args.length < 1) {
      println("Property file path not provided.")
      sys.exit(1)
    }
    var propertyFilePath = args(0)
    var activityCountRequired = false
    //if(args.length > 1){
    //activityCountRequired = args(1).toBoolean
    //}

    var executor = new ExecutorDao(activityCountRequired, "third_party_final")
    var dbParams = executor.loadProperties(propertyFilePath)
    var job_start_time = new Timestamp(new Date().getTime())
    println("################################################################")
    println("Started Job 'thirdpartyvendorfinal' at %s.".format(job_start_time.toString()))
    var thirdpartyvendorfinalObj = new thirdpartyvendorfinal(executor, dbParams)
    try {
      thirdpartyvendorfinalObj.run()
    } finally {
      var job_end_time = new Timestamp(new Date().getTime())
      println("Completed 'thirdpartyvendorfinal' at %s.".format(job_end_time.toString()))
      println("Total Job Execution Time: %s".format(executor.getExecutionTime(job_start_time, job_end_time)))
      executor.postProcess()
      println("################################################################")
    }
  }
}
