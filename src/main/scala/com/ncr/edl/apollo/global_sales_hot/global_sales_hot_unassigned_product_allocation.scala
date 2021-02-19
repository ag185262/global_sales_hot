package com.ncr.edl.apollo.global_sales_hot

import com.ncr.edl.apollo.common.ExecutorDao
import java.sql.Timestamp
import java.util.Date

class globalsaleshotunassignedproductallocation(executor: ExecutorDao, dbParams: scala.collection.mutable.Map[String, String]) {

  def run(){
    var query = ""	

    query = """ with 
	            non_revenue 
				as (
                select
                  cost.*
                from(
                    select
                      *
                    from
                      """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step3
                  ) cost
                  left join (
                    select
                      distinct master_number
                    from
                      """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_final_1
                  ) rev on cost.master_number = rev.master_number
                where
                  rev.master_number is null
                )
                insert 
				            overwrite
					              table
						            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step9
                            select
                              inc_data.*,
                              non_rev.total_alloc_cost as non_allocated_cost
                            from
                              (
                                select
                                  *
                                from
                                  """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_final_2
                              ) inc_data
                              inner join
                                  non_revenue non_rev
                              on  inc_data.master_number = non_rev.master_number
                              and inc_data.fml_org_cd = non_rev.fml_org_cd
                              and inc_data.fml_acct_cd = non_rev.fml_acct_cd
                              and inc_data.csub_acct_cd = non_rev.csub_acct_cd
                              and inc_data.prod_srvc_cd = non_rev.prod_srvc_cd
                              and inc_data.rptg_sobp_cd = non_rev.rptg_sobp_cd
                              and inc_data.fin_proj_id = non_rev.fin_proj_id
                              and inc_data.source_type = non_rev.source_type
                              and inc_data.accounting_yr_month = non_rev.accounting_yr_month
                              and inc_data.currency_cd = non_rev.currency_cd
                              and inc_data.industry = non_rev.industry """
    executor.executeQuery(query)
	
	query = """ with 
	            total_cost 
				      as (
                  select
                      master_number,
                      fml_org_cd,
                      fml_acct_cd,
                      csub_acct_cd,
                      prod_srvc_cd,
                      rptg_sobp_cd,
                      fin_proj_id,
                      source_type,
                      accounting_yr_month,
                      currency_cd,
                      sum(total_alloc_cost) as total_cost_for_allocation
                  from
                      """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step9
                  group by
                      master_number,
                      fml_org_cd,
                      fml_acct_cd,
                      csub_acct_cd,
                      prod_srvc_cd,
                      rptg_sobp_cd,
                      fin_proj_id,
                      source_type,
                      accounting_yr_month,
                      currency_cd
                    ) 
              insert
				          overwrite
					             table
					          	 """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step10
                            select
                              a.total_alloc_cost + a.cost_ratio * non_allocated_cost as total_alloc_cost_1,
                              a.*
                            from
                              (
                                select
                                  inc_data.total_alloc_cost / total_cost.total_cost_for_allocation as cost_ratio,
                                  inc_data.*
                                from
                                  """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step9 inc_data
                                  inner join
                                  total_cost
                                  on  inc_data.master_number = total_cost.master_number
                                  and inc_data.fml_org_cd = total_cost.fml_org_cd
                                  and inc_data.fml_acct_cd = total_cost.fml_acct_cd
                                  and inc_data.csub_acct_cd = total_cost.csub_acct_cd
                                  and inc_data.prod_srvc_cd = total_cost.prod_srvc_cd
                                  and inc_data.rptg_sobp_cd = total_cost.rptg_sobp_cd
                                  and inc_data.fin_proj_id = total_cost.fin_proj_id
                                  and inc_data.source_type = total_cost.source_type
                                  and inc_data.accounting_yr_month = total_cost.accounting_yr_month
                                  and inc_data.currency_cd = total_cost.currency_cd
                              ) a """
    executor.executeQuery(query)
	
	query = """ with 
	            cust_list 
				      as (
                    select
                      distinct master_number
                    from
                      """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step10
                    ) 
              insert
				           overwrite
      					   table
						            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_final_5
                            select
                                inc_data.master_number,
                                incident_count,
                                incident_ratio,
                                total_incident,
                                product_name,
                                product_mdm_id,
                                total_alloc_cost,
                                fml_cost_1,
                                fml_org_cd,
                                fml_acct_cd,
                                csub_acct_cd,
                                prod_srvc_cd,
                                rptg_sobp_cd,
                                fin_proj_id,
                                source_type,
                                category,
                                gl_code_combination_id,
                                hfm_custom3,
                                instance_id,
                                inter_entity_cd,
                                currency_cd,
                                exchange_rate,
                                hfm_adj_cost_amt_us,
                                acctg_period_name,
                                industry,
                                down2_dept_name,
                                down2_dept_code,
                                down2_dept_code_name,
                                down3_dept_name,
                                down3_dept_code,
                                down3_dept_code_name,
                                down4_dept_name,
                                down4_dept_code,
                                down4_dept_code_name,
                                down5_dept_name,
                                down5_dept_code,
                                down5_dept_code_name,
                                down6_dept_name,
                                down6_dept_code,
                                down6_dept_code_name,
                                down7_dept_name,
                                down7_dept_code,
                                down7_dept_code_name,
                                dataset,
                                sub_region_name,
                                country_cd,
                                country_name,
                                currency_cd_entered,
                                gl_posted_date,
                                accounting_yr_month,
                                fml_cost
                            from
                                """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_final_2 inc_data
                                left join cust_list on inc_data.master_number = cust_list.master_number
                            where
                                 cust_list.master_number is null
                            union
                            select
                                master_number,
                                incident_count,
                                incident_ratio,
                                total_incident,
                                product_name,
                                product_mdm_id,
                                total_alloc_cost_1 as total_alloc_cost,
                                fml_cost_1,
                                fml_org_cd,
                                fml_acct_cd,
                                csub_acct_cd,
                                prod_srvc_cd,
                                rptg_sobp_cd,
                                fin_proj_id,
                                source_type,
                                category,
                                gl_code_combination_id,
                                hfm_custom3,
                                instance_id,
                                inter_entity_cd,
                                currency_cd,
                                exchange_rate,
                                hfm_adj_cost_amt_us,
                                acctg_period_name,
                                industry,
                                down2_dept_name,
                                down2_dept_code,
                                down2_dept_code_name,
                                down3_dept_name,
                                down3_dept_code,
                                down3_dept_code_name,
                                down4_dept_name,
                                down4_dept_code,
                                down4_dept_code_name,
                                down5_dept_name,
                                down5_dept_code,
                                down5_dept_code_name,
                                down6_dept_name,
                                down6_dept_code,
                                down6_dept_code_name,
                                down7_dept_name,
                                down7_dept_code,
                                down7_dept_code_name,
                                dataset,
                                sub_region_name,
                                country_cd,
                                country_name,
                                currency_cd_entered,
                                gl_posted_date,
                                accounting_yr_month,
                                fml_cost
                            from
                                """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step10 """
    executor.executeQuery(query)

  }
}

object globalsaleshotunassignedproductallocationObjRunner{

  def main(args: Array[String]){
    if (args.length < 1) {
      println("Property file path not provided.")
      sys.exit(1)
    }
    var propertyFilePath = args(0)
    var activityCountRequired = false
    //   if(args.length > 1){
    //     activityCountRequired = args(1).toBoolean
    //  }
    var executor = new ExecutorDao(activityCountRequired, "global_sales_hot_unassigned_product_allocation")
    var dbParams = executor.loadProperties(propertyFilePath)
    var job_start_time = new Timestamp(new Date().getTime())
    println("################################################################")
    println("Started Job 'globalsaleshotunassignedproductallocation' at %s.".format(job_start_time.toString()))
    var globalsaleshotunassignedproductallocationObj = new globalsaleshotunassignedproductallocation(executor, dbParams)
    try {
      globalsaleshotunassignedproductallocationObj.run()
    } finally {
      var job_end_time = new Timestamp(new Date().getTime())
      println("Completed 'laborincidentunassignedproductallocation' at %s.".format(job_end_time.toString()))
      println("Total Job Execution Time: %s".format(executor.getExecutionTime(job_start_time, job_end_time)))
      executor.postProcess()
      println("################################################################")
    }
  }
}
