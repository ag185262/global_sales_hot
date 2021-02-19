package com.ncr.edl.apollo.global_sales_hot

import com.ncr.edl.apollo.common.ExecutorDao
import java.sql.Timestamp
import java.util.Date

class globalsaleshotfinalallocation(executor: ExecutorDao, dbParams: scala.collection.mutable.Map[String, String]) {

  def run(){
    var query = ""

    query = """ INSERT
	                  OVERWRITE
					               TABLE
								              """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_final_temp1
	                                 SELECT
                                        final_load.acctg_period_name ,             
                                        final_load.source_type       ,             
                                        final_load.category          ,             
                                        final_load.cust_ind_name     ,             
                                        final_load.data_source_cd    ,             
                                        final_load.edl_load_ts       ,
                                        final_load.enterprise_nbr    ,             
                                        final_load.enterprise_name   ,             
                                        final_load.fml_acct_cd       ,             
                                        final_load.fml_org_cd        ,             
                                        final_load.func_group_id     ,             
                                        final_load.gl_code_combination_id ,        
                                        final_load.hfm_custom3            ,        
                                        final_load.industry               ,        
                                        final_load.instance_id            ,        
                                        final_load.master_cust_nbr        ,        
                                        final_load.master_cust_name       ,        
                                        final_load.prod_srvc_cd           ,        
                                        final_load.rptg_sobp_cd           ,        
                                        final_load.oper_unit_country_code ,        
                                        final_load.total_cost*exchange_rate as base_tran_amt_func,
                                        final_load.total_cost as base_tran_amt_us ,          
                                        final_load.gl_adj_amt_us                  ,
                                        final_load.gl_adj_amt_func                ,
                                        final_load.hospitality_master_parent_id   ,
                                        final_load.hospitality_master_parent_name ,
                                        final_load.fin_proj_id      ,              
                                        final_load.org_func_cd      ,              
                                        final_load.inter_entity_cd  ,              
                                        final_load.csub_acct_cd     ,              
                                        final_load.currency_cd      ,              
                                        final_load.exchange_rate    ,              
                                        final_load.mdm_product_family  ,           
                                        final_load.product_mdm_id      ,           
                                        final_load.product_class       ,           
                                        final_load.hfm_adj_cost_amt_us ,           
                                        final_load.down2_dept_name     ,           
                                        final_load.down2_dept_code     ,           
                                        final_load.down2_dept_code_name,           
                                        final_load.down3_dept_name     ,           
                                        final_load.down3_dept_code     ,           
                                        final_load.down3_dept_code_name,           
                                        final_load.down4_dept_name     ,           
                                        final_load.down4_dept_code     ,           
                                        final_load.down4_dept_code_name,           
                                        final_load.down5_dept_name     ,           
                                        final_load.down5_dept_code     ,           
                                        final_load.down5_dept_code_name,           
                                        final_load.down6_dept_name     ,           
                                        final_load.down6_dept_code     ,           
                                        final_load.down6_dept_code_name,           
                                        final_load.down7_dept_name     ,           
                                        final_load.down7_dept_code     ,           
                                        final_load.down7_dept_code_name,           
                                        final_load.currency_cd_entered ,           
                                        final_load.total_cost          ,           
                                        final_load.asset_product_mdm_id,           
                                        final_load.service_product_mdm_id ,        
                                        final_load.alloc_cd               ,        
                                        final_load.product_family         ,        
                                        final_load.gl_posted_date         ,        
                                        final_load.service_mdm_product_family,     
                                        final_load.service_product_name      ,     
                                        final_load.asset_mdm_product_family  ,     
                                        final_load.asset_product_name        ,
                                        final_load.sub_region_name,
                                        final_load.country_cd,
                                        final_load.country_name,
                                        final_load.summarised_cost ,                                
                                        final_load.accounting_yr_month
                                   FROM (
                                        SELECT
                                            allocation.acctg_period_name,
                                            allocation.source_type      ,
                                            allocation.category         ,
                                            custmdm.industry_code  as cust_ind_name     ,
                                            'ABC_Global_Sales_Hot_Cost' as data_source_cd ,
                                            current_timestamp() as edl_load_ts          ,
                                            custmdm.enterprise_nbr  as enterprise_nbr   ,              
                                            custmdm.enterprise_name  as enterprise_name ,
                                            allocation.fml_acct_cd                      ,
                                            allocation.fml_org_cd                       ,
                                            ncrorg.functional_group_id as func_group_id ,
                                            cast(allocation.gl_code_combination_id as bigint) as gl_code_combination_id            ,
                                            allocation.hfm_custom3                      ,
                                            allocation.industry as industry           ,
                                            allocation.instance_id                      ,
                                            allocation.master_number  as master_cust_nbr,                
                                            custmdm.customer_name  as master_cust_name  ,
                                            allocation.prod_srvc_cd                     ,
                                            allocation.rptg_sobp_cd                     ,
                                            custmdm.bus_oper_unit_country_code as oper_unit_country_code, 
                                            cast(null as bigint) as gl_adj_amt_us ,
                                            cast(null as bigint) as gl_adj_amt_func ,
                                            custmdm.hospitality_master_parent_id as hospitality_master_parent_id   ,
                                            custmdm.hospitality_master_parent_name as hospitality_master_parent_name ,
                                            allocation.fin_proj_id,
                                            ncrorg.org_function_code as org_func_cd,
                                            allocation.inter_entity_cd,
                                            allocation.csub_acct_cd   ,
                                            allocation.currency_cd    ,                                        
                                            CASE 
                                               WHEN allocation.currency_cd='USD' THEN 1.0
                                               ELSE tr.average_rate 
                                            END as exchange_rate,                
                                            'Unassigned' as mdm_product_family ,           
                                            -3 as product_mdm_id               ,  
                                            allocation.product_class      ,
                                            allocation.hfm_adj_cost_amt_us     ,
                                            allocation.down2_dept_name         ,      
                                            allocation.down2_dept_code         ,      
                                            allocation.down2_dept_code_name    ,      
                                            allocation.down3_dept_name         ,      
                                            allocation.down3_dept_code         ,      
                                            allocation.down3_dept_code_name    ,      
                                            allocation.down4_dept_name         ,      
                                            allocation.down4_dept_code         ,      
                                            allocation.down4_dept_code_name    ,      
                                            allocation.down5_dept_name         ,      
                                            allocation.down5_dept_code         ,      
                                            allocation.down5_dept_code_name    ,      
                                            allocation.down6_dept_name         ,      
                                            allocation.down6_dept_code         ,      
                                            allocation.down6_dept_code_name    ,      
                                            allocation.down7_dept_name         ,      
                                            allocation.down7_dept_code         ,      
                                            allocation.down7_dept_code_name    ,
                                            allocation.currency_cd_entered     ,
                                            allocation.allocated_cost_prdct_wise as total_cost ,                    
                                            asset_product_mdm_id         ,  
                                            -3 as service_product_mdm_id ,       
                                            alloc.alloc_cd as alloc_cd   ,                    
                                            allocation.product_family    ,
                                            allocation.gl_posted_date    ,
                                            'Unassigned' as service_mdm_product_family,     
                                            'Unassigned' as service_product_name      ,   
                                            allocation.asset_mdm_product_family       ,
                                            allocation.asset_product_name             ,
                                            allocation.sub_region_name                ,
                                            allocation.country_cd                     ,
                                            allocation.country_name                   ,
                                            allocation.summarized_cost as summarised_cost ,                  
                                            allocation.accounting_yr_month
                                        FROM(
                                            SELECT
                                                master_number         ,
                                                total_alloc_cost as summarized_cost,
                                                fml_org_cd     ,           
                                                fml_acct_cd    ,           
                                                csub_acct_cd   ,           
                                                prod_srvc_cd   ,           
                                                rptg_sobp_cd   ,           
                                                fin_proj_id    ,           
                                                source_type    ,           
                                                category       ,           
                                                gl_code_combination_id,    
                                                hfm_custom3           ,    
                                                instance_id           ,    
                                                inter_entity_cd       ,    
                                                currency_cd           ,               
                                                hfm_adj_cost_amt_us   ,    
                                                acctg_period_name     ,    
                                                down2_dept_name       ,    
                                                down2_dept_code       ,    
                                                down2_dept_code_name  ,    
                                                down3_dept_name       ,    
                                                down3_dept_code       ,    
                                                down3_dept_code_name  ,    
                                                down4_dept_name       ,    
                                                down4_dept_code       ,    
                                                down4_dept_code_name  ,    
                                                down5_dept_name       ,    
                                                down5_dept_code       ,    
                                                down5_dept_code_name  ,    
                                                down6_dept_name     ,      
                                                down6_dept_code     ,      
                                                down6_dept_code_name,      
                                                down7_dept_name     ,      
                                                down7_dept_code     ,      
                                                down7_dept_code_name,      
                                                dataset             ,
                                                sub_region_name     ,
                                                country_cd          ,
                                                country_name        ,
                                                currency_cd_entered ,      
                                                gl_posted_date      ,      
                                                accounting_yr_month ,                
                                                allocated_cost_prdct_wise,
                                                asset_product_name       ,             
                                                product_family           , 
                                                asset_mdm_product_family ,
                                                asset_product_mdm_id ,
                                                industry,
                                                product_class
                                            FROM 
				                    	                  """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_final_1

                                            UNION

                                            SELECT
                                                master_number  ,
                                                total_alloc_cost as summarized_cost,
                                                fml_org_cd   ,
                                                fml_acct_cd  ,
                                                csub_acct_cd ,
                                                prod_srvc_cd ,
                                                rptg_sobp_cd ,
                                                fin_proj_id  ,
                                                source_type  ,
                                                category     ,
                                                gl_code_combination_id ,   
                                                hfm_custom3      ,         
                                                instance_id      ,         
                                                inter_entity_cd  ,         
                                                currency_cd      ,                      
                                                hfm_adj_cost_amt_us,       
                                                acctg_period_name  ,       
                                                down2_dept_name    ,       
                                                down2_dept_code    ,       
                                                down2_dept_code_name,      
                                                down3_dept_name     ,      
                                                down3_dept_code     ,      
                                                down3_dept_code_name,      
                                                down4_dept_name     ,      
                                                down4_dept_code     ,      
                                                down4_dept_code_name,      
                                                down5_dept_name     ,      
                                                down5_dept_code     ,      
                                                down5_dept_code_name,      
                                                down6_dept_name     ,      
                                                down6_dept_code     ,      
                                                down6_dept_code_name,      
                                                down7_dept_name     ,      
                                                down7_dept_code     ,      
                                                down7_dept_code_name,      
                                                dataset             ,
                                                sub_region_name     ,
                                                country_cd          ,
                                                country_name        ,
                                                currency_cd_entered ,      
                                                gl_posted_date      ,      
                                                accounting_yr_month ,                
                                                total_alloc_cost as allocated_cost_prdct_wise,
                                                product_name as asset_product_name ,                   
                                                'Unassigned' as product_family ,
                                                pids.product_family as asset_mdm_product_family,
                                                main.product_mdm_id as asset_product_mdm_id,
                                                industry,
                                                null as product_class
                                            from
                                                """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_final_5 main
                                            LEFT JOIN
                                                (select
                                                    distinct product_mdm_id,
                                                    product_family
                                                from
                                                    """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_PID_step1) pids
                                                on  main.product_mdm_id=pids.product_mdm_id
                                                ) allocation,
                                                (SELECT  alloc_cd FROM """ + dbParams("EEDDLLDDBB") + """.hub_bi_dim_alloc_type WHERE alloc_name = 'SWM Global Sales Hot' ) alloc
                                            LEFT JOIN 
				                    	                  (select distinct accounting_period_name,translate_from_curr_code,average_rate  FROM """ + dbParams("EEDDLLDDBB") + """.gl_translation_rates_ht a where trim(currency_code)='USD') tr
                                            ON 
				                    	                  allocation.acctg_period_name=tr.accounting_period_name
                                            AND 
				                    	                  allocation.currency_cd=trim(tr.translate_from_curr_code)
                                            LEFT JOIN 
				                    	                  """ + dbParams("EEDDLLDDBB") + """.dim_customer_mdm custmdm
                                            ON 
				                    	                  custmdm.customer_nbr=allocation.master_number
                                            LEFT JOIN 
				                    	                  """ + dbParams("EEDDLLDDBB") + """.ncr_organization ncrorg
                                            ON 
				                    	                  allocation.fml_org_cd=ncrorg.fml_organization_code
                                            ) final_load """
    executor.executeQuery(query)
  }
}

object globalsaleshotfinalallocationObjRunner{

  def main(args: Array[String]){
    if (args.length < 1) {
      println("Property file path not provided.")
      sys.exit(1)
    }
    var propertyFilePath = args(0)
    var activityCountRequired = false
    //if(args.length > 1){
    //    activityCountRequired = args(1).toBoolean
    //}

    var executor = new ExecutorDao(activityCountRequired, "global_sales_hot_final_allocation")
    var dbParams = executor.loadProperties(propertyFilePath)
    var job_start_time = new Timestamp(new Date().getTime())
    println("################################################################")
    println("Started Job 'globalsaleshotfinalallocation' at %s.".format(job_start_time.toString()))
    var globalsaleshotfinalallocationObj = new globalsaleshotfinalallocation(executor, dbParams)
    try {
      globalsaleshotfinalallocationObj.run()
    } finally {
      var job_end_time = new Timestamp(new Date().getTime())
      println("Completed 'globalsaleshotfinalallocation' at %s.".format(job_end_time.toString()))
      println("Total Job Execution Time: %s".format(executor.getExecutionTime(job_start_time, job_end_time)))
      executor.postProcess()
      println("################################################################")
    }
  }
}
