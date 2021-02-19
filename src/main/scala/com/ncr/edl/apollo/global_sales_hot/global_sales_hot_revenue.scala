package com.ncr.edl.apollo.global_sales_hot

import com.ncr.edl.apollo.common.ExecutorDao
import java.sql.Timestamp
import java.util.Date

class globalsaleshotrevenue(executor: ExecutorDao, dbParams: scala.collection.mutable.Map[String, String]) {

  def run(){
    var query = ""

    query = """ INSERT
	                OVERWRITE
					            TABLE
						              """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_tmp
                               SELECT
                                    customer_pnl.industry,
                                    customer_pnl.category,
                                    customer_pnl.rptg_sobp_cd,
                                    customer_pnl.source_type,
                                    customer_pnl.data_source_cd,
                                    customer_pnl.fml_org_cd,
                                    customer_pnl.hfm_custom3,
                                    customer_pnl.org_func_cd,
                                    customer_pnl.down2_dept_code_name,
                                    customer_pnl.down3_dept_code_name,
                                    customer_pnl.down4_dept_code_name,
                                    customer_pnl.down5_dept_code_name,
                                    customer_pnl.down6_dept_code_name,
                                    customer_pnl.down7_dept_code_name,
                                    customer_pnl.func_group_id,
                                    customer_pnl.prod_srvc_cd,
                                    customer_pnl.fml_acct_cd,
                                    customer_pnl.csub_acct_cd,
                                    customer_pnl.gl_code_combination_id,
                                    customer_pnl.asset_product_name,
                                    customer_pnl.asset_product_mdm_id,
                                    customer_pnl.master_cust_nbr,
                                    customer_pnl.master_cust_name,
                                    customer_pnl.mdm_product_family,
                                    customer_pnl.service_product_mdm_id,
                                    customer_pnl.product_family,
                                    customer_pnl.service_mdm_product_family,
                                    customer_pnl.service_product_name,
                                    customer_pnl.asset_mdm_product_family,
                                    customer_pnl.dataset,
                                    customer_pnl.fin_proj_id,
                                    customer_pnl.product_class ,
                                    customer_pnl.revenue,
                                    customer_pnl.accounting_yr_month
                               FROM
                                    (
                                    SELECT
                                        bi_customer_pnl.accounting_yr_month,
                                        bi_customer_pnl.industry,
                                        bi_customer_pnl.category,
                                        bi_customer_pnl.rptg_sobp_cd,
                                        bi_customer_pnl.source_type,
                                        bi_customer_pnl.data_source_cd,
                                        bi_customer_pnl.fml_org_cd,
                                        bi_customer_pnl.hfm_custom3,
                                        bi_customer_pnl.org_func_cd,
                                        bi_customer_pnl.down2_dept_code_name,
                                        bi_customer_pnl.down3_dept_code_name,
                                        bi_customer_pnl.down4_dept_code_name,
                                        bi_customer_pnl.down5_dept_code_name,
                                        bi_customer_pnl.down6_dept_code_name,
                                        bi_customer_pnl.down7_dept_code_name,
                                        bi_customer_pnl.func_group_id,
                                        bi_customer_pnl.prod_srvc_cd,
                                        bi_customer_pnl.fml_acct_cd,
                                        bi_customer_pnl.csub_acct_cd,
                                        bi_customer_pnl.gl_code_combination_id,
                                        bi_customer_pnl.asset_product_name,
                                        bi_customer_pnl.asset_product_mdm_id,
                                        bi_customer_pnl.master_cust_nbr,
                                        bi_customer_pnl.master_cust_name,
                                        bi_customer_pnl.mdm_product_family,
                                        bi_customer_pnl.service_product_mdm_id,
                                        bi_customer_pnl.product_family,
                                        bi_customer_pnl.service_mdm_product_family,
                                        bi_customer_pnl.service_product_name,
                                        bi_customer_pnl.asset_mdm_product_family,
                                        CASE
										                        WHEN bi_customer_pnl.source_type = 'ERP_COST' THEN 'cost'
										                        WHEN bi_customer_pnl.data_source_cd like '%COST%' THEN 'cost'
											                      WHEN bi_customer_pnl.data_source_cd like '%COS%' then 'cost'
											                      WHEN bi_customer_pnl.data_source_cd like '%Cost%' then 'cost'
											                      WHEN bi_customer_pnl.source_type = 'ERP_REV' then 'revenue'
											                      WHEN bi_customer_pnl.data_source_cd like '%REV%' then 'revenue'
										                      	WHEN bi_customer_pnl.source_type = 'HFM_ADJ' AND data_source_cd = 'HFM_ADJ' THEN 'revenue'
										                       	WHEN bi_customer_pnl.source_type = 'HFM_ADJ' AND data_source_cd like '%HILT%' THEN 'hilt'
										                      	WHEN bi_customer_pnl.source_type = 'FileValue' then 'filevalue'
										                      	ELSE bi_customer_pnl.source_type
									                      END as dataset,
                                        bi_customer_pnl.fin_proj_id,
                                        bi_customer_pnl.product_class,
                                        SUM(
                                           (
                                            CASE
											                         WHEN bi_customer_pnl.dist_amt_us is null THEN 0
												                    ELSE bi_customer_pnl.dist_amt_us END + CASE WHEN bi_customer_pnl.gl_adj_amt_us is null THEN 0 ELSE bi_customer_pnl.gl_adj_amt_us END + CASE when bi_customer_pnl.hfm_adj_amt_us is null THEN 0 ELSE (bi_customer_pnl.hfm_adj_amt_us)
										                       	END
                                            )
                                            ) as revenue
                                        FROM
                                            """ + dbParams("EEDDLLDDBB") + """.bi_customer_pnl bi_customer_pnl
                                        INNER JOIN (
                                            SELECT
                                                TRIM(m2gl.fml_account_code) fml_account_code
                                        FROM
                                            """ + dbParams("EEDDLLDDBB") + """.m2_gl_reporting_map_dn_drive m2gl
                                        WHERE
                                            m2gl.hyperion_rpt_line_code = 'CustomerRevenue'
                                        AND m2gl.hyperion_reporting_code = '4001'
                                        ) m2g
									                  ON  bi_customer_pnl.fml_acct_cd = m2g.fml_account_code
                                    WHERE
                                        (
                                        bi_customer_pnl.accounting_yr_month between 201901 AND 202009
                                    AND bi_customer_pnl.industry is not null
                                    AND (
                                        bi_customer_pnl.source_type = 'HFM_ADJ'
                                        OR (bi_customer_pnl.source_type != ('HFM_ADJ'))
                                        )
                                    AND bi_customer_pnl.category IN ('SWM')
                                      )
                                    GROUP BY
                                        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
                                    ) customer_pnl
                                WHERE
                                  customer_pnl.dataset in ('revenue', 'hilt') """
    executor.executeQuery(query)
  }
}

object globalsaleshotrevenueObjRunner{

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
    var executor = new ExecutorDao(activityCountRequired, "global_sales_hot_revenue")
    var dbParams = executor.loadProperties(propertyFilePath)
    var job_start_time = new Timestamp(new Date().getTime())
    println("################################################################")
    println("Started Job 'globalsaleshotrevenue' at %s.".format(job_start_time.toString()))
    var globalsaleshotrevenueObj = new globalsaleshotrevenue(executor, dbParams)
    try {
      globalsaleshotrevenueObj.run()
    } finally {
      var job_end_time = new Timestamp(new Date().getTime())
      println("Completed 'globalsaleshotrevenue' at %s.".format(job_end_time.toString()))
      println("Total Job Execution Time: %s".format(executor.getExecutionTime(job_start_time, job_end_time)))
      executor.postProcess()
      println("################################################################")
    }
  }
}
