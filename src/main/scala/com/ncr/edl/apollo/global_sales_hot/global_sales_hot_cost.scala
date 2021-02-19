package com.ncr.edl.apollo.global_sales_hot

import com.ncr.edl.apollo.common.ExecutorDao
import java.sql.Timestamp
import java.util.Date

class globalsaleshotcost(executor: ExecutorDao, dbParams: scala.collection.mutable.Map[String, String]) {

  def run(){
    var query = ""

    query = """ INSERT
	                OVERWRITE TABLE """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_with_pov partition (accounting_yr_month)
                       SELECT
                           customer_pnl.acctg_period_name,
                           customer_pnl.source_type,
                           customer_pnl.category,
                           customer_pnl.fml_acct_cd,
                           customer_pnl.fml_org_cd,
                           customer_pnl.gl_code_combination_id,
                           customer_pnl.hfm_custom3,
                           customer_pnl.industry,
                           customer_pnl.instance_id,
                           customer_pnl.prod_srvc_cd,
                           customer_pnl.rptg_sobp_cd,
                           customer_pnl.data_source_cd,
                           customer_pnl.func_group_id,
                           customer_pnl.gl_adj_amt_us,
                           customer_pnl.gl_adj_amt_func,
                           customer_pnl.fin_proj_id,
                           customer_pnl.inter_entity_cd,
                           customer_pnl.csub_acct_cd,
                           customer_pnl.down2_dept_name,
                           customer_pnl.down2_dept_code,
                           customer_pnl.down2_dept_code_name,
                           customer_pnl.down3_dept_name,
                           customer_pnl.down3_dept_code,
                           customer_pnl.down3_dept_code_name,
                           customer_pnl.down4_dept_name,
                           customer_pnl.down4_dept_code,
                           customer_pnl.down4_dept_code_name,
                           customer_pnl.down5_dept_name,
                           customer_pnl.down5_dept_code,
                           customer_pnl.down5_dept_code_name,
                           customer_pnl.down6_dept_name,
                           customer_pnl.down6_dept_code,
                           customer_pnl.down6_dept_code_name,
                           customer_pnl.down7_dept_name,
                           customer_pnl.down7_dept_code,
                           customer_pnl.down7_dept_code_name,
                           customer_pnl.gl_posted_date,
                           customer_pnl.asset_product_name,
                           customer_pnl.base_tran_amt_func,
                           customer_pnl.base_tran_amt_us,
                           customer_pnl.currency_cd,
                           customer_pnl.exchange_rate,
                           customer_pnl.hfm_adj_cost_amt_us,
                           customer_pnl.currency_cd_entered,
                           customer_pnl.service_product_mdm_id,
                           customer_pnl.service_mdm_product_family,
                           customer_pnl.service_product_name,
                           customer_pnl.dataset,
                           customer_pnl.sub_region_name,
                           customer_pnl.country_cd,
                           customer_pnl.country_name,
                           customer_pnl.cost,
                           customer_pnl.accounting_yr_month
                       FROM
                          (
                           SELECT
                               bi_customer_pnl.acctg_period_name,
                               bi_customer_pnl.accounting_yr_month,
                               bi_customer_pnl.source_type,
                               bi_customer_pnl.category,
                               bi_customer_pnl.fml_acct_cd,
                               bi_customer_pnl.fml_org_cd,
                               bi_customer_pnl.gl_code_combination_id,
                               bi_customer_pnl.hfm_custom3,
                               bi_customer_pnl.industry,
                               bi_customer_pnl.instance_id,
                               bi_customer_pnl.prod_srvc_cd,
                               bi_customer_pnl.rptg_sobp_cd,
                               bi_customer_pnl.data_source_cd,
                               bi_customer_pnl.func_group_id,
                               bi_customer_pnl.gl_adj_amt_us,
                               bi_customer_pnl.gl_adj_amt_func,
                               bi_customer_pnl.fin_proj_id,
                               bi_customer_pnl.inter_entity_cd,
                               bi_customer_pnl.csub_acct_cd,
                               bi_customer_pnl.down2_dept_name,
                               bi_customer_pnl.down2_dept_code,
                               bi_customer_pnl.down2_dept_code_name,
                               bi_customer_pnl.down3_dept_name,
                               bi_customer_pnl.down3_dept_code,
                               bi_customer_pnl.down3_dept_code_name,
                               bi_customer_pnl.down4_dept_name,
                               bi_customer_pnl.down4_dept_code,
                               bi_customer_pnl.down4_dept_code_name,
                               bi_customer_pnl.down5_dept_name,
                               bi_customer_pnl.down5_dept_code,
                               bi_customer_pnl.down5_dept_code_name,
                               bi_customer_pnl.down6_dept_name,
                               bi_customer_pnl.down6_dept_code,
                               bi_customer_pnl.down6_dept_code_name,
                               bi_customer_pnl.down7_dept_name,
                               bi_customer_pnl.down7_dept_code,
                               bi_customer_pnl.down7_dept_code_name,
                               bi_customer_pnl.gl_posted_date,
                               bi_customer_pnl.asset_product_name,
                               bi_customer_pnl.base_tran_amt_func,
                               bi_customer_pnl.base_tran_amt_us,
                               bi_customer_pnl.currency_cd,
                               bi_customer_pnl.exchange_rate,
                               bi_customer_pnl.hfm_adj_cost_amt_us,
                               bi_customer_pnl.currency_cd_entered,
                               bi_customer_pnl.service_product_mdm_id,
                               bi_customer_pnl.service_mdm_product_family,
                               bi_customer_pnl.service_product_name,
                               CASE
                                   WHEN bi_customer_pnl.source_type = 'ERP_COST' THEN 'cost'
                                   WHEN bi_customer_pnl.data_source_cd like '%COST%' THEN 'cost'
                                   WHEN bi_customer_pnl.data_source_cd like '%COS%' THEN 'cost'
                                   WHEN bi_customer_pnl.data_source_cd like '%Cost%' THEN 'cost' when bi_customer_pnl.source_type = 'ERP_REV' THEN 'revenue' when bi_customer_pnl.data_source_cd like '%REV%' THEN 'revenue' when bi_customer_pnl.source_type = 'HFM_ADJ'
                                   AND data_source_cd = 'HFM_ADJ' THEN 'revenue' when bi_customer_pnl.source_type = 'HFM_ADJ'
                                   AND data_source_cd like '%HILT%' THEN 'hilt' when bi_customer_pnl.source_type = 'FileValue' THEN 'filevalue'
                                   ELSE bi_customer_pnl.source_type
                               END as dataset,
                               sobp_rollup_xref.sub_region_name as sub_region_name,
                               sobp_rollup_xref.country_code as country_cd,
                               sobp_rollup_xref.country_name as country_name,
                               SUM(
                                   CASE
                                       WHEN (bi_customer_pnl.category in ('PS')) THEN COALESCE(bi_customer_pnl.psa_cost_amt_us, 0) +         COALESCE(bi_customer_pnl.hfm_adj_cost_amt_us, 0)
                                       WHEN (bi_customer_pnl.category in ('HW')) THEN COALESCE(bi_customer_pnl.base_tran_amt_us, 0) + COALESCE(bi_customer_pnl.gl_adj_cost_amt_us, 0) + COALESCE(bi_customer_pnl.hfm_adj_cost_amt_us, 0) + COALESCE(bi_customer_pnl.uplift_tot_amt_us, 0)
                                       ELSE COALESCE(bi_customer_pnl.base_tran_amt_us, 0) + COALESCE(bi_customer_pnl.gl_adj_cost_amt_us, 0) + COALESCE(bi_customer_pnl.hfm_adj_cost_amt_us, 0)
                                   END
                                   )as cost
                               FROM
                                   """ + dbParams("EEDDLLDDBB") + """.m2_gl_reporting_map_dn_drive m2_gl_reporting_map_dn_drive
                               RIGHT JOIN
									                 """ + dbParams("EEDDLLDDBB") + """.bi_customer_pnl bi_customer_pnl
                               ON  m2_gl_reporting_map_dn_drive.fml_account_code = bi_customer_pnl.fml_acct_cd
                               LEFT JOIN
                                   """ + dbParams("EEDDLLDDBB") + """.bi_pkg_hierarchy bi_pkg_hierarchy
                               ON bi_customer_pnl.fml_acct_cd = bi_pkg_hierarchy.fml_acct_cd
                               AND bi_customer_pnl.csub_acct_cd = bi_pkg_hierarchy.csub_acct_cd
                               LEFT JOIN
									                 """ + dbParams("EEDDLLDDBB") + """.sobp_rollup_xref sobp_rollup_xref
                               ON  trim(bi_customer_pnl.rptg_sobp_cd) = trim(sobp_rollup_xref.set_of_books_partition_code)
                               AND sobp_rollup_xref.global_code = 'G1'
                               LEFT JOIN
                                   """ + dbParams("EEDDLLDDBB") + """.dim_product_mdm dim_product_mdm
                               ON dim_product_mdm.product_mdm_id=bi_customer_pnl.product_mdm_id
                               LEFT JOIN
                                   """ + dbParams("EEDDLLDDBB") + """.hub_bi_cpnl_bu_rptg_hierarchy_cb
                               ON bi_customer_pnl.HFM_Custom3 =hub_bi_cpnl_bu_rptg_hierarchy_cb.l2_Custom3
                               and hub_bi_cpnl_bu_rptg_hierarchy_cb.sub_industry_name=bi_customer_pnl.cust_ind_name
                               WHERE
                                   (
                                   bi_customer_pnl.accounting_yr_month = 202009
                               AND bi_customer_pnl.industry = 'HOT'
                               AND (
                                   bi_customer_pnl.source_type = 'HFM_ADJ'
                                   OR (
                                       bi_customer_pnl.source_type != ('HFM_ADJ')
                                   AND (
                                       bi_customer_pnl.fml_acct_cd like '8%'
                                   OR  bi_customer_pnl.fml_acct_cd like '5%'
                                       )
                                      )
                                   )
                               AND bi_customer_pnl.data_source_cd  in  ('GL')
                               AND bi_customer_pnl.category in ('SWM')
                               AND bi_pkg_hierarchy.down2_pkg_name not in ('3RD PARTY SOFTWARE FOR RESALE')
                               AND trim(bi_pkg_hierarchy.down3_pkg_name) not in ('ISC From Software Dev', 'ISC To Software PS')
                               AND trim(bi_customer_pnl.down2_dept_code_name) like 'D15009%'
                               AND trim(sobp_rollup_xref.sub_region_name)='NAMER'
                                )
                               GROUP BY
                                   1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,
                                   27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52
                           ) customer_pnl
                       WHERE
                           customer_pnl.dataset in ('cost', 'hilt')

                       UNION

                       SELECT
                           customer_pnl.acctg_period_name,
                           customer_pnl.source_type,
                           customer_pnl.category,
                           customer_pnl.fml_acct_cd,
                           customer_pnl.fml_org_cd,
                           customer_pnl.gl_code_combination_id,
                           customer_pnl.hfm_custom3,
                           customer_pnl.industry,
                           customer_pnl.instance_id,
                           customer_pnl.prod_srvc_cd,
                           customer_pnl.rptg_sobp_cd,
                           customer_pnl.data_source_cd,
                           customer_pnl.func_group_id,
                           customer_pnl.gl_adj_amt_us,
                           customer_pnl.gl_adj_amt_func,
                           customer_pnl.fin_proj_id,
                           customer_pnl.inter_entity_cd,
                           customer_pnl.csub_acct_cd,
                           customer_pnl.down2_dept_name,
                           customer_pnl.down2_dept_code,
                           customer_pnl.down2_dept_code_name,
                           customer_pnl.down3_dept_name,
                           customer_pnl.down3_dept_code,
                           customer_pnl.down3_dept_code_name,
                           customer_pnl.down4_dept_name,
                           customer_pnl.down4_dept_code,
                           customer_pnl.down4_dept_code_name,
                           customer_pnl.down5_dept_name,
                           customer_pnl.down5_dept_code,
                           customer_pnl.down5_dept_code_name,
                           customer_pnl.down6_dept_name,
                           customer_pnl.down6_dept_code,
                           customer_pnl.down6_dept_code_name,
                           customer_pnl.down7_dept_name,
                           customer_pnl.down7_dept_code,
                           customer_pnl.down7_dept_code_name,
                           customer_pnl.gl_posted_date,
                           customer_pnl.asset_product_name,
                           customer_pnl.base_tran_amt_func,
                           customer_pnl.base_tran_amt_us,
                           customer_pnl.currency_cd,
                           customer_pnl.exchange_rate,
                           customer_pnl.hfm_adj_cost_amt_us,
                           customer_pnl.currency_cd_entered,
                           customer_pnl.service_product_mdm_id,
                           customer_pnl.service_mdm_product_family,
                           customer_pnl.service_product_name,
                           customer_pnl.dataset,
                           customer_pnl.sub_region_name,
                           customer_pnl.country_cd,
                           customer_pnl.country_name,
                           customer_pnl.cost,
                           customer_pnl.accounting_yr_month
                       FROM
                          (
                           SELECT
                               bi_customer_pnl.acctg_period_name,
                               bi_customer_pnl.accounting_yr_month,
                               bi_customer_pnl.source_type,
                               bi_customer_pnl.category,
                               bi_customer_pnl.fml_acct_cd,
                               bi_customer_pnl.fml_org_cd,
                               bi_customer_pnl.gl_code_combination_id,
                               bi_customer_pnl.hfm_custom3,
                               bi_customer_pnl.industry,
                               bi_customer_pnl.instance_id,
                               bi_customer_pnl.prod_srvc_cd,
                               bi_customer_pnl.rptg_sobp_cd,
                               bi_customer_pnl.data_source_cd,
                               bi_customer_pnl.func_group_id,
                               bi_customer_pnl.gl_adj_amt_us,
                               bi_customer_pnl.gl_adj_amt_func,
                               bi_customer_pnl.fin_proj_id,
                               bi_customer_pnl.inter_entity_cd,
                               bi_customer_pnl.csub_acct_cd,
                               bi_customer_pnl.down2_dept_name ,
                               bi_customer_pnl.down2_dept_code,
                               bi_customer_pnl.down2_dept_code_name,
                               bi_customer_pnl.down3_dept_name,
                               bi_customer_pnl.down3_dept_code,
                               bi_customer_pnl.down3_dept_code_name,
                               bi_customer_pnl.down4_dept_name,
                               bi_customer_pnl.down4_dept_code,
                               bi_customer_pnl.down4_dept_code_name,
                               bi_customer_pnl.down5_dept_name,
                               bi_customer_pnl.down5_dept_code,
                               bi_customer_pnl.down5_dept_code_name,
                               bi_customer_pnl.down6_dept_name,
                               bi_customer_pnl.down6_dept_code,
                               bi_customer_pnl.down6_dept_code_name,
                               bi_customer_pnl.down7_dept_name,
                               bi_customer_pnl.down7_dept_code,
                               bi_customer_pnl.down7_dept_code_name,
                               bi_customer_pnl.gl_posted_date,
                               bi_customer_pnl.asset_product_name,
                               bi_customer_pnl.base_tran_amt_func,
                               bi_customer_pnl.base_tran_amt_us,
                               bi_customer_pnl.currency_cd,
                               bi_customer_pnl.exchange_rate,
                               bi_customer_pnl.hfm_adj_cost_amt_us,
                               bi_customer_pnl.currency_cd_entered,
                               bi_customer_pnl.service_product_mdm_id,
                               bi_customer_pnl.service_mdm_product_family,
                               bi_customer_pnl.service_product_name,
                               CASE
                                  WHEN bi_customer_pnl.source_type = 'ERP_COST' THEN 'cost'
                                  WHEN bi_customer_pnl.data_source_cd like '%COST%' THEN 'cost'
                                  WHEN bi_customer_pnl.data_source_cd like '%COS%' THEN 'cost'
                                  WHEN bi_customer_pnl.data_source_cd like '%Cost%' THEN 'cost'
                                  WHEN bi_customer_pnl.source_type = 'ERP_REV' THEN 'revenue'
                                  WHEN bi_customer_pnl.data_source_cd like '%REV%' THEN 'revenue'
                                  WHEN bi_customer_pnl.source_type = 'HFM_ADJ' AND data_source_cd = 'HFM_ADJ' THEN 'revenue'
                                  WHEN bi_customer_pnl.source_type = 'HFM_ADJ' AND data_source_cd like '%HILT%' THEN 'hilt'
                                  WHEN bi_customer_pnl.source_type = 'FileValue' THEN 'filevalue' else bi_customer_pnl.source_type end as dataset,
                               sobp_rollup_xref.sub_region_name as sub_region_name,
                               sobp_rollup_xref.country_code as country_cd,
                               sobp_rollup_xref.country_name as country_name,
                               SUM(
                                  CASE
                                     WHEN (bi_customer_pnl.category in ('PS')) THEN COALESCE(bi_customer_pnl.psa_cost_amt_us, 0) + COALESCE(bi_customer_pnl.hfm_adj_cost_amt_us, 0)
                                     WHEN (bi_customer_pnl.category in ('HW')) THEN COALESCE(bi_customer_pnl.base_tran_amt_us, 0) + COALESCE(bi_customer_pnl.gl_adj_cost_amt_us, 0) + COALESCE(bi_customer_pnl.hfm_adj_cost_amt_us, 0) + COALESCE(bi_customer_pnl.uplift_tot_amt_us, 0) else COALESCE(bi_customer_pnl.base_tran_amt_us, 0) + COALESCE(bi_customer_pnl.gl_adj_cost_amt_us, 0) + COALESCE(bi_customer_pnl.hfm_adj_cost_amt_us, 0)
                                  END
                                  ) as cost
                              FROM
                                  """ + dbParams("EEDDLLDDBB") + """.m2_gl_reporting_map_dn_drive m2_gl_reporting_map_dn_drive
                              RIGHT JOIN
										              """ + dbParams("EEDDLLDDBB") + """.bi_customer_pnl bi_customer_pnl
                              ON  m2_gl_reporting_map_dn_drive.fml_account_code = bi_customer_pnl.fml_acct_cd
                              LEFT JOIN
                                  """ + dbParams("EEDDLLDDBB") + """.bi_pkg_hierarchy bi_pkg_hierarchy
                              ON  bi_customer_pnl.fml_acct_cd = bi_pkg_hierarchy.fml_acct_cd
                              AND bi_customer_pnl.csub_acct_cd = bi_pkg_hierarchy.csub_acct_cd
                              LEFT JOIN
     									            """ + dbParams("EEDDLLDDBB") + """.sobp_rollup_xref sobp_rollup_xref
                              ON trim(bi_customer_pnl.rptg_sobp_cd) = trim(sobp_rollup_xref.set_of_books_partition_code)
                              AND trim(sobp_rollup_xref.global_code) = 'G1'
                              LEFT JOIN
                                  """ + dbParams("EEDDLLDDBB") + """.dim_product_mdm dim_product_mdm
                              ON dim_product_mdm.product_mdm_id=bi_customer_pnl.product_mdm_id
                              LEFT JOIN
                                 """ + dbParams("EEDDLLDDBB") + """.hub_bi_cpnl_bu_rptg_hierarchy_cb hub_bi_cpnl_bu_rptg_hierarchy_cb
                              ON bi_customer_pnl.HFM_Custom3 =hub_bi_cpnl_bu_rptg_hierarchy_cb.l2_Custom3
                              AND hub_bi_cpnl_bu_rptg_hierarchy_cb.sub_industry_name=bi_customer_pnl.cust_ind_name
                              INNER JOIN
                                 (
                                 SELECT
                                     TRIM(m2gl.fml_account_code) fml_account_code
                                 FROM
                                     """ + dbParams("EEDDLLDDBB") + """.m2_gl_reporting_map_dn_drive m2gl
                                 WHERE
                                     m2gl.hyperion_rpt_line_code = 'CustomerRevenue'
                                 AND m2gl.hyperion_reporting_code = '4001'
                                  ) m2g on bi_customer_pnl.fml_acct_cd = m2g.fml_account_code
                              WHERE
                                  (
                                  bi_customer_pnl.accounting_yr_month = 202009
                              AND bi_customer_pnl.industry = 'HOT'
                              AND (
                                  bi_customer_pnl.source_type = 'HFM_ADJ'
                                  OR (bi_customer_pnl.source_type != ('HFM_ADJ'))
                                  )
                              AND bi_customer_pnl.data_source_cd in ('GL')
                              AND bi_customer_pnl.category in ('SWM')
                              AND bi_pkg_hierarchy.down2_pkg_name not in ('3RD PARTY SOFTWARE FOR RESALE')
                              AND TRIM(bi_pkg_hierarchy.down3_pkg_name) not in ('ISC From Software Dev', 'ISC To Software PS')
                              AND TRIM(bi_customer_pnl.down2_dept_code_name) like 'D15009%'
                              AND trim(sobp_rollup_xref.sub_region_name)='NAMER'
                                   )
                              GROUP BY
                                  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,
										              27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52
                           ) customer_pnl
                       WHERE
                           customer_pnl.dataset in ('cost', 'hilt') """
    executor.executeQuery(query)
  }
}

object globalsaleshotcostObjRunner{

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
    var executor = new ExecutorDao(activityCountRequired, "global_sales_hot_cost")
    var dbParams = executor.loadProperties(propertyFilePath)
    var job_start_time = new Timestamp(new Date().getTime())
    println("################################################################")
    println("Started Job 'globalsaleshotcost' at %s.".format(job_start_time.toString()))
    var globalsaleshotcostObj = new globalsaleshotcost(executor, dbParams)
    try {
      globalsaleshotcostObj.run()
    } finally {
      var job_end_time = new Timestamp(new Date().getTime())
      println("Completed 'globalsaleshotcost' at %s.".format(job_end_time.toString()))
      println("Total Job Execution Time: %s".format(executor.getExecutionTime(job_start_time, job_end_time)))
      executor.postProcess()
      println("################################################################")
    }
  }
}
