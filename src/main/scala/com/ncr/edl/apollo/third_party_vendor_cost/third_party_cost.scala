package com.ncr.edl.apollo.third_party_vendor_cost

import com.ncr.edl.apollo.common.ExecutorDao
import java.sql.Timestamp
import java.util.Date

class thirdpartyvendorcost(executor: ExecutorDao, dbParams: scala.collection.mutable.Map[String, String]) {

  def run(){
    var query = ""

    query = """
            INSERT
              OVERWRITE
                  TABLE
                     """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_cost_new partition (accounting_yr_month)
                     select
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
                       customer_pnl.product_class,
                       customer_pnl.dataset,
                       customer_pnl.cost,
                       customer_pnl.accounting_yr_month
               from
                 (
                 select
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
                       null as gl_adj_amt_us,
                       null as gl_adj_amt_func,
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
                       bi_customer_pnl.currency_cd,
                       bi_customer_pnl.exchange_rate,
                       bi_customer_pnl.currency_cd_entered,
                       bi_customer_pnl.service_product_mdm_id,
                       bi_customer_pnl.service_mdm_product_family,
                       bi_customer_pnl.service_product_name,
                       bi_customer_pnl.product_class,
                 case
                     when bi_customer_pnl.source_type = 'ERP_COST' then 'cost'
                     when bi_customer_pnl.data_source_cd like '%COST%' then 'cost'
                     when bi_customer_pnl.data_source_cd like '%COS%' then 'cost'
                     when bi_customer_pnl.data_source_cd like '%Cost%' then 'cost'
                     when bi_customer_pnl.source_type = 'ERP_REV' then 'revenue'
                     when bi_customer_pnl.data_source_cd like '%REV%' then 'revenue'
                     when bi_customer_pnl.source_type = 'HFM_ADJ' and data_source_cd = 'HFM_ADJ' then 'revenue'
                     when bi_customer_pnl.source_type = 'HFM_ADJ' and data_source_cd like '%HILT%' then 'hilt'
                     when bi_customer_pnl.source_type = 'FileValue' then 'filevalue'
                     else bi_customer_pnl.source_type
                     end as dataset,
            sum(
                 case
                     when (bi_customer_pnl.category in ('PS')) then coalesce(bi_customer_pnl.psa_cost_amt_us, 0) + coalesce(bi_customer_pnl.hfm_adj_cost_amt_us, 0)
                     when (bi_customer_pnl.category in ('HW')) then coalesce(bi_customer_pnl.base_tran_amt_us, 0) + coalesce(bi_customer_pnl.gl_adj_cost_amt_us, 0) + coalesce(bi_customer_pnl.hfm_adj_cost_amt_us, 0) + coalesce(bi_customer_pnl.uplift_tot_amt_us, 0)
                     else coalesce(bi_customer_pnl.base_tran_amt_us, 0) + coalesce(bi_customer_pnl.gl_adj_cost_amt_us, 0) + coalesce(bi_customer_pnl.hfm_adj_cost_amt_us, 0)
                     end
                ) as cost,
            sum(bi_customer_pnl.base_tran_amt_us) as base_tran_amt_us,
            sum(bi_customer_pnl.hfm_adj_cost_amt_us) as hfm_adj_cost_amt_us
        from """ + dbParams("EEDDLLDDBB") + """.m2_gl_reporting_map_dn_drive m2_gl_reporting_map_dn_drive
            right join """ + dbParams("EEDDLLDDBB") + """.bi_customer_pnl bi_customer_pnl on m2_gl_reporting_map_dn_drive.fml_account_code = bi_customer_pnl.fml_acct_cd
            left join """ + dbParams("EEDDLLDDBB") + """.bi_pkg_hierarchy bi_pkg_hierarchy on bi_customer_pnl.fml_acct_cd = bi_pkg_hierarchy.fml_acct_cd and bi_customer_pnl.csub_acct_cd = bi_pkg_hierarchy.csub_acct_cd
            left join """ + dbParams("EEDDLLDDBB") + """.sobp_rollup_xref sobp_rollup_xref on bi_customer_pnl.rptg_sobp_cd = sobp_rollup_xref.set_of_books_partition_code and sobp_rollup_xref.global_code = 'G1'
            where
            (bi_customer_pnl.accounting_yr_month = 202009
            and bi_customer_pnl.industry is not null
            and ( bi_customer_pnl.source_type = 'HFM_ADJ'
            or (
            bi_customer_pnl.source_type != ('HFM_ADJ')
            and (
              bi_customer_pnl.fml_acct_cd like '8%'
              or bi_customer_pnl.fml_acct_cd like '5%'
            )
          )
        )
        and bi_customer_pnl.category in ('CLOUD')
        and bi_pkg_hierarchy.down2_pkg_name not in ('AMORTIZATION') --OR bi_pkg_hierarchy.down2_pkg_name IS NULL
        and bi_customer_pnl.fml_org_cd = '201225650'
        and TRIM(bi_customer_pnl.down7_dept_code_name)='D69541: BUL:Banking:Digital:DI:Support and Services COR'
      )
    group by
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48
  ) customer_pnl
  where
     customer_pnl.dataset in ('cost', 'hilt')
  UNION
       select
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
             customer_pnl.product_class,
             customer_pnl.dataset,
             customer_pnl.cost,
             customer_pnl.accounting_yr_month
       from
        (
        select
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
             null as gl_adj_amt_us,
             null as gl_adj_amt_func,
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
             bi_customer_pnl.currency_cd,
             bi_customer_pnl.exchange_rate,
             bi_customer_pnl.currency_cd_entered,
             bi_customer_pnl.service_product_mdm_id,
             bi_customer_pnl.service_mdm_product_family,
             bi_customer_pnl.service_product_name,
             bi_customer_pnl.product_class,
        case
            when bi_customer_pnl.source_type = 'ERP_COST' then 'cost'
            when bi_customer_pnl.data_source_cd like '%COST%' then 'cost'
            when bi_customer_pnl.data_source_cd like '%COS%' then 'cost'
            when bi_customer_pnl.data_source_cd like '%Cost%' then 'cost'
            when bi_customer_pnl.source_type = 'ERP_REV' then 'revenue'
            when bi_customer_pnl.data_source_cd like '%REV%' then 'revenue'
            when bi_customer_pnl.source_type = 'HFM_ADJ' and data_source_cd = 'HFM_ADJ' then 'revenue'
            when bi_customer_pnl.source_type = 'HFM_ADJ' and data_source_cd like '%HILT%' then 'hilt'
            when bi_customer_pnl.source_type = 'FileValue' then 'filevalue'
            else bi_customer_pnl.source_type
            end as dataset,
      sum(
        case
           when (bi_customer_pnl.category in ('PS')) then coalesce(bi_customer_pnl.psa_cost_amt_us, 0) + coalesce(bi_customer_pnl.hfm_adj_cost_amt_us, 0)
           when (bi_customer_pnl.category in ('HW')) then coalesce(bi_customer_pnl.base_tran_amt_us, 0) + coalesce(bi_customer_pnl.gl_adj_cost_amt_us, 0) + coalesce(bi_customer_pnl.hfm_adj_cost_amt_us, 0) + coalesce(bi_customer_pnl.uplift_tot_amt_us, 0)
           else coalesce(bi_customer_pnl.base_tran_amt_us, 0) + coalesce(bi_customer_pnl.gl_adj_cost_amt_us, 0) + coalesce(bi_customer_pnl.hfm_adj_cost_amt_us, 0)
           end
         ) as cost,
      sum(bi_customer_pnl.base_tran_amt_us) as base_tran_amt_us,
      sum(bi_customer_pnl.hfm_adj_cost_amt_us) as hfm_adj_cost_amt_us
    from
        """ + dbParams("EEDDLLDDBB") + """.m2_gl_reporting_map_dn_drive m2_gl_reporting_map_dn_drive
            right join """ + dbParams("EEDDLLDDBB") + """.bi_customer_pnl bi_customer_pnl on m2_gl_reporting_map_dn_drive.fml_account_code = bi_customer_pnl.fml_acct_cd
            left join """ + dbParams("EEDDLLDDBB") + """.bi_pkg_hierarchy bi_pkg_hierarchy on bi_customer_pnl.fml_acct_cd = bi_pkg_hierarchy.fml_acct_cd and bi_customer_pnl.csub_acct_cd = bi_pkg_hierarchy.csub_acct_cd
            left join """ + dbParams("EEDDLLDDBB") + """.sobp_rollup_xref sobp_rollup_xref on bi_customer_pnl.rptg_sobp_cd = sobp_rollup_xref.set_of_books_partition_code and sobp_rollup_xref.global_code = 'G1'
            inner join (select trim(m2gl.fml_account_code) fml_account_code from """ + dbParams("EEDDLLDDBB") + """.m2_gl_reporting_map_dn_drive m2gl
        where
          m2gl.hyperion_rpt_line_code = 'CustomerRevenue'
          and m2gl.hyperion_reporting_code = '4001'
      ) m2g on bi_customer_pnl.fml_acct_cd = m2g.fml_account_code
    where
      (
        bi_customer_pnl.accounting_yr_month = 202009
        and bi_customer_pnl.industry is not null
        and (
          bi_customer_pnl.source_type = 'HFM_ADJ'
          or (bi_customer_pnl.source_type != ('HFM_ADJ'))
        )
        and bi_customer_pnl.category IN ('CLOUD')
        and bi_pkg_hierarchy.down2_pkg_name NOT IN ('AMORTIZATION') --OR bi_pkg_hierarchy.down2_pkg_name IS NULL
        and bi_customer_pnl.fml_org_cd = '201225650'
        and TRIM(bi_customer_pnl.down7_dept_code_name)='D69541: BUL:Banking:Digital:DI:Support and Services COR'
      )
    group by
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48
  ) customer_pnl
where
  customer_pnl.dataset in ('cost', 'hilt')"""

    executor.executeQuery(query)

    query = """
        DROP TABLE IF EXISTS """ + dbParams("EEDDLLDDBB") + """.tmp_in_cpnl_thirdpartyvendor_cost """

    executor.executeQuery(query)

    query = """
		           CREATE
				          TABLE
				             """ + dbParams("EEDDLLDDBB") + """.tmp_in_cpnl_thirdpartyvendor_cost AS
                      SELECT ma.*,
                         CASE
                            WHEN ma.asset_product_name='Unassigned' THEN COALESCE(finproj.product_name,'Unassigned')
                            ELSE ma.asset_product_name
                            END as prod_name,
                            finproj.product_family as fin_product_family
                      FROM """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_cost_new ma
                       LEFT JOIN
     							        """ + dbParams("EEDDLLDDBB") + """.hub_cloud_di_third_party_vendor_fin_project finproj
                            ON
							              finproj.financial_project_id=ma.fin_proj_id
                            WHERE ma.accounting_yr_month=202009 """
    executor.executeQuery(query)
	
	
	
	    //Added unassigned cost logic 26 Jan 2021(Cost ratio calc and assignunassign cost)//

    query=
      """
        DROP TABLE IF EXISTS  """ + dbParams("EEDDLLDDBB") + """.tmp_unassigned_cost_alloc """
    executor.executeQuery(query)

    query= """
            CREATE
               TABLE
                 """ + dbParams("EEDDLLDDBB") + """.tmp_unassigned_cost_alloc as
                     select r.accounting_yr_month,u.rptg_sobp_cd,u.fml_acct_cd,u.fml_org_cd,u.csub_acct_cd,u.fin_proj_id,u.prod_srvc_cd,u.inter_entity_cd, r.prod_name,
r.cost_ratio,r.cost_ratio*u.unassigned_cost as total_cost
from
(select g.accounting_yr_month, g.prod_name,round((g.total_cost/p.assigned_cost),5) as cost_ratio
 from (select prod_name,accounting_yr_month,sum(cost) as total_cost from 
""" + dbParams("EEDDLLDDBB") + """.tmp_in_cpnl_thirdpartyvendor_cost group by prod_name,accounting_yr_month)g left join
(select accounting_yr_month,sum(case when prod_name='Unassigned' then cost end) as unassigned_cost,
sum(case when prod_name!='Unassigned' then cost end) as assigned_cost from """ + dbParams("EEDDLLDDBB") + """.tmp_in_cpnl_thirdpartyvendor_cost
group by accounting_yr_month)p on 
g.accounting_yr_month=p.accounting_yr_month where g.prod_name!='Unassigned')r
inner join(select accounting_yr_month,rptg_sobp_cd,fml_acct_cd,fml_org_cd,csub_acct_cd,fin_proj_id,prod_srvc_cd,inter_entity_cd,
sum(case when prod_name='Unassigned' then cost end) as unassigned_cost
from """ + dbParams("EEDDLLDDBB") + """.tmp_in_cpnl_thirdpartyvendor_cost where prod_name='Unassigned' group by accounting_yr_month,rptg_sobp_cd,fml_acct_cd,fml_org_cd,csub_acct_cd,fin_proj_id,prod_srvc_cd,
inter_entity_cd)u on  r.accounting_yr_month=u.accounting_yr_month """

    executor.executeQuery(query)
	
		
  }
}

object thirdpartyvendorcostObjRunner{

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

    var executor = new ExecutorDao(activityCountRequired, "third_party_cost")
    var dbParams = executor.loadProperties(propertyFilePath)
    var job_start_time = new Timestamp(new Date().getTime())
    println("################################################################")
    println("Started Job 'thirdpartyvendorcost' at %s.".format(job_start_time.toString()))
    var thirdpartyvendorcostObj = new thirdpartyvendorcost(executor, dbParams)
    try {
      thirdpartyvendorcostObj.run()
    } finally {
      var job_end_time = new Timestamp(new Date().getTime())
      println("Completed 'thirdpartyvendorcost' at %s.".format(job_end_time.toString()))
      println("Total Job Execution Time: %s".format(executor.getExecutionTime(job_start_time, job_end_time)))
      executor.postProcess()
      println("################################################################")
    }
  }
}
