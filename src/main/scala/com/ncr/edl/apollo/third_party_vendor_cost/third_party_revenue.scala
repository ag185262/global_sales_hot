package com.ncr.edl.apollo.third_party_vendor_cost

import com.ncr.edl.apollo.common.ExecutorDao
import java.sql.Timestamp
import java.util.Date

class thirdpartyvendorrevenue(executor: ExecutorDao, dbParams: scala.collection.mutable.Map[String, String]) {

  def run(){
    var query = ""

    query = """
           INSERT
              OVERWRITE
                  TABLE
                  """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue partition (accounting_yr_month)
                   select
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
                     customer_pnl.dataset,
                     customer_pnl.fin_proj_id,
                     customer_pnl.revenue,
                     customer_pnl.accounting_yr_month
                  from
                      (
                       select
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
                         bi_customer_pnl.fin_proj_id,
                         sum(
                            (
                            case
                                when bi_customer_pnl.dist_amt_us is null then 0
                                else bi_customer_pnl.dist_amt_us end + case when bi_customer_pnl.gl_adj_amt_us is null then 0 else bi_customer_pnl.gl_adj_amt_us end + case when bi_customer_pnl.hfm_adj_amt_us is null then 0 else (bi_customer_pnl.hfm_adj_amt_us)
                                end )
                                    ) as revenue
                         from
                            """ + dbParams("EEDDLLDDBB") + """.bi_customer_pnl bi_customer_pnl
                         inner join (
                             select
                                 trim(m2gl.fml_account_code) fml_account_code
                         from
                            """ + dbParams("EEDDLLDDBB") + """.m2_gl_reporting_map_dn_drive m2gl
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
                             or (bi_customer_pnl.source_type != ('HFM_ADJ')
                             )
                             )
                         and bi_customer_pnl.category IN ('CLOUD')
                         and bi_customer_pnl.industry = 'FIN' and bi_customer_pnl.rptg_sobp_cd='201' and bi_customer_pnl.fml_org_cd='201100037' --Added
                            )
                         group by
                           1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
                           ) customer_pnl
                         where
                            customer_pnl.dataset in ('revenue', 'hilt') and customer_pnl.revenue!=0"""
    executor.executeQuery(query)

    query = """
        DROP TABLE IF EXISTS  """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue1a """
    executor.executeQuery(query)

    query= """
           CREATE
              TABLE
                 """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue1a as select ou.*
                 from (
                 select ma.*,--,platform.customer_type
                 case
                 when platform.master_customer_number is null then 'A La Carte'
                 else platform.customer_type
                 end as customer_type
                 from (
                 select ou.master_cust_nbr,ou.asset_product_name,ou.accounting_yr_month,sum(ou.revenue) revenue
                 from """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue ou where ou.accounting_yr_month=202009
                          and ou.master_cust_nbr!='-3' and ou.asset_product_name!='Unassigned'      --Added
                 group by ou.accounting_yr_month,ou.asset_product_name,ou.master_cust_nbr
                 ) ma
                 left join """ + dbParams("EEDDLLDDBB") + """.hub_cloud_di_third_party_vendor_platform platform  --- hub_cloud_di_third_party_vendor_platform  // tmp_cpnl_plat1
                 on platform.master_customer_number=ma.master_cust_nbr
                 ) ou """
    executor.executeQuery(query)

    query=
      """
        DROP TABLE IF EXISTS  """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue2a"""
    executor.executeQuery(query)

    query= """
           CREATE
              TABLE
                """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue2a as
                select
                   rev1.asset_product_name,
                   COALESCE(rev1.revenue, 0) as revenue,
                   lkp_rev.master_cust_nbr,
                   lkp_rev.product as asset_product_name_1,
                   lkp_rev.allocation_2,
                   lkp_rev.accounting_yr_month,
                   lkp_rev.customer_type,
                   lkp_rev.customer_type_lkp
                from
                  (
                select
                   lkp.product,
                   lkp.customer_type as customer_type_lkp,
                   lkp.customer_type,
                   lkp.allocation_2,
                   rev_inner.master_cust_nbr,
                   rev_inner.accounting_yr_month
                from
                 (
                select
                  distinct master_cust_nbr,
                  customer_type,
                  accounting_yr_month
                from
                  """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue1a
                 ) rev_inner
              inner join """ + dbParams("EEDDLLDDBB") + """.hub_cloud_di_third_party_vendor_lookup lkp on lkp.customer_type = rev_inner.customer_type
                 ) lkp_rev
              left join (select * from """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue1a) rev1 on rev1.master_cust_nbr = lkp_rev.master_cust_nbr and rev1.asset_product_name = lkp_rev.product
              UNION
              select
                 rev.asset_product_name,
                 COALESCE(rev.revenue, 0) as revenue,
                 rev.master_cust_nbr,
                 rev.asset_product_name as asset_product_name_1,
                 NULL as allocation_2,
                 rev.accounting_yr_month,
                 rev.customer_type,
                 NULL as customer_type_lkp
              from """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue1a rev where customer_type = 'A La Carte' """
    executor.executeQuery(query)

    query=
      """
        DROP TABLE IF EXISTS  """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue3a """
    executor.executeQuery(query)

    query= """
           CREATE
             TABLE
                """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue3a as
                 select distinct ma.customer_type,ma.master_cust_nbr,ma.asset_product_name_1,ma.revenue,ma.allocation_2,ma.accounting_yr_month,
                 case
                 when ma.customer_type='A La Carte' then ma.revenue   --Modified
                 when ma.asset_product_name_1='Internet Banking' then (ma1.revenue*ma.allocation_2)
                 else (ma1.revenue*ma.allocation_2)+ma.revenue
                 end as allocated_rev_1
                 from """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue2a ma
                 inner join (select distinct customer_type_lkp,master_cust_nbr,revenue from """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue2a where asset_product_name_1='Internet Banking'
                 and customer_type_lkp is not null) ma1
                 on ma1.customer_type_lkp=ma.customer_type
                 and ma1.master_cust_nbr=ma.master_cust_nbr
                 UNION
                 select distinct ma.customer_type,ma.master_cust_nbr,ma.asset_product_name_1,ma.revenue,ma.allocation_2,ma.accounting_yr_month,
                  case
                    when ma.customer_type='A La Carte' then ma.revenue  --Modified
                    else ma.revenue
                    end as allocated_rev_1
                from """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue2a ma
                 left join (select distinct customer_type_lkp,master_cust_nbr,revenue from """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue2a where asset_product_name_1='Internet Banking'
                   and customer_type_lkp is not null) ma1
                   on ma1.customer_type_lkp=ma.customer_type
                   and ma1.master_cust_nbr=ma.master_cust_nbr
                   where ma1.customer_type_lkp is null or ma1.master_cust_nbr is null"""
    executor.executeQuery(query)


    query = """  with get_tot_customer_prod_rev as (
              select asset_product_name_1,
              sum(allocated_rev_1) as total_revenue
              from """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue3a
              group by asset_product_name_1 ),
              get_tot_prod_cost as (
              select t.prod_name,sum(t.cost) as total_cost
              from """ + dbParams("EEDDLLDDBB") + """.tmp_in_cpnl_thirdpartyvendor_cost t where t.prod_name!='Unassigned' group by t.prod_name
              )
              insert overwrite table """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue4a
              select a.*,
              a.revenue_ratio*a.total_cost as cost
              from (
              select master_cust_nbr,
              get_tot_customer_prod_rev.asset_product_name_1 as prodname,
              allocated_rev_1,
              get_tot_customer_prod_rev.total_revenue as totalrevenue,
              customer_type,
               round((allocated_rev_1/get_tot_customer_prod_rev.total_revenue),7) as revenue_ratio,
              get_tot_prod_cost.total_cost
              from  """ + dbParams("EEDDLLDDBB") + """.tmp_cpnl_thirdpartyvendor_revenue3a
              inner join get_tot_customer_prod_rev
              on tmp_cpnl_thirdpartyvendor_revenue3a.asset_product_name_1 = get_tot_customer_prod_rev.asset_product_name_1
              inner join get_tot_prod_cost
              on get_tot_prod_cost.prod_name=get_tot_customer_prod_rev.asset_product_name_1
              )a  """

    executor.executeQuery(query)

  }
}

object thirdpartyvendorrevenueObjRunner{

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

    var executor = new ExecutorDao(activityCountRequired, "third_party_revenue")
    var dbParams = executor.loadProperties(propertyFilePath)
    var job_start_time = new Timestamp(new Date().getTime())
    println("################################################################")
    println("Started Job 'thirdpartyvendorrevenue' at %s.".format(job_start_time.toString()))
    var thirdpartyvendorrevenueObj = new thirdpartyvendorrevenue(executor, dbParams)
    try {
      thirdpartyvendorrevenueObj.run()
    } finally {
      var job_end_time = new Timestamp(new Date().getTime())
      println("Completed 'thirdpartyvendorrevenue' at %s.".format(job_end_time.toString()))
      println("Total Job Execution Time: %s".format(executor.getExecutionTime(job_start_time, job_end_time)))
      executor.postProcess()
      println("################################################################")
    }
  }
}
