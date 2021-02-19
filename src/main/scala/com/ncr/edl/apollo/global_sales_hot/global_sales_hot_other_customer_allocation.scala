package com.ncr.edl.apollo.global_sales_hot

import com.ncr.edl.apollo.common.ExecutorDao
import java.sql.Timestamp
import java.util.Date

class globalsaleshotothercustomerallocation(executor: ExecutorDao, dbParams: scala.collection.mutable.Map[String, String]) {

  def run(){
    var query = ""

    query = """ INSERT
	                  OVERWRITE
					              TABLE
					                  """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_PID_step1
                            SELECT *
                            FROM
                                (SELECT DISTINCT
                                    product_code,
                                    product,
                                    product_mdm_id,
                                    lob,
                                    product_family,
                                    product_mdm_family,
                                    product_family_code,
                                    pid,
                                    as_of_dt,
                                    row_number() OVER (PARTITION BY sq.pid, sq.product ORDER BY sq.as_of_dt DESC) AS r
                                FROM
                                    (SELECT substr(EDW1.product_key, -9) AS product_code,
                                        regexp_replace(EDW1.Product_ID, '-', '') AS pid,
                                        EDW2.Product_Name AS product,
                                        EDW2.product_mdm_id AS product_mdm_id,
                                        EDW3.product_hier_level_1_name AS lob,
                                        CASE
                                            WHEN lob='hardware' THEN COALESCE(edw3.product_hier_level_3_name, 'Unassigned')
                                            WHEN lob='Professional Service' THEN coalesce(EDW3.product_hier_level_3_name, EDW3.product_hier_level_2_name, 'Unassigned')
                                            ELSE COALESCE(edw3.product_hier_level_2_name, 'Unassigned')
                                        END product_family,
                                        EDW2.product_hierarchy_mdm_id AS product_mdm_family,
                                        prod.product_family_code AS product_family_code,
                                        'RA01' AS rev_alloc_cd,
                                        Cast(EDW1.effective_start_date AS date) AS as_of_dt,
                                        NULL AS as_of_tm
                                    FROM 
									                      """ + dbParams("EEDDLLDDBB") + """.PRODUCT_MDM_PID_XREF EDW1
                                    INNER JOIN 
									                      """ + dbParams("EEDDLLDDBB") + """.dim_product_mdm EDW2
								                    ON  EDW1.product_key = EDW2.product_key
                                    INNER JOIN 
									                  """ + dbParams("EEDDLLDDBB") + """.PROD_MDM_HIER_ROLLUP EDW3
								                    ON  EDW2.product_hierarchy_mdm_id = EDW3.lowest_lvl_prod_hier_mdm_id
                                    INNER JOIN 
									                  """ + dbParams("EEDDLLDDBB") + """.Product_dn EDW4
									                  ON  EDW1.product_id = EDW4.formatted_product_id
                                    LEFT JOIN 
									                  """ + dbParams("EEDDLLDDBB") + """.bi_dim_product prod
									                  ON  prod.product_code=substr(EDW1.product_key, -9)
                                    WHERE ((EDW1.primary_product_flag = 'Y'
                                    AND EDW3.product_hier_level_1_name='Software')
                                    OR (EDW3.product_hier_level_1_name<>'Software'))
                                    AND EDW1.effective_end_date > CURRENT_TIMESTAMP
                                    GROUP BY 1, 2, 3, 4, 5, 6, 7,8, 9, 10, 11
                            		)SQ
                            	) a
                                WHERE r=1 """
    executor.executeQuery(query)

    query = """ WITH 
	                  total_cust
	                  AS
                        (SELECT
                             master_number
                        FROM
                             """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_gems_neos_customer_incident_data
                        GROUP BY
                             master_number
                        HAVING
                             sum(incident_count) > 5)
                INSERT OVERWRITE TABLE """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1_1
                    SELECT
                        master_number,
                        customer_name,
                        product_mdm_id,
                        product_name,
                        sum(incident_count) AS incident_count
                    FROM
                        (SELECT master_number,
                            customer_name,
                            product_id_fmt,
                            incident_count,
                            product_mdm_id,
                            product_name
                        FROM
                           (SELECT category.master_number,
                                category.customer_name,
                                category.product_id_fmt,
                                category.incident_count,
                                CASE
                                    WHEN category.product_mdm_id IS NULL
                                        AND pids.product_mdm_id IS NOT NULL THEN pids.product_mdm_id
                                    WHEN category.product_mdm_id IS NOT NULL
                                        AND pids.product_mdm_id IS NULL THEN category.product_mdm_id
                                    WHEN category.product_mdm_id IS NOT NULL
                                        AND pids.product_mdm_id IS NOT NULL THEN category.product_mdm_id
                                    ELSE category.product_mdm_id
                                END AS product_mdm_id,
                                CASE
                                    WHEN category.product_name IS NULL
                                        AND pids.product IS NOT NULL THEN pids.product
                                    WHEN category.product_name IS NOT NULL
                                        AND pids.product IS NULL THEN category.product_name
                                    WHEN category.product_name IS NOT NULL
                                        AND pids.product IS NOT NULL THEN category.product_name
                                    ELSE category.product_name
                                END AS product_name
                            FROM
                               (SELECT gems.master_number,
                                    cdm.customer_name,
                                    gems.product_id_fmt,
                                    gems.incident_count,
                                    pdm.product_mdm_id,
                                    pdm.product_name
                               FROM
                                    """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_gems_neos_customer_incident_data gems
                               INNER JOIN
                                    total_cust
                               ON total_cust.master_number=gems.master_number
                               INNER JOIN
                                    """ + dbParams("EEDDLLDDBB") + """.dim_customer_mdm cdm
                               ON gems.master_number=cdm.customer_nbr
                               LEFT JOIN
                                    """ + dbParams("EEDDLLDDBB") + """.dim_product_mdm pdm
                               ON lcase(trim(gems.product_name))=lcase(trim(pdm.product_name))
                               where cdm.customer_industry='HSR'
                           ) category
                           LEFT JOIN 
						                   """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_PID_step1 pids
                           ON pids.pid=category.product_id_fmt
                           ) a
                           ) a
                            GROUP BY
								                master_number,
                                customer_name,
                                product_mdm_id,
                                product_name """
    executor.executeQuery(query)

    query = """ INSERT
	                  OVERWRITE
					               TABLE
					                   """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1
      select inc.* from """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1_1 inc
      left join
      (
        select inc.master_number from (
      select distinct master_number
        from """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1_1 a where product_mdm_id is null
    and not exists (select "X" from """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1_1 b
      where b.product_mdm_id is not null and b.master_number=a.master_number)
    ) inc
    left join (select distinct master_cust_nbr from """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_tmp
      where asset_product_name NOT IN ('Unassigned') )rev
    on rev.master_cust_nbr=inc.master_number
    where rev.master_cust_nbr is null
    ) nonprod
    on inc.master_number=nonprod.master_number
    where nonprod.master_number is null"""

    executor.executeQuery(query)

    query = """ INSERT
	                  OVERWRITE
					               TABLE
					                   """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step2
                                 SELECT
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
                                     SUM(cost) as fml_cost
                                 FROM
								                     """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_with_pov
                                 WHERE
								                     accounting_yr_month=202009
                                 AND
                                     source_type='ERP_COST'
                                 GROUP BY
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
									                   accounting_yr_month """
    executor.executeQuery(query)

    query = """ WITH
                    get_tot_gems_neos_cust_incident_count
                    AS (
                        SELECT
                            sum(incident_count) as tot_gems_neos_cust_incident_count
                        FROM
                            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1
                        ),
                    seven_seg_cost_dtls
                    AS (
                        select
                            fml_cost as fml_cost_1,
                            seg_cost.*
                        from
                           ( select * from """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step2) seg_cost
                        )
                INSERT
                    OVERWRITE
                        TABLE
                            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step3
                                SELECT
                                    inc_ratio.*,
                                    incident_ratio*cost_details.fml_cost_1 as total_alloc_cost,
                                    cost_details.*
                                FROM
                                   (
                                    SELECT
                                        inc_data.master_number,
                                        inc_data.incident_count,
                                        COALESCE(incident_count,0)/get_tot_gems_neos_cust_incident_count.tot_gems_neos_cust_incident_count as incident_ratio,
                                        get_tot_gems_neos_cust_incident_count.tot_gems_neos_cust_incident_count as total_incident
                                    FROM (
                                        SELECT
                                            master_number,
                                            SUM(incident_count) as incident_count
                                        FROM
                                            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1
                                        WHERE
                                            global_sales_hot_cost_alloc_step1.product_mdm_id is null
                                        GROUP BY
                                            master_number
                                        ) inc_data,get_tot_gems_neos_cust_incident_count
                                    ) inc_ratio,seven_seg_cost_dtls cost_details """
    executor.executeQuery(query)

    query = """ WITH
                    tot_revenue
                    AS (
                        SELECT
                            inn.master_cust_nbr,
                            inn.tot_rev,
                            0.05 * abs(inn.tot_rev) as rev_per,
                            abs(inn.tot_rev) as abs_tot_rev
                        FROM
                           (
                            SELECT
                                master_cust_nbr,
                                sum(revenue) as tot_rev,
                                count(distinct asset_product_name) as total_count
                            FROM
                                """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_tmp
                            WHERE
                                asset_product_name NOT IN ('Unassigned')
                            GROUP BY
                                master_cust_nbr
                            ) inn
                        )
                INSERT
                    OVERWRITE
                        TABLE
                            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_step1_1
                                SELECT
                                    final.*
                                FROM
                                    (
                                    SELECT
                                        rev.*
                                    FROM
                                        (
                                        SELECT
                                            *
                                        FROM
                                            (
                                            SELECT
                                                sum(abs(inn.tot_revenue_cust_prd)) over (partition by inn.master_cust_nbr order by abs(inn.tot_revenue_cust_prd)) as running_sum,
                                                tot_revenue.tot_rev,
                                                tot_revenue.rev_per,
                                                inn.tot_revenue_cust_prd,
                                                tot_revenue.abs_tot_rev,
                                                inn.master_cust_nbr,
                                                inn.asset_product_name,
                                                inn.product_family,
                                                inn.asset_mdm_product_family,
                                                inn.asset_product_mdm_id,
                                                inn.product_class
                                            FROM
                                                (
                                                SELECT
                                                    master_cust_nbr,
                                                    asset_product_name,
                                                    product_family,
                                                    asset_mdm_product_family,
                                                    asset_product_mdm_id,
                                                    product_class,
                                                    sum(revenue) as tot_revenue_cust_prd
                                                FROM
                                                    """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_tmp
                                                WHERE
                                                    asset_product_name NOT IN ('Unassigned')
                                                GROUP BY
                                                    master_cust_nbr,
                                                    asset_product_name,
                                                    product_family,
                                                    asset_mdm_product_family,
                                                    asset_product_mdm_id,
                                                    product_class
                                                ) inn
                                                INNER JOIN 
												                            tot_revenue
											                          ON
												                            tot_revenue.master_cust_nbr = inn.master_cust_nbr
                                                ) ou
                                            WHERE
                                                running_sum >= rev_per
                                            ) rev
                                            INNER JOIN (
                                                SELECT
                                                    distinct master_number
                                                FROM
                                                    """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1
                                                WHERE
                                                    product_mdm_id is null
                                                ) inc_data on inc_data.master_number = rev.master_cust_nbr
                                            ) final
                                            LEFT JOIN (
                                                SELECT
                                                    distinct master_number,
                                                    product_mdm_id
                                                FROM
                                                    """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1
                                                WHERE
                                                    product_mdm_id is null
                                                ) inc_data on inc_data.master_number = final.master_cust_nbr
                                            AND 
											                          inc_data.product_mdm_id = final.asset_product_mdm_id
                                            WHERE
                                                inc_data.product_mdm_id is null """
    executor.executeQuery(query)

    query = """ WITH 
	            senerio1 
				AS (
                    SELECT
                        *
                    FROM
                        (
                        SELECT
                            *,
                            row_number() over (partition by master_cust_nbr order by tot_revenue_cust_prd desc) as dr
                        FROM
                            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_step1_1
                        WHERE
                            tot_rev = 0
                        AND running_sum > 0
                        ) a
                    WHERE
                        dr <= 3
                    ),
                main 
				AS (
                    SELECT
                        *
                    FROM
                        """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_step1_1
                    WHERE
                        tot_rev = 0
                    )
                INSERT 
				            OVERWRITE
                        TABLE 
					             	""" + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_step1
                            SELECT
                                *
                            FROM
                                (
                                SELECT
                                    main.*,
                                    row_number() over(order by main.tot_rev) as r
                                FROM
                                    main
                                LEFT JOIN senerio1 
								                ON
								                   senerio1.master_cust_nbr = main.master_cust_nbr
                                WHERE
                                    senerio1.master_cust_nbr is null
                                ) a
                            WHERE
                                r = 1
                            UNION
                            SELECT
                                *
                            FROM
                                senerio1
                            UNION
                            SELECT
                                *,
                                1 as r
                            FROM
                                """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_step1_1
                            WHERE
                                tot_rev != 0 """
    executor.executeQuery(query)
	
    query = """ WITH
                    get_tot_revenue
                    AS(
				            SELECT
                        master_cust_nbr,
                        cast(sum(tot_revenue_cust_prd) as double) as rev_on_customer
                    FROM
                        """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_step1
                    WHERE
                        asset_product_name NOT IN ('Unassigned')
                    GROUP BY
                        master_cust_nbr
                    )
                INSERT
                    OVERWRITE
                        TABLE """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_step2
                            SELECT rev.*,
                                coalesce(revenue/get_tot_revenue.rev_on_customer,0) as rev_ratio,
                                get_tot_revenue.rev_on_customer as total_revenue
                            FROM (
                                SELECT
                                    master_cust_nbr,
                                    asset_product_name,
                                    product_family,
                                    asset_mdm_product_family,
                                    asset_product_mdm_id,
                                    product_class,
                                    sum(tot_revenue_cust_prd) as revenue
                                FROM
                                    """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_step1
                                WHERE
                                    asset_product_name NOT IN ('Unassigned')
                                GROUP BY
                                    master_cust_nbr,
                                    asset_product_name,
                                    product_family,
                                    asset_mdm_product_family,
                                    asset_product_mdm_id,
                                    product_class
                                ) rev
                            INNER JOIN
                                get_tot_revenue
                            ON
                                get_tot_revenue.master_cust_nbr=rev.master_cust_nbr """
    executor.executeQuery(query)


    query = """ INSERT
                    OVERWRITE
                        TABLE
                            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step4
                                SELECT 
								                    cost_dtls.*,
                                    CASE 
									                  WHEN rev_dtls.master_cust_nbr is not null THEN cost_dtls.total_alloc_cost*rev_dtls.rev_ratio
                                        ELSE cost_dtls.total_alloc_cost
                                    END as allocated_cost_prdct_wise,
                                    rev_dtls.master_cust_nbr,
                                    coalesce(rev_dtls.asset_product_name,"Unassigned") as asset_product_name,
                                    coalesce(rev_dtls.rev_ratio,0) as rev_ratio,
                                    coalesce(rev_dtls.revenue,0) as revenue,
                                    coalesce(rev_dtls.total_revenue,0) as total_revenue,
                                    coalesce(rev_dtls.asset_product_mdm_id,-3) as asset_product_mdm_id, 
                                    coalesce(rev_dtls.product_family,"Unassigned") as product_family,
                                    coalesce(rev_dtls.product_class,"Unassigned") as product_class,
                                    coalesce(rev_dtls.asset_mdm_product_family,"Unassigned") as asset_mdm_product_family
                                FROM
                                    """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step3 cost_dtls
                                INNER JOIN
                                    """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_revenue_step2 rev_dtls
                                ON
                                    cost_dtls.master_number = rev_dtls.master_cust_nbr """
    executor.executeQuery(query)

    query = """ WITH
                    customer_count
                    AS (
                        SELECT
                            master_number,
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
                            inter_entity_cd,
                            instance_id,
                            currency_cd,
                            exchange_rate,
                            acctg_period_name,
                            hfm_adj_cost_amt_us,
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
                            gl_posted_date,
                            accounting_yr_month,
                            currency_cd_entered,
                            count(master_number) as cmaster
                        FROM
                            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step4
                        WHERE
                            abs(allocated_cost_prdct_wise)=0
                        GROUP BY
                            master_number,
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
                            inter_entity_cd,
                            instance_id,
                            currency_cd,
                            exchange_rate,
                            acctg_period_name,
                            hfm_adj_cost_amt_us,
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
                            gl_posted_date,
                            accounting_yr_month,
                            currency_cd_entered
                        )

                INSERT
                    OVERWRITE
                        TABLE
						                """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step5
                                SELECT
                                    cust_list.total_alloc_cost/customer_count.cmaster as allocated_cost_prdct_wise_1,
                                    cust_list.*
                                FROM (
                                    SELECT *
                                    FROM
                                        """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step4
                                    WHERE
                                        abs(allocated_cost_prdct_wise)=0
                                    ) cust_list
                                INNER JOIN customer_count
                                ON
                                    cust_list.master_number=customer_count.master_number
                                AND
                                    cust_list.fml_org_cd=customer_count.fml_org_cd
                                AND
                                    cust_list.fml_acct_cd=customer_count.fml_acct_cd
                                AND
                                    cust_list.csub_acct_cd=customer_count.csub_acct_cd
                                AND
                                    cust_list.prod_srvc_cd=customer_count.prod_srvc_cd
                                AND
                                    cust_list.rptg_sobp_cd=customer_count.rptg_sobp_cd
                                AND
                                    cust_list.fin_proj_id=customer_count.fin_proj_id
                                AND
                                    cust_list.source_type=customer_count.source_type
                                AND
                                    cust_list.accounting_yr_month=customer_count.accounting_yr_month
                                AND
                                    cust_list.currency_cd=customer_count.currency_cd
                                AND
                                    cust_list.industry=customer_count.industry """
    executor.executeQuery(query)


    query = """ INSERT
                    OVERWRITE
                        TABLE
                            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_final_1
                                select
                                    final_load.*
                                from (
                                    select
                                        master_number ,
                                        incident_count ,
                                        incident_ratio ,
                                        total_incident ,
                                        total_alloc_cost ,
                                        fml_cost_1 ,
                                        fml_org_cd,
                                        fml_acct_cd ,
                                        csub_acct_cd,
                                        prod_srvc_cd,
                                        rptg_sobp_cd ,
                                        fin_proj_id ,
                                        source_type ,
                                        category ,
                                        gl_code_combination_id,
                                        hfm_custom3,
                                        instance_id,
                                        inter_entity_cd ,
                                        currency_cd ,
                                        exchange_rate ,
                                        hfm_adj_cost_amt_us,
                                        acctg_period_name,
                                        down2_dept_name,
                                        down2_dept_code,
                                        down2_dept_code_name,
                                        down3_dept_name ,
                                        down3_dept_code ,
                                        down3_dept_code_name ,
                                        down4_dept_name,
                                        down4_dept_code ,
                                        down4_dept_code_name,
                                        down5_dept_name ,
                                        down5_dept_code,
                                        down5_dept_code_name,
                                        down6_dept_name ,
                                        down6_dept_code,
                                        down6_dept_code_name,
                                        down7_dept_name,
                                        down7_dept_code,
                                        down7_dept_code_name ,
                                        dataset,
                                        sub_region_name,
                                        country_cd,
                                        country_name,
                                        currency_cd_entered ,
                                        gl_posted_date ,
                                        accounting_yr_month,
                                        fml_cost ,
                                        allocated_cost_prdct_wise ,
                                        master_cust_nbr ,
                                        asset_product_name,
                                        rev_ratio ,
                                        revenue ,
                                        total_revenue,
                                        product_family,
                                        asset_mdm_product_family,
                                        asset_product_mdm_id,
                                        product_class,
                                        industry
                                    from
                                        """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step4
                                    where
                                        abs(allocated_cost_prdct_wise)!=0
                                    union
                                    select
                                        master_number,
                                        incident_count,
                                        incident_ratio,
                                        total_incident,
                                        total_alloc_cost,
                                        fml_cost_1,
                                        fml_org_cd ,
                                        fml_acct_cd ,
                                        csub_acct_cd ,
                                        prod_srvc_cd ,
                                        rptg_sobp_cd ,
                                        fin_proj_id ,
                                        source_type,
                                        category,
                                        gl_code_combination_id ,
                                        hfm_custom3,
                                        instance_id,
                                        inter_entity_cd,
												                currency_cd,
												                exchange_rate ,
											                	hfm_adj_cost_amt_us ,
											                	acctg_period_name,
												                down2_dept_name ,
												                down2_dept_code ,
												                down2_dept_code_name ,
												                down3_dept_name  ,
												                down3_dept_code  ,
												                down3_dept_code_name,
												                down4_dept_name  ,
												                down4_dept_code  ,
												                down4_dept_code_name ,
												                down5_dept_name  ,
												                down5_dept_code      ,
											                	down5_dept_code_name ,
												                down6_dept_name      ,
												                down6_dept_code      ,
												                down6_dept_code_name ,
												                down7_dept_name      ,
												                down7_dept_code       ,
												                down7_dept_code_name ,
												                dataset ,
                                        sub_region_name,
                                        country_cd,
                                        country_name,
												                currency_cd_entered   ,
												                gl_posted_date        ,
												                accounting_yr_month   ,
												                fml_cost    ,
												                allocated_cost_prdct_wise_1 as allocated_cost_prdct_wise ,
												                master_cust_nbr,
												                asset_product_name  ,
												                rev_ratio ,
												                revenue  ,
												                total_revenue ,
											                 	product_family ,
												                asset_mdm_product_family,
                                        asset_product_mdm_id,
                                        product_class,
                                        industry
												            from
												                """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step5
												            ) final_load """
    executor.executeQuery(query)

    query = """ with
                    get_tot_gems_neos_cust_incident_count
                    as (
                        select
                            sum(incident_count) as tot_gems_neos_cust_incident_count
                        from 
						            """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1
                        ),
                    seven_seg_cost_dtls 
				           	as (
                        select
                            fml_cost as fml_cost_1,
                            seg_cost.*
                        from 
                            (select * from """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step2) seg_cost
                        )
               INSERT
                    OVERWRITE
                        TABLE """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_final_2
                            select
                                inc_ratio.*,
                                incident_ratio*cost_details.fml_cost_1 as total_alloc_cost,
                                cost_details.*
                            from
                                (
                                select
                                    inc_data.master_number,
                                    inc_data.incident_count,
                                    coalesce(incident_count,0)/get_tot_gems_neos_cust_incident_count.tot_gems_neos_cust_incident_count as incident_ratio,
                                    get_tot_gems_neos_cust_incident_count.tot_gems_neos_cust_incident_count as total_incident,
                                    inc_data.product_name,
                                    inc_data.product_mdm_id
                                from (
                                    select
                                        master_number,
                                        incident_count,
                                        product_name,
                                        product_mdm_id
                                    from
                                        """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_cost_alloc_step1
                                    where
                                        global_sales_hot_cost_alloc_step1.product_mdm_id is not null
                                    ) inc_data,get_tot_gems_neos_cust_incident_count
                                ) inc_ratio,seven_seg_cost_dtls cost_details """
    executor.executeQuery(query)

  }
}

object globalsaleshotothercustomerallocationObjRunner{

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
    var executor = new ExecutorDao(activityCountRequired, "global_sales_hot_other_customer_allocation")
    var dbParams = executor.loadProperties(propertyFilePath)
    var job_start_time = new Timestamp(new Date().getTime())
    println("################################################################")
    println("Started Job 'globalsaleshotothercustomerallocation' at %s.".format(job_start_time.toString()))
    var globalsaleshotothercustomerallocationObj = new globalsaleshotothercustomerallocation(executor, dbParams)
    try {
      globalsaleshotothercustomerallocationObj.run()
    } finally {
      var job_end_time = new Timestamp(new Date().getTime())
      println("Completed 'globalsaleshotothercustomerallocation' at %s.".format(job_end_time.toString()))
      println("Total Job Execution Time: %s".format(executor.getExecutionTime(job_start_time, job_end_time)))
      executor.postProcess()
      println("################################################################")
    }
  }
}
