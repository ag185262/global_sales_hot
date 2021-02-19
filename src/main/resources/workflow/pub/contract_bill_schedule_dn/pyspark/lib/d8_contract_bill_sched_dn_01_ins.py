from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractbillscheddn01inssql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t1"""
        print("Job: 'd8_contract_bill_sched_dn_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t1           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.author_oper_unit_country_code,
                    EDW1.bill_to_site_use_id,
                    CAST (EDW1.end_date_time AS date) AS auto_c04,
                    EDW1.contract_est_amt_ent,
                    EDW1.contract_nbr,
                    EDW1.contract_nbr_modifier,
                    CAST (EDW1.start_date_time AS date) AS auto_c08,
                    EDW1.curr_code,
                    EDW1.cust_po_nbr,
                    EDW1.invoice_rule_id,
                    EDW1.migration_contract_nbr,
                    CAST (EDW1.renew_date_time AS date) AS auto_c013,
                    EDW1.renewal_type_code,
                    EDW1.solution_portfolio_id,
                    EDW1.status_code,
                    EDW1.tran_code  
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.contract_header_ld    AS EDW1   
                WHERE
                    upper(trim(EDW1.tran_code)) <> 'D'
                    AND upper(trim(EDW1.subclass_code)) = 'SERVICE'
                    AND upper(trim(EDW1.status_code))  IN (
                        'ACTIVE' , 'EXPIRED' , 'HOLD' , 'QA_HOLD' , 'SIGNED' , 'TERMINATED'  
                    )   
                    """
        print("Job: 'd8_contract_bill_sched_dn_01'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t2"""
        print("Job: 'd8_contract_bill_sched_dn_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ with CTE AS 
                    (SELECT alt_contract_header_id,instance_id,contract_line_id,object1_id1, row_number() over(partition by alt_contract_header_id, instance_id,contract_line_id order by alt_contract_header_id,instance_id,contract_line_id ) rnk
                    from """ + db_params.EEDDWWDDBB2 + """.contract_party_role_ld EDW5 where upper(trim(EDW5.role_code)) = 'CUSTOMER' AND upper(trim(EDW5.contract_line_id)) = upper(trim(' ')) AND upper(trim(EDW5.tran_code)) <> 'D'
                    )
              
        INSERT INTO """ + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t2 
           
        SELECT 
          EDW1.contract_header_id, 
          EDW1.instance_id, 
          EDW1.author_oper_unit_country_code, 
          EDW1.bill_to_site_use_id, 
          EDW1.contract_end_date, 
          EDW1.contract_est_amt_ent, 
          EDW1.contract_nbr, 
          EDW1.contract_nbr_modifier, 
          EDW1.contract_start_date, 
          EDW1.curr_code, 
          EDW1.cust_po_nbr, 
          EDW1.invoice_rule_id, 
          EDW1.migration_contract_nbr, 
          EDW1.renew_date, 
          EDW1.renewal_type_code, 
          EDW1.solution_portfolio_id, 
          EDW1.status_code, 
          EDW1.tran_code, 
          COALESCE(EDW2.renewal_type_name, ' '), 
          COALESCE(EDW3.invoice_rule_desc, ' '), 
          COALESCE(EDW3.invoice_rule_name, ' '), 
          COALESCE(EDW4.cust_trx_type_id, ' '), 
          COALESCE(EDW4.tax_exempt_status_code, ' '), 
          COALESCE(EDW4.tax_exemption_id, 0) , 
          CASE WHEN EDW4.cust_trx_type_id = '1699' THEN 'MO-Invoice-OKS' WHEN EDW4.cust_trx_type_id = '1701' THEN 'BN-Invoice-OKS' WHEN EDW4.cust_trx_type_id = '1824' THEN 'LU-Invoice-OKS' ELSE ' ' END , 
          COALESCE(EDW4.hold_bill_flag, ' ') , 
          COALESCE(CAST(EDW5.object1_id1 AS DECIMAL(18, 0)), 0) , 
          COALESCE(EDW6.oper_unit_country_code, ' ') , 
          COALESCE(EDW6.address_line_1, ' ') , 
          COALESCE(EDW6.address_line_2, ' ') , 
          COALESCE(EDW6.address_line_3, ' ') , 
          COALESCE(EDW6.address_line_4, ' ') , 
          COALESCE(EDW6.city_name, ' ') , 
          COALESCE(EDW6.contract_collctn_org_code, ' '), 
          COALESCE(EDW6.country_code, ' '), 
          COALESCE(EDW6.county_name, ' '), 
          COALESCE(EDW6.cs_fml_organization_code, ' ') , 
          COALESCE(EDW6.cust_industry_code, ' ') , 
          COALESCE(EDW6.cust_industry_name, ' ') , 
          COALESCE(EDW6.customer_name, ' ') , 
          COALESCE(EDW6.customer_nbr, ' ') , 
          COALESCE(EDW6.customer_site_name, ' ') , 
          COALESCE(EDW6.customer_site_nbr, ' ') , 
          COALESCE(EDW6.customer_type_code, ' ') , 
          COALESCE(EDW6.fml_organization_code, ' ') , 
          COALESCE(EDW6.oper_unit_country_code, ' ') , 
          COALESCE(EDW6.party_id, 0) , 
          COALESCE(EDW6.party_site_addr_id, 0) , 
          COALESCE(EDW6.postal_code, ' ') , 
          COALESCE(EDW6.province_name, ' ') , 
          COALESCE(EDW6.state_name, ' '), 
          COALESCE(EDW6.store_nbr, ' ') , 
          COALESCE(EDW6.service_loc_code, ' ') , 
          COALESCE(EDW6.service_territory_id, 0) , 
          COALESCE(EDW8.contract_group_name, ' ') 
        FROM 
          """ + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t1 EDW1 
          LEFT OUTER JOIN """ + db_params.EEDDWWDDBB2 + """.renewal_type EDW2 ON upper(trim(EDW1.renewal_type_code)) = upper(trim(EDW2.renewal_type_code)) AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))
          LEFT OUTER JOIN """ + db_params.EEDDWWDDBB2 + """.invoice_rule EDW3 ON EDW1.invoice_rule_id = EDW3.invoice_rule_id AND upper(trim(EDW1.instance_id)) = upper(trim(EDW3.instance_id))
          LEFT OUTER JOIN """ + db_params.EEDDWWDDBB2 + """.contract_header_svc_supp_ld EDW4 ON upper(trim(EDW1.contract_header_id)) = upper(trim(EDW4.contract_header_id)) AND upper(trim(EDW1.instance_id)) = upper(trim(EDW4.instance_id)) AND upper(trim(EDW4.tran_code)) <> 'D' 
          LEFT OUTER JOIN CTE EDW5 ON upper(trim(EDW1.contract_header_id)) = upper(trim(EDW5.alt_contract_header_id)) AND upper(trim(EDW1.instance_id)) = upper(trim(EDW5.instance_id)) AND EDW5.rnk=1
          LEFT OUTER JOIN """ + db_params.EEDDWWDDBB2 + """.cust_acct_site_use_dn EDW6 ON EDW1.bill_to_site_use_id = EDW6.cust_acct_site_use_id AND upper(trim(EDW1.instance_id)) = upper(trim(EDW6.instance_id)) AND upper(trim(EDW6.cust_acct_site_use_code)) = 'BILL_TO' 
          LEFT OUTER JOIN 
          (SELECT upper(trim(included_contract_header_id)) as included_contract_header_id, parent_contract_group_id, upper(trim(instance_id)) as instance_id, contract_group_xref_id, ROW_NUMBER() OVER (PARTITION BY upper(trim(included_contract_header_id)), instance_id ORDER BY contract_group_xref_id ASC) AS ROW_NUM FROM """ + db_params.EEDDWWDDBB2 + """.contract_group_xref_ld WHERE upper(trim(tran_code)) <> 'D') EDW7 
          ON 
          upper(trim(EDW1.contract_header_id)) = EDW7.included_contract_header_id AND upper(trim(EDW1.instance_id)) = EDW7.instance_id AND EDW7.ROW_NUM =1
          LEFT OUTER JOIN """ + db_params.EEDDWWDDBB2 + """.contract_group_lk_ld EDW8 ON EDW7.parent_contract_group_id = EDW8.contract_group_id and upper(trim(EDW7.instance_id))= upper(trim(EDW8.instance_id)) and upper(trim(EDW8.tran_code)) <> 'D'"""
        print("Job: 'd8_contract_bill_sched_dn_02'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t3"""
        print("Job: 'd8_contract_bill_sched_dn_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t3           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.author_oper_unit_country_code,
                    EDW1.bill_to_site_use_id,
                    EDW1.contract_end_date,
                    EDW1.contract_est_amt_ent,
                    EDW1.contract_nbr,
                    EDW1.contract_nbr_modifier,
                    EDW1.contract_start_date,
                    EDW1.curr_code,
                    EDW1.cust_po_nbr,
                    EDW1.invoice_rule_id,
                    EDW1.migration_contract_nbr,
                    EDW1.renew_date,
                    EDW1.renewal_type_code,
                    EDW1.solution_portfolio_id,
                    EDW1.status_code,
                    EDW1.tran_code,
                    EDW1.renewal_type_name,
                    EDW1.invoice_rule_desc,
                    EDW1.invoice_rule_name,
                    EDW1.cust_trx_type_id,
                    EDW1.hdr_tax_exempt_sts_code,
                    EDW1.hdr_tax_exemption_id,
                    EDW1.hdr_trx_type_name,
                    EDW1.hold_billing_flag,
                    EDW1.party_id,
                    EDW1.country_code,
                    EDW1.bill_to_addr_line_1,
                    EDW1.bill_to_addr_line_2,
                    EDW1.bill_to_addr_line_3,
                    EDW1.bill_to_addr_line_4,
                    EDW1.bill_to_city_name,
                    EDW1.bill_to_collctn_org_code,
                    EDW1.bill_to_country_code,
                    EDW1.bill_to_county_name,
                    EDW1.bill_to_cs_fml_org_code,
                    EDW1.bill_to_cust_indstry_code,
                    EDW1.bill_to_cust_indstry_name,
                    EDW1.bill_to_cust_name,
                    EDW1.bill_to_cust_nbr,
                    EDW1.bill_to_cust_site_name,
                    EDW1.bill_to_cust_site_nbr,
                    EDW1.bill_to_cust_type_code,
                    EDW1.bill_to_fml_org_code,
                    EDW1.bill_to_oper_unit_country_code,
                    EDW1.bill_to_party_id,
                    EDW1.bill_to_party_site_addr_id,
                    EDW1.bill_to_postal_code,
                    EDW1.bill_to_province_name,
                    EDW1.bill_to_state_name,
                    EDW1.bill_to_store_nbr,
                    EDW1.bill_to_svc_loc_code,
                    EDW1.bill_to_svc_territory_id,
                    COALESCE( EDW2.area_code ,
                    ' ' ) AS auto_c055,
                    COALESCE( EDW2.area_desc ,
                    ' ' ) AS auto_c056,
                    COALESCE( EDW2.country_desc ,
                    ' ' ) AS auto_c057,
                    COALESCE( EDW2.region_code ,
                    ' ' ) AS auto_c058,
                    COALESCE( EDW2.region_desc ,
                    ' ' ) AS auto_c059,
                    COALESCE( EDW3.organization_name ,
                    ' ' ) AS auto_c060,
                    COALESCE( EDW4.organization_name ,
                    ' ' ) AS auto_c061,
                    COALESCE( EDW5.customer_exemption_nbr ,
                    ' ' ) AS auto_c062,
                    COALESCE( EDW5.reason_code ,
                    ' ' ) AS auto_c063,
                    COALESCE( EDW6.party_name ,
                    ' ' ) AS auto_c064,
                    COALESCE( EDW7.customer_nbr ,
                    ' ' ) AS auto_c065,
                    EDW1.contract_group_name  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t2    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.geo_rollup_xref    AS EDW2    
                        ON EDW1.country_code = EDW2.country_code  
                        AND upper(trim(EDW2.business_unit_code)) = upper(trim('B4'))    
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.ncr_organization    AS EDW3    
                        ON upper(trim(EDW1.bill_to_cs_fml_org_code)) = upper(trim(EDW3.fml_organization_code))   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.ncr_organization    AS EDW4    
                        ON upper(trim(EDW1.bill_to_fml_org_code)) = upper(trim(EDW4.fml_organization_code))   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB1 + """.tax_exemption    AS EDW5    
                        ON upper(trim(EDW1.instance_id)) = upper(trim(EDW5.instance_id))  
                        AND EDW1.hdr_tax_exemption_id = EDW5.tax_exemption_id
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.party    AS EDW6    
                        ON EDW1.party_id = EDW6.party_id
                        AND upper(trim(EDW1.instance_id)) = upper(trim(EDW6.instance_id))    
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.customer_account    AS EDW7    
                        ON EDW1.party_id = EDW7.party_id
                        AND upper(trim(EDW1.instance_id)) = upper(trim(EDW7.instance_id))"""
        print("Job: 'd8_contract_bill_sched_dn_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_21'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t21"""
        print("Job: 'd8_contract_bill_sched_dn_21'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t21           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    CL.parent_contract_line_id,
                    CL.contract_line_id,
                    CAST (CL.end_date_time AS date) AS auto_c04,
                    COALESCE( CL.contract_line_nbr ,
                    ' ' ) AS auto_c05,
                    CAST (CL.start_date_time AS date) AS auto_c06,
                    CAST (CL.termination_date_time AS date) AS auto_c07,
                    CL.status_code,
                    CL.contract_line_style_id,
                    CL.serviced_qty  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t1    AS EDW1   ,
                    """ + db_params.EEDDWWDDBB2 + """.contract_line_ld    AS CL   
                WHERE
                    EDW1.contract_header_id = CL.alt_contract_header_id  
                    AND EDW1.instance_id = CL.instance_id   
                    AND CL.contract_line_style_id  IN (
                        9,10  
                    )   
                    AND upper(trim(CL.tran_code)) <> upper(trim('D'))   
                    AND CL.contract_line_style_id  IN (
                        9,10  
                    )"""
        print("Job: 'd8_contract_bill_sched_dn_21'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_22'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t22"""
        print("Job: 'd8_contract_bill_sched_dn_22'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t21 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_22'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t21 COMPUTE STATISTICS  FOR COLUMNS service_line_id"""
        print("Job: 'd8_contract_bill_sched_dn_22'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t22           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    COALESCE( EDW2.contract_line_nbr ,
                    ' ' ) AS auto_c03,
                    COALESCE( EDW2.contract_line_style_id ,
                    0 ) AS auto_c04,
                    EDW2.serviced_qty  
                FROM
                    (   SELECT
                        contract_header_id,
                        service_line_id,
                        instance_id  
                    FROM
                        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t21      
                    GROUP BY
                        contract_header_id ,
                        service_line_id ,
                        instance_id      )    AS EDW1   ,
                    """ + db_params.EEDDWWDDBB2 + """.contract_line_ld    AS EDW2   
                WHERE
                    upper(trim(EDW1.service_line_id)) = upper(trim(EDW2.contract_line_id))  
                    AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))   
                    AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_bill_sched_dn_22'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_23'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t22 COMPUTE STATISTICS  FOR COLUMNS service_line_id"""
        print("Job: 'd8_contract_bill_sched_dn_23'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t22 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_23'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t22 COMPUTE STATISTICS  FOR COLUMNS service_line_style_id"""
        print("Job: 'd8_contract_bill_sched_dn_23'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ DROP TABLE IF EXISTS  """ + db_params.TTMMPPDDBB1 + """.EDW1"""
        print("Job: 'd8_contract_bill_sched_dn_23'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
            CREATE  TABLE IF NOT EXISTS  """ + db_params.TTMMPPDDBB1 + """.EDW1 STORED AS PARQUET        AS     SELECT
                V1.contract_header_id,
                V1.instance_id,
                V1.service_line_id,
                V1.service_line_nbr,
                V1.service_line_style_id,
                V2.line_style_name,
                V1.serviced_qty  
            FROM
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t22    AS V1   
            LEFT OUTER  JOIN
                """ + db_params.EEDDWWDDBB + """.contract_line_style_lk_ld    AS V2    
                    ON V1.instance_id = V2.instance_id  
                    AND V1.service_line_style_id = V2.contract_line_style_id   
                    AND upper(trim(V2.language_code)) = upper(trim('US'))   
                    AND upper(trim(V2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_bill_sched_dn_23'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t23"""
        print("Job: 'd8_contract_bill_sched_dn_23'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t23           SELECT
                    V3.contract_header_id,
                    V3.instance_id,
                    V3.service_line_id,
                    V3.service_line_nbr,
                    V3.service_line_style_id,
                    V3.line_style_name,
                    EDW3.tax_exempt_status_code,
                    EDW3.tax_exemption_id,
                    EDW3.vat_tax_id,
                    V3.serviced_qty  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.EDW1    AS V3   
                LEFT OUTER  JOIN
                    (
                        SELECT
                            tax_exempt_status_code,
                            tax_exemption_id,
                            vat_tax_id,
                            instance_id,
                            contract_line_id  
                        FROM
                            """ + db_params.EEDDWWDDBB + """.contract_line_svc_supp_ld     
                        WHERE
                            upper(trim(contract_line_id)) <> upper(trim(' '))  
                            AND upper(trim(tran_code)) <> upper(trim('D'))   
                        GROUP BY
                            tax_exempt_status_code ,
                            tax_exemption_id ,
                            vat_tax_id ,
                            instance_id ,
                            contract_line_id      
                    )    AS EDW3    
                        ON V3.service_line_id = EDW3.contract_line_id  
                        AND V3.instance_id = EDW3.instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_23'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_cfs_invtry_item_xref'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_invtry_item_xref"""
        print("Job: 'd8_cfs_invtry_item_xref'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_invtry_item_xref           SELECT
                    instance_id,
                    inventory_item_id,
                    inventory_org_id,
                    offering_id,
                    offering_id_hyphenated  
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.invtry_item"""
        print("Job: 'd8_cfs_invtry_item_xref'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_24'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t24"""
        print("Job: 'd8_contract_bill_sched_dn_24'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t23 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_24'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t23 COMPUTE STATISTICS  FOR COLUMNS vat_tax_id"""
        print("Job: 'd8_contract_bill_sched_dn_24'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t23 COMPUTE STATISTICS  FOR COLUMNS tax_exemption_id"""
        print("Job: 'd8_contract_bill_sched_dn_24'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t24           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.service_line_nbr,
                    EDW1.service_line_style_id,
                    EDW1.service_line_style_name,
                    EDW1.tax_exempt_status_code,
                    EDW1.tax_exemption_id,
                    EDW1.vat_tax_id,
                    COALESCE( EDW2.customer_exemption_nbr ,
                    ' ' ) AS auto_c09,
                    COALESCE( EDW2.reason_code ,
                    ' ' ) AS auto_c010,
                    COALESCE( EDW2.tax_code ,
                    ' ' ) AS auto_c011,
                    COALESCE( EDW3.vat_tax_code ,
                    ' ' ) AS auto_c012,
                    COALESCE( EDW4.obj_id1 ,
                    0 ) AS auto_c013,
                    COALESCE( EDW4.obj_id2 ,
                    0 ) AS auto_c014,
                    COALESCE( EDW5.offering_id ,
                    ' ' ) AS auto_c015,
                    COALESCE( EDW5.offering_id_hyphenated ,
                    ' ' ) AS auto_c016,
                    EDW1.serviced_qty  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t23    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.tax_exemption    AS EDW2    
                        ON EDW1.instance_id = EDW2.instance_id  
                        AND EDW1.tax_exemption_id = EDW2.tax_exemption_id    
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB2 + """.ar_vat_tax    AS EDW3    
                        ON EDW1.instance_id = EDW3.instance_id  
                        AND EDW1.vat_tax_id = EDW3.vat_tax_id    
                LEFT OUTER  JOIN
                    (
                        SELECT
                            contract_line_id,
                            instance_id,
                            SUBSTR( ROUND(INT(object1_id1)),1,18) AS obj_id1,
                            SUBSTR( ROUND(INT(object1_id2)),1,18) AS obj_id2
                        FROM
                            """ + db_params.EEDDWWDDBB2 + """.contract_line_xref_ld     
                        WHERE
                            upper(trim(object1_code)) = upper(trim('OKX_SERVICE'))  
                            AND upper(trim(tran_code)) <> upper(trim('D'))         
                    )    AS EDW4    
                        ON EDW1.service_line_id = EDW4.contract_line_id  
                        AND EDW1.instance_id = COALESCE( EDW4.instance_id ,
                    '' )     
                LEFT OUTER  JOIN
                    (
                        SELECT
                            inventory_item_id,
                            inventory_org_id,
                            instance_id,
                            offering_id,
                            offering_id_hyphenated  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_invtry_item_xref            
                    )    AS EDW5    
                        ON COALESCE( EDW4.instance_id ,
                    '' )  = EDW5.instance_id  
                    AND EDW4.obj_id1 = EDW5.inventory_item_id   
                    AND EDW4.obj_id2 = EDW5.inventory_org_id     
                GROUP BY
                    EDW1.contract_header_id ,
                    EDW1.instance_id ,
                    EDW1.service_line_id ,
                    EDW1.service_line_nbr ,
                    EDW1.service_line_style_id ,
                    EDW1.service_line_style_name ,
                    EDW1.tax_exempt_status_code ,
                    EDW1.tax_exemption_id ,
                    EDW1.vat_tax_id ,
                    COALESCE( EDW2.customer_exemption_nbr ,
                    ' ' ) ,
                    COALESCE( EDW2.reason_code ,
                    ' ' ) ,
                    COALESCE( EDW2.tax_code ,
                    ' ' ) ,
                    COALESCE( EDW3.vat_tax_code ,
                    ' ' ) ,
                    COALESCE( EDW4.obj_id1 ,
                    0 ) ,
                    COALESCE( EDW4.obj_id2 ,
                    0 ) ,
                    COALESCE( EDW5.offering_id ,
                    ' ' ) ,
                    COALESCE( EDW5.offering_id_hyphenated ,
                    ' ' ) ,
                    EDW1.serviced_qty"""
        print("Job: 'd8_contract_bill_sched_dn_24'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t24 COMPUTE STATISTICS  FOR COLUMNS contract_header_id  , instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_24'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB + """ """
        print("Job: 'd8_contract_bill_sched_dn_25'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t25"""
        print("Job: 'd8_contract_bill_sched_dn_25'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t25           SELECT
                    EDW2.contract_header_id,
                    EDW2.instance_id,
                    EDW2.service_line_id,
                    EDW1.author_oper_unit_country_code,
                    EDW1.bill_to_site_use_id,
                    EDW1.contract_end_date,
                    EDW1.contract_est_amt_ent,
                    EDW1.contract_nbr,
                    EDW1.contract_nbr_modifier,
                    EDW1.contract_start_date,
                    EDW1.curr_code,
                    EDW1.cust_po_nbr,
                    EDW1.invoice_rule_id,
                    EDW1.migration_contract_nbr,
                    EDW1.renew_date,
                    EDW1.renewal_type_code,
                    EDW1.solution_portfolio_id,
                    EDW1.status_code,
                    EDW1.tran_code,
                    EDW1.renewal_type_name,
                    EDW1.invoice_rule_desc,
                    EDW1.invoice_rule_name,
                    EDW1.cust_trx_type_id,
                    EDW1.hdr_tax_exempt_sts_code,
                    EDW1.hdr_tax_exemption_id,
                    EDW1.hdr_trx_type_name,
                    EDW1.hold_billing_flag,
                    EDW1.party_id,
                    EDW1.country_code,
                    EDW1.bill_to_addr_line_1,
                    EDW1.bill_to_addr_line_2,
                    EDW1.bill_to_addr_line_3,
                    EDW1.bill_to_addr_line_4,
                    EDW1.bill_to_city_name,
                    EDW1.bill_to_collctn_org_code,
                    EDW1.bill_to_country_code,
                    EDW1.bill_to_county_name,
                    EDW1.bill_to_cs_fml_org_code,
                    EDW1.bill_to_cust_indstry_code,
                    EDW1.bill_to_cust_indstry_name,
                    EDW1.bill_to_cust_name,
                    EDW1.bill_to_cust_nbr,
                    EDW1.bill_to_cust_site_name,
                    EDW1.bill_to_cust_site_nbr,
                    EDW1.bill_to_cust_type_code,
                    EDW1.bill_to_fml_org_code,
                    EDW1.bill_to_oper_unit_country_code,
                    EDW1.bill_to_party_id,
                    EDW1.bill_to_party_site_addr_id,
                    EDW1.bill_to_postal_code,
                    EDW1.bill_to_province_name,
                    EDW1.bill_to_state_name,
                    EDW1.bill_to_store_nbr,
                    EDW1.bill_to_svc_loc_code,
                    EDW1.bill_to_svc_territory_id,
                    EDW1.area_code,
                    EDW1.area_desc,
                    EDW1.country_desc,
                    EDW1.region_code,
                    EDW1.region_desc,
                    EDW1.bill_to_cs_fml_org_name,
                    EDW1.bill_to_fml_org_name,
                    EDW1.hdr_cust_exempt_nbr,
                    EDW1.hdr_exempt_reason_code,
                    EDW1.customer_name,
                    EDW1.customer_nbr,
                    EDW1.contract_group_name,
                    EDW2.service_line_nbr,
                    EDW2.service_line_style_id,
                    EDW2.service_line_style_name,
                    EDW2.tax_exempt_status_code,
                    EDW2.tax_exemption_id,
                    EDW2.vat_tax_id,
                    EDW2.customer_exemption_nbr,
                    EDW2.exemption_reason_code,
                    EDW2.exemption_tax_code,
                    EDW2.vat_tax_code,
                    EDW2.inventory_item_id,
                    EDW2.inventory_org_id,
                    EDW2.line_product_id,
                    EDW2.line_product_id_formatted,
                    EDW2.serviced_qty  
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t24    AS EDW2   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t3    AS EDW1    
                        ON upper(trim(EDW2.instance_id)) = upper(trim(EDW1.instance_id))  
                        AND upper(trim(EDW2.contract_header_id)) = upper(trim(EDW1.contract_header_id))"""
        print("Job: 'd8_contract_bill_sched_dn_25'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_41'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t41"""
        print("Job: 'd8_contract_bill_sched_dn_41'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t21 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_41'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t21 COMPUTE STATISTICS  FOR COLUMNS service_subline_id"""
        print("Job: 'd8_contract_bill_sched_dn_41'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t21 COMPUTE STATISTICS  FOR COLUMNS sline_line_style_id"""
        print("Job: 'd8_contract_bill_sched_dn_41'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ DROP TABLE IF EXISTS  """ + db_params.TTMMPPDDBB1 + """.EDW1"""
        print("Job: 'd8_contract_bill_sched_dn_41'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
            CREATE  TABLE IF NOT EXISTS  """ + db_params.TTMMPPDDBB1 + """.EDW1 STORED AS PARQUET        AS     SELECT
                V1.contract_header_id,
                V1.instance_id,
                V1.service_line_id,
                V1.service_subline_id,
                V1.sline_line_style_id,
                V2.line_style_name  
            FROM
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t21    AS V1   
            LEFT OUTER  JOIN
                """ + db_params.EEDDWWDDBB + """.contract_line_style_lk_ld    AS V2    
                    ON V1.instance_id = V2.instance_id  
                    AND V1.sline_line_style_id = V2.contract_line_style_id    
            WHERE
                upper(trim(V2.language_code)) = upper(trim('US'))  
                AND upper(trim(V2.tran_code)) <> upper(trim('D'))"""
        print("Job: 'd8_contract_bill_sched_dn_41'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t41           SELECT
                    V3.contract_header_id,
                    V3.instance_id,
                    V3.service_line_id,
                    V3.service_subline_id,
                    V3.sline_line_style_id,
                    V3.line_style_name,
                    EDW3.billing_seq_nbr,
                    EDW3.billing_stream_end_date,
                    EDW3.billing_stream_start_date,
                    EDW3.contr_bill_stream_id,
                    EDW3.interface_offset_day_nbr,
                    EDW3.billing_period_uom_cnt,
                    EDW3.billing_period_uom_code  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.EDW1    AS V3   
                LEFT OUTER  JOIN
                    (
                        SELECT
                            contract_line_id,
                            instance_id,
                            billing_seq_nbr,
                            billing_stream_end_date,
                            billing_stream_start_date,
                            contr_bill_stream_id,
                            interface_offset_day_nbr,
                            billing_period_uom_cnt,
                            billing_period_uom_code  
                        FROM
                            """ + db_params.EEDDWWDDBB + """.contr_bill_stream_ld    AS CBS   
                        WHERE
                            upper(trim(tran_code)) <> upper(trim('D'))        
                    )    AS EDW3    
                        ON upper(trim( V3.service_subline_id)) = upper(trim( EDW3.contract_line_id )) 
                        AND upper(trim( V3.instance_id)) = upper(trim( EDW3.instance_id))"""
        print("Job: 'd8_contract_bill_sched_dn_41'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_42'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t42"""
        print("Job: 'd8_contract_bill_sched_dn_42'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t41 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_42'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t41 COMPUTE STATISTICS  FOR COLUMNS service_subline_id"""
        print("Job: 'd8_contract_bill_sched_dn_42'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t42           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.service_subline_id,
                    EDW1.sline_line_style_id,
                    EDW1.sline_covered_lvl_name,
                    EDW1.sequence_nbr,
                    EDW1.billing_stream_end_date,
                    EDW1.billing_stream_start_date,
                    EDW1.contr_bill_stream_id,
                    EDW1.interface_offset_day_nbr,
                    EDW1.period_uom_cnt,
                    EDW1.period_uom_code,
                    CASE 
                        WHEN ( EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent ) > 0  THEN ( EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent )  
                        ELSE 0.0  
                    END AS auto_c013,
                    CASE 
                        WHEN ( EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent ) < 0  THEN ( EDW2.top_lvl_oprnd_prc_amt_ent - EDW2.top_lvl_adj_prc_amt_ent )  
                        ELSE 0.0  
                    END AS auto_c014,
                    EDW2.top_lvl_adj_prc_amt_ent,
                    EDW2.top_lvl_oprnd_prc_amt_ent,
                    EDW2.top_lvl_pricing_uom_qty,
                    EDW2.unbilled_amt_ent,
                    EDW2.credit_amt_ent  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t41    AS EDW1   
                LEFT OUTER  JOIN
                    (
                        SELECT
                            a.contract_line_id,
                            a.instance_id,
                            a.top_lvl_oprnd_prc_amt_ent,
                            a.top_lvl_adj_prc_amt_ent,
                            a.top_lvl_pricing_uom_qty,
                            a.unbilled_amt_ent,
                            a.credit_amt_ent  
                        FROM
                            (   SELECT
                                contract_line_id,
                                instance_id,
                                top_lvl_oprnd_prc_amt_ent,
                                top_lvl_adj_prc_amt_ent,
                                top_lvl_pricing_uom_qty,
                                unbilled_amt_ent,
                                credit_amt_ent  
                            FROM
                                """ + db_params.EEDDWWDDBB + """.contract_line_svc_supp_ld     
                            WHERE
                                upper(trim(tran_code)) <> upper(trim('D'))  
                                AND upper(trim(contract_line_id)) <> upper(trim(' '))         )    AS a    
                        JOIN
                            (
                                SELECT
                                    service_subline_id,
                                    instance_id  
                                FROM
                                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t41      
                                GROUP BY
                                    service_subline_id ,
                                    instance_id      
                            )    AS b    
                                ON upper(trim(a.contract_line_id)) = upper(trim(b.service_subline_id))  
                                AND upper(trim(a.instance_id)) = upper(trim(b.instance_id))           
                            )    AS EDW2    
                                ON upper(trim(EDW1.service_subline_id)) = upper(trim(EDW2.contract_line_id))  
                                AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))"""
        print("Job: 'd8_contract_bill_sched_dn_42'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_43_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t43_trg1"""
        print("Job: 'd8_contract_bill_sched_dn_43_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t42 COMPUTE STATISTICS  FOR COLUMNS contr_bill_stream_id  , instance_id  , instance_id,contr_bill_stream_id  , CONTRACT_HEADER_ID,INSTANCE_ID,SERVICE_LINE_ID,SERVICE_SUBLINE_ID,SEQUENCE_NBR"""
        print("Job: 'd8_contract_bill_sched_dn_43_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.EEDDWWDDBB2 + """.contract_lvl_element COMPUTE STATISTICS  FOR COLUMNS LEVEL_ELEMENT_AMT_ENT  , INTERFACE_DATE_TIME,PERIOD_START_DATE,SEQUENCE_NBR,PERIOD_END_DATE  , TRAN_CODE"""
        print("Job: 'd8_contract_bill_sched_dn_43_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.EEDDWWDDBB2 + """.contract_lvl_element_ld COMPUTE STATISTICS"""
        print("Job: 'd8_contract_bill_sched_dn_43_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t43_trg1           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW2.interface_date_time,
                    EDW2.sequence_nbr,
                    EDW2.period_end_date,
                    EDW2.period_start_date,
                    EDW1.sequence_nbr,
                    EDW1.service_line_id,
                    EDW1.service_subline_id,
                    MIN( EDW2.level_element_id ) AS auto_c09  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t42    AS EDW1   ,
                    """ + db_params.EEDDWWDDBB2 + """.contract_lvl_element_ld    AS EDW2   
                WHERE
                    upper(trim(EDW1.contr_bill_stream_id)) = upper(trim(EDW2.rule_id))  
                    AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))   
                    AND EDW1.contr_bill_stream_id  IS NOT NULL   
                    AND EDW2.level_element_amt_ent <> '0'   
                    AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))   
                    AND upper(trim(EDW2.rule_id)) <> upper(trim(' '))   
                GROUP BY
                    EDW1.contract_header_id ,
                    EDW1.instance_id ,
                    EDW2.interface_date_time ,
                    EDW2.sequence_nbr ,
                    EDW2.period_end_date ,
                    EDW2.period_start_date ,
                    EDW1.sequence_nbr ,
                    EDW1.service_line_id ,
                    EDW1.service_subline_id"""
        print("Job: 'd8_contract_bill_sched_dn_43_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_43'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t43"""
        print("Job: 'd8_contract_bill_sched_dn_43'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t43           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.service_subline_id,
                    EDW1.sline_line_style_id,
                    EDW1.sline_covered_lvl_name,
                    EDW1.sequence_nbr,
                    EDW1.billing_stream_end_date,
                    EDW1.billing_stream_start_date,
                    EDW1.contr_bill_stream_id,
                    EDW1.interface_offset_day_nbr,
                    EDW1.period_uom_cnt,
                    EDW1.period_uom_code,
                    EDW1.sl_discount_amt_ent,
                    EDW1.sl_surcharge_amt_ent,
                    EDW1.sl_top_lvl_adj_prc_amt_ent,
                    EDW1.sl_top_lvl_oprnd_prc_amt_ent,
                    EDW1.sl_top_lvl_pricing_uom_qty,
                    EDW1.sl_unbilled_amt_ent,
                    EDW1.credit_amt_ent,
                    CAST (EDW2.interface_date_time AS date) AS auto_c020,
                    EDW2.sequence_nbr,
                    EDW2.period_end_date,
                    EDW2.period_start_date,
                    CAST (EDW2.completed_date_time AS date) AS auto_c024,
                    EDW2.level_element_amt_ent  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t42    AS EDW1   ,
                    """ + db_params.EEDDWWDDBB + """.contract_lvl_element_ld    AS EDW2   ,
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t43_trg1    AS EDW3   
                WHERE
                    upper(trim(EDW1.contr_bill_stream_id)) = upper(trim(EDW2.rule_id )) 
                    AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id ))  
                    AND upper(trim(EDW1.contract_header_id)) = upper(trim(EDW3.contract_header_id  )) 
                    AND upper(trim(EDW1.instance_id)) = upper(trim(EDW3.instance_id  )) 
                    AND EDW1.sequence_nbr = EDW3.sequence_nbr 
                    AND upper(trim(EDW1.service_line_id)) = upper(trim(EDW3.service_line_id ))  
                    AND upper(trim(EDW1.service_subline_id)) = upper(trim(EDW3.service_subline_id))   
                    AND upper(trim(EDW2.level_element_id)) = upper(trim(EDW3.level_element_id ))  
                    AND upper(trim(EDW2.instance_id)) = upper(trim(EDW3.instance_id))"""
        print("Job: 'd8_contract_bill_sched_dn_43'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB2 + """ """
        print("Job: 'd8_contract_bill_sched_dn_44_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg1"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg1           SELECT
                    EDW1.instance_id,
                    EDW1.bill_instance_nbr,
                    CAST (EDW2.creation_date_time AS date) AS auto_c02,
                    EDW3.contract_line_id,
                    CAST (EDW3.bill_from_date_time AS date) AS auto_c04,
                    CAST (EDW3.bill_to_date_time AS date) AS auto_c05,
                    EDW3.bill_amt_ent  
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.contract_bill_invc_line_ld    AS EDW1   ,
                    """ + db_params.EEDDWWDDBB2 + """.contract_line_bill_ld    AS EDW2   ,
                    """ + db_params.EEDDWWDDBB2 + """.contract_subline_bill_ld    AS EDW3   
                WHERE
                    upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))  
                    AND upper(trim(EDW1.contract_line_bill_id)) = upper(trim(EDW2.contract_line_bill_id))   
                    AND upper(trim(EDW2.instance_id)) = upper(trim(EDW3.instance_id))   
                    AND upper(trim(EDW2.contract_line_bill_id)) = upper(trim(EDW3.contract_line_bill_id))   
                    AND upper(trim(EDW1.tran_code)) <> upper(trim('D'))   
                    AND upper(trim(EDW2.tran_code)) <> upper(trim('D'))   
                    AND upper(trim(EDW3.tran_code)) <> upper(trim('D'))   
                GROUP BY
                    EDW1.instance_id ,
                    EDW1.bill_instance_nbr ,
                    CAST (EDW2.creation_date_time AS date) ,
                    EDW3.contract_line_id ,
                    CAST (EDW3.bill_from_date_time AS date) ,
                    CAST (EDW3.bill_to_date_time AS date) ,
                    EDW3.bill_amt_ent"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg1 COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,BILL_INSTANCE_NBR"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg1 COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,BILL_INSTANCE_NBR,CREATION_DATE,SERVICE_SUBLINE_ID,BILL_FROM_DATE,BILL_TO_DATE"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB + """ """
        print("Job: 'd8_contract_bill_sched_dn_44_trg2'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg2"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg2'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg2           SELECT
                    EDW1.instance_id,
                    EDW1.bill_instance_nbr,
                    EDW2.invoice_type,
                    EDW2.invoice_date,
                    EDW2.invoice_nbr,
                    EDW2.invoice_currency_code  
                FROM
                    """ + db_params.EEDDWWDDBB2 + """.invoice_line    AS EDW1   ,
                    """ + db_params.EEDDWWDDBB2 + """.customer_invoice    AS EDW2   
                WHERE
                    upper(trim(EDW1.source_country_code)) = upper(trim(EDW2.source_country_code))  
                    AND EDW1.customer_trx_id = EDW2.customer_trx_id 
                    AND EDW1.bill_instance_nbr  IS NOT NULL   
                GROUP BY
                    EDW1.instance_id ,
                    EDW1.bill_instance_nbr ,
                    EDW2.invoice_type ,
                    EDW2.invoice_date ,
                    EDW2.invoice_nbr ,
                    EDW2.invoice_currency_code"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg2'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    WITH qq1 AS (    SELECT
                d8t_contr_bill_schd_dn_t8_keep.*  
            FROM
                """ + db_params.TTMMPPDDBB1 + """.d8t_contr_bill_schd_dn_t8_keep AS d8t_contr_bill_schd_dn_t8_keep
            LEFT OUTER JOIN
                (SELECT
                    DISTINCT instance_id AS auto_c00,
                    bill_instance_nbr AS auto_c01,
                    invoice_type_code AS auto_c02,
                    invoice_date AS auto_c03,
                    invoice_nbr AS auto_c04,
                    Invoice_currency_code AS auto_c05  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg2 ) AS autoAlias_37 
                    ON ( upper(trim(instance_id)) = upper(trim(autoAlias_37.AUTO_C00)) 
                    AND upper(trim(bill_instance_nbr)) = upper(trim(autoAlias_37.AUTO_C01)) 
                    AND upper(trim(invoice_type_code)) = upper(trim(autoAlias_37.AUTO_C02)) 
                    AND invoice_date = autoAlias_37.AUTO_C03 
                    AND upper(trim(invoice_nbr)) = upper(trim(autoAlias_37.AUTO_C04)) 
                    AND upper(trim(Invoice_currency_code)) = upper(trim(autoAlias_37.AUTO_C05)) ) 
            WHERE
                autoAlias_37.AUTO_C00 IS  NULL  
                AND autoAlias_37.AUTO_C01 IS  NULL  
                AND autoAlias_37.AUTO_C02 IS  NULL  
                AND autoAlias_37.AUTO_C03 IS  NULL  
                AND autoAlias_37.AUTO_C04 IS  NULL  
                AND autoAlias_37.AUTO_C05 IS  NULL         ) INSERT 
                INTO
                    TABLE
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg2       SELECT
                        * 
                    FROM
                        qq1"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg2'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg2 COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,BILL_INSTANCE_NBR"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg2'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_44_trg3'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg3"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg3'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg3           SELECT
                    EDW1.service_subline_id,
                    EDW1.bill_from_date,
                    EDW1.bill_to_date,
                    EDW1.instance_id,
                    EDW1.creation_date,
                    EDW2.invoice_type_code,
                    EDW2.invoice_date,
                    EDW2.invoice_nbr,
                    EDW2.invoice_currency_code,
                    SUM( EDW1.bill_amt_ent ) AS auto_c010,
                    EDW1.bill_instance_nbr  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg1    AS EDW1   
                INNER JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg2    AS EDW2   
                ON
                    EDW1.bill_instance_nbr = EDW2.bill_instance_nbr
                    AND EDW1.instance_id = EDW2.instance_id   
                GROUP BY
                    EDW1.service_subline_id ,
                    EDW1.bill_from_date ,
                    EDW1.bill_to_date ,
                    EDW1.instance_id ,
                    EDW1.creation_date ,
                    EDW2.invoice_type_code ,
                    EDW2.invoice_date ,
                    EDW2.invoice_currency_code ,
                    EDW2.invoice_nbr ,
                    EDW1.bill_instance_nbr"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg3'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg4 
        SELECT 
          EDW1.service_subline_id, 
          CASE WHEN EDW2.instance_id IS NOT NULL
          AND EDW2.service_subline_id IS NOT NULL
          AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.bill_from_date ELSE EDW1.bill_from_date END AS bill_from_date, 
          CASE WHEN EDW2.instance_id IS NOT NULL
          AND EDW2.service_subline_id IS NOT NULL
          AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.bill_to_date ELSE EDW1.bill_to_date END AS bill_to_date, 
          EDW1.instance_id, 
          CASE WHEN EDW2.instance_id IS NOT NULL
          AND EDW2.service_subline_id IS NOT NULL
          AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.creation_date ELSE EDW1.creation_date END AS creation_date, 
          CASE WHEN EDW2.instance_id IS NOT NULL
          AND EDW2.service_subline_id IS NOT NULL
          AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.invoice_type_code ELSE EDW1.invoice_type_code END AS invoice_type_code, 
          CASE WHEN EDW2.instance_id IS NOT NULL
          AND EDW2.service_subline_id IS NOT NULL
          AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.invoice_date ELSE EDW1.invoice_date END AS invoice_date, 
          CASE WHEN EDW2.instance_id IS NOT NULL
          AND EDW2.service_subline_id IS NOT NULL
          AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.invoice_nbr ELSE EDW1.invoice_nbr END AS invoice_nbr, 
          CASE WHEN EDW2.instance_id IS NOT NULL
          AND EDW2.service_subline_id IS NOT NULL
          AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.invoice_currency_code ELSE EDW1.invoice_currency_code END AS invoice_currency_code, 
          CASE WHEN EDW2.instance_id IS NOT NULL
          AND EDW2.service_subline_id IS NOT NULL
          AND EDW2.bill_instance_nbr IS NOT NULL THEN EDW2.bill_amt_ent ELSE EDW1.bill_amt_ent END AS bill_amt_ent, 
          EDW1.bill_instance_nbr 
        FROM 
          """ + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg4 AS EDW1 
          LEFT JOIN """ + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg3 AS EDW2 
          ON 
              EDW1.instance_id = EDW2.instance_id 
          AND EDW1.service_subline_id = EDW2.service_subline_id 
          AND EDW1.bill_instance_nbr = EDW2.bill_instance_nbr"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg3'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT INTO """ + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg4
        SELECT
                A.service_subline_id,
                A.bill_from_date,
                A.bill_to_date,
                A.instance_id,
                A.creation_date,
                A.invoice_type_code,
                A.invoice_date,
                A.invoice_nbr,
                A.invoice_currency_code,
                A.bill_amt_ent,
                A.bill_instance_nbr
            FROM
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg3 A 
        LEFT JOIN
                """ + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg4 B
        ON
           A.instance_id = B.instance_id AND A.service_subline_id = B.service_subline_id AND A.bill_instance_nbr = B.bill_instance_nbr
        WHERE B.instance_id IS NULL AND B.service_subline_id IS NULL AND B.bill_instance_nbr IS NULL"""
        print("Job: 'd8_contract_bill_sched_dn_44_trg3'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg4
        SELECT A.*
        FROM 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg4 A 
        LEFT JOIN 
        """ + db_params.EEDDWWDDBB + """.contract_subline_bill_ld B
        ON A.instance_id = B.instance_id AND A.service_subline_id = B.contract_line_id  AND B.tran_code='D'
        WHERE B.instance_id IS NULL AND B.contract_line_id IS NULL  """
        print("Job: 'd8_contract_bill_sched_dn_44_trg3'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_44'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t44"""
        print("Job: 'd8_contract_bill_sched_dn_44'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT INTO TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t44 
        SELECT 
     EDW1.contract_header_id
    ,EDW1.instance_id
    ,EDW1.service_line_id
    ,EDW1.service_subline_id
    ,EDW1.sline_line_style_id
    ,EDW1.sline_covered_lvl_name
    ,EDW1.sequence_nbr
    ,EDW1.billing_stream_end_date
    ,EDW1.billing_stream_start_date
    ,EDW1.contr_bill_stream_id
    ,EDW1.interface_offset_day_nbr
    ,EDW1.period_uom_cnt
    ,EDW1.period_uom_code
    ,EDW1.sl_discount_amt_ent
    ,EDW1.sl_surcharge_amt_ent
    ,EDW1.sl_top_lvl_adj_prc_amt_ent
    ,EDW1.sl_top_lvl_oprnd_prc_amt_ent
    ,EDW1.sl_top_lvl_pricing_uom_qty
    ,EDW1.sl_unbilled_amt_ent
    ,EDW1.credit_amt_ent
    ,EDW1.level_sequence_nbr
    ,EDW1.period_end_date
    ,EDW1.period_start_date 
    ,EDW2.creation_date 
    ,CASE 
        WHEN EDW2.bill_from_date <> EDW1.period_start_date
            AND EDW2.bill_from_date >= EDW1.period_start_date
            AND EDW2.bill_from_date <= EDW1.period_end_date
            AND upper(trim(EDW2.invoice_type_code)) IN ('CM','INV')
            THEN EDW2.creation_date
        ELSE EDW1.interface_date
        END 
    ,CASE 
        WHEN EDW2.invoice_type_code IS NULL
            THEN 'NOT BILLED'
        ELSE EDW2.invoice_type_code
        END 
    ,EDW2.bill_amt_ent 
    ,CASE 
        WHEN EDW2.bill_from_date <> EDW1.period_start_date
            AND EDW2.bill_from_date >= EDW1.period_start_date
            AND EDW2.bill_from_date <= EDW1.period_end_date
            AND upper(trim(EDW2.invoice_type_code)) IN ('CM','INV')
            THEN EDW2.creation_date
        ELSE EDW1.completed_date
        END 
    ,CASE 
        WHEN upper(trim(EDW2.invoice_type_code)) ='CM'
            THEN 0
        ELSE EDW1.level_element_amt_ent
        END 
    ,EDW2.invoice_currency_code 
    ,EDW2.invoice_date 
    ,EDW2.invoice_nbr

FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t43 EDW1

LEFT OUTER JOIN 
  (
    SELECT service_subline_id    ,instance_id    ,bill_from_date    ,creation_date   ,invoice_type_code    ,bill_amt_ent    ,invoice_currency_code    ,invoice_date    ,invoice_nbr
    FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sch_dn_t44_trg4
    GROUP BY 1,2,3,4,5,6,7,8,9

   ) EDW2 ON upper(trim(EDW2.service_subline_id)) = upper(trim(EDW1.service_subline_id))
    AND upper(trim(EDW2.instance_id)) = upper(trim(EDW1.instance_id))
    AND (EDW2.bill_from_date = EDW1.period_start_date
    OR (
    EDW2.bill_from_date <> EDW1.period_start_date
    AND EDW2.bill_from_date >= EDW1.period_start_date
     AND EDW2.bill_from_date <= EDW1.period_end_date)
        )"""
        print("Job: 'd8_contract_bill_sched_dn_44'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_44'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
                TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t44 
        SELECT
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contract_header_id AS contract_header_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.instance_id AS instance_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_line_id AS service_line_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_subline_id AS service_subline_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sline_line_style_id AS sline_line_style_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sline_covered_lvl_name AS sline_covered_lvl_name ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sequence_nbr AS sequence_nbr ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.billing_stream_end_date AS billing_stream_end_date ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.billing_stream_start_date AS billing_stream_start_date ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contr_bill_stream_id AS contr_bill_stream_id ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_offset_day_nbr AS interface_offset_day_nbr ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_uom_cnt AS period_uom_cnt ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_uom_code AS period_uom_code ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_discount_amt_ent AS sl_discount_amt_ent ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_surcharge_amt_ent AS sl_surcharge_amt_ent ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_adj_prc_amt_ent AS sl_top_lvl_adj_prc_amt_ent ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_oprnd_prc_amt_ent AS sl_top_lvl_oprnd_prc_amt_ent ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_top_lvl_pricing_uom_qty AS sl_top_lvl_pricing_uom_qty ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sl_unbilled_amt_ent AS sl_unbilled_amt_ent ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.credit_amt_ent AS credit_amt_ent ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_date AS interface_date ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_sequence_nbr AS level_sequence_nbr ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_end_date AS period_end_date ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_start_date AS period_start_date ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_on_date AS bill_on_date ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_type_code AS invoice_type_code ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_amt_ent AS bill_amt_ent ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.completed_date AS completed_date ,
                    CASE 
                        WHEN autoAlias_37.auto_c00 IS not null and autoAlias_37.auto_c01 IS not null and autoAlias_37.auto_c02 IS not null and autoAlias_37.auto_c03 IS not null and autoAlias_37.auto_c04 IS not null and autoAlias_37.auto_c05 IS not null and autoAlias_37.auto_c06 IS not null and autoAlias_37.auto_c07 IS not null and autoAlias_37.auto_c08 IS not null and autoAlias_37.auto_c09 IS not null and autoAlias_37.auto_c010 IS not null and autoAlias_37.auto_c011 is null THEN 0 
                        ELSE AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_element_amt_ent 
                    END AS level_element_amt_ent ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_currency_code AS invoice_currency_code ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_date AS invoice_date ,
                    AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_nbr AS invoice_nbr 
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t44 AS AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44 
                Left Outer Join
                    (
                        SELECT
                            bill_on_date AS auto_c00,
                            contract_header_id AS auto_c01,
                            instance_id AS auto_c02,
                            interface_date AS auto_c03,
                            invoice_type_code AS auto_c04,
                            level_sequence_nbr AS auto_c05,
                            period_end_date AS auto_c06,
                            period_start_date AS auto_c07,
                            sequence_nbr AS auto_c08,
                            service_line_id AS auto_c09,
                            service_subline_id AS auto_c010,
                            MIN( level_element_amt_ent ) AS auto_c011  
                        FROM
                            """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t44     
                        WHERE
                            UPPER(TRIM(INVOICE_TYPE_CODE)) = UPPER(TRIM('INV'))  
                        GROUP BY
                            bill_on_date ,
                            contract_header_id ,
                            instance_id ,
                            interface_date ,
                            invoice_type_code ,
                            level_sequence_nbr ,
                            period_end_date ,
                            period_start_date ,
                            sequence_nbr ,
                            service_line_id ,
                            service_subline_id 
                        HAVING
                            COUNT(*) > 1
                    )autoAlias_37 
                        ON (
                            autoAlias_37.auto_c00 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.bill_on_date 
                            AND UPPER(TRIM(autoAlias_37.auto_c01)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.contract_header_id)) 
                            AND UPPER(TRIM(autoAlias_37.auto_c02)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.instance_id ))
                            AND autoAlias_37.auto_c03= AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.interface_date 
                            AND UPPER(TRIM(autoAlias_37.auto_c04)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.invoice_type_code)) 
                            AND UPPER(TRIM(autoAlias_37.auto_c05)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_sequence_nbr)) 
                            AND autoAlias_37.auto_c06 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_end_date 
                            AND autoAlias_37.auto_c07 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.period_start_date 
                            AND autoAlias_37.auto_c08 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.sequence_nbr 
                            AND UPPER(TRIM(autoAlias_37.auto_c09)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_line_id)) 
                            AND UPPER(TRIM(autoAlias_37.auto_c010)) = UPPER(TRIM(AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.service_subline_id)) 
                            AND autoAlias_37.auto_c011 = AAPPLLIIDD1EENNVV_contr_bill_sched_dn_t44.level_element_amt_ent
                        )"""
                
        print("Job: 'd8_contract_bill_sched_dn_44'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_45'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t45"""
        print("Job: 'd8_contract_bill_sched_dn_45'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t45           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.service_subline_id,
                    EDW1.sline_line_style_id,
                    EDW1.sline_covered_lvl_name,
                    EDW1.sequence_nbr,
                    EDW1.billing_stream_end_date,
                    EDW1.billing_stream_start_date,
                    EDW1.contr_bill_stream_id,
                    EDW1.interface_offset_day_nbr,
                    EDW1.period_uom_cnt,
                    EDW1.period_uom_code,
                    EDW1.sl_discount_amt_ent,
                    EDW1.sl_surcharge_amt_ent,
                    EDW1.sl_top_lvl_adj_prc_amt_ent,
                    EDW1.sl_top_lvl_oprnd_prc_amt_ent,
                    EDW1.sl_top_lvl_pricing_uom_qty,
                    EDW1.sl_unbilled_amt_ent,
                    EDW1.credit_amt_ent,
                    EDW1.interface_date,
                    EDW1.level_sequence_nbr,
                    EDW1.period_end_date,
                    EDW1.period_start_date,
                    EDW1.bill_on_date,
                    EDW1.invoice_type_code,
                    EDW1.bill_amt_ent,
                    EDW1.completed_date,
                    EDW1.level_element_amt_ent,
                    EDW1.invoice_currency_code,
                    EDW1.invoice_date,
                    EDW1.invoice_nbr,
                    CASE 
                        WHEN EDW2.contract_line_style_id = 1  THEN EDW1.credit_amt_ent  
                        ELSE 0  
                    END AS auto_c032,
                    CASE 
                        WHEN EDW2.contract_line_style_id  IN (  9,
                        10  )  THEN EDW1.credit_amt_ent  
                        ELSE 0  
                    END AS auto_c033  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t44    AS EDW1   
                LEFT OUTER  JOIN
                    (
                        SELECT
                            contract_line_style_id,
                            contract_line_id,
                            instance_id,
                            tran_code  
                        FROM
                            """ + db_params.EEDDWWDDBB + """.contract_line_ld      
                        GROUP BY
                            contract_line_style_id ,
                            contract_line_id ,
                            instance_id ,
                            tran_code      
                    )    AS EDW2    
                        ON UPPER(TRIM(EDW1.service_subline_id)) = UPPER(TRIM(EDW2.contract_line_id))  
                        AND UPPER(TRIM(EDW1.instance_id)) = UPPER(TRIM(EDW2.instance_id))   
                        AND UPPER(TRIM(EDW2.tran_code)) <> 'D'"""
        print("Job: 'd8_contract_bill_sched_dn_45'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_46'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t46"""
        print("Job: 'd8_contract_bill_sched_dn_46'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t46           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.service_subline_id,
                    EDW1.sline_line_style_id,
                    EDW1.sline_covered_lvl_name,
                    EDW1.sequence_nbr,
                    EDW1.billing_stream_end_date,
                    EDW1.billing_stream_start_date,
                    EDW1.contr_bill_stream_id,
                    EDW1.interface_offset_day_nbr,
                    EDW1.period_uom_cnt,
                    EDW1.period_uom_code,
                    EDW1.sl_discount_amt_ent,
                    EDW1.sl_surcharge_amt_ent,
                    EDW1.sl_top_lvl_adj_prc_amt_ent,
                    EDW1.sl_top_lvl_oprnd_prc_amt_ent,
                    EDW1.sl_top_lvl_pricing_uom_qty,
                    EDW1.sl_unbilled_amt_ent,
                    EDW1.credit_amt_ent,
                    EDW1.interface_date,
                    EDW1.level_sequence_nbr,
                    EDW1.period_end_date,
                    EDW1.period_start_date,
                    EDW1.bill_on_date,
                    EDW1.invoice_type_code,
                    EDW1.bill_amt_ent,
                    EDW1.completed_date,
                    EDW1.level_element_amt_ent,
                    EDW1.invoice_currency_code,
                    EDW1.invoice_date,
                    EDW1.invoice_nbr,
                    EDW1.line_credit_amt_ent,
                    EDW1.sline_credit_amt_ent,
                    CASE 
                        WHEN EDW1.sline_line_style_id = 9  
                        AND upper(trim(EDW2.object1_code)) = upper(trim('OKX_CUSTPROD'))   THEN SUBSTR( ROUND(INT(EDW2.object1_id1)),1,18)              
                    END AS auto_c035,
                    CASE 
                        WHEN EDW1.sline_line_style_id = 10  
                        AND upper(trim(EDW2.object1_code)) = upper(trim('OKX_PARTYSITE'))   THEN SUBSTR( ROUND(INT(EDW2.object1_id1)),1,18)  
                    END AS auto_c034  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t45    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB + """.contract_line_xref_ld    AS EDW2    
                        ON trim(upper(EDW1.service_subline_id)) = trim(upper(EDW2.contract_line_id))  
                        AND trim(upper(EDW1.instance_id)) = trim(upper(EDW2.instance_id))"""
        print("Job: 'd8_contract_bill_sched_dn_46'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t47"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t46 COMPUTE STATISTICS  FOR COLUMNS item_instance_id"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t46 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t46 COMPUTE STATISTICS  FOR COLUMNS SLINE_LINE_STYLE_ID"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t46 COMPUTE STATISTICS  FOR COLUMNS INSTANCE_ID,ITEM_INSTANCE_ID"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t47           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.service_subline_id,
                    EDW1.sline_line_style_id,
                    EDW1.sline_covered_lvl_name,
                    EDW1.sequence_nbr,
                    EDW1.billing_stream_end_date,
                    EDW1.billing_stream_start_date,
                    EDW1.contr_bill_stream_id,
                    EDW1.interface_offset_day_nbr,
                    EDW1.period_uom_cnt,
                    EDW1.period_uom_code,
                    EDW1.sl_discount_amt_ent,
                    EDW1.sl_surcharge_amt_ent,
                    EDW1.sl_top_lvl_adj_prc_amt_ent,
                    EDW1.sl_top_lvl_oprnd_prc_amt_ent,
                    EDW1.sl_top_lvl_pricing_uom_qty,
                    EDW1.sl_unbilled_amt_ent,
                    EDW1.credit_amt_ent,
                    EDW1.interface_date,
                    EDW1.level_sequence_nbr,
                    EDW1.period_end_date,
                    EDW1.period_start_date,
                    EDW1.bill_on_date,
                    EDW1.invoice_type_code,
                    EDW1.bill_amt_ent,
                    EDW1.completed_date,
                    EDW1.level_element_amt_ent,
                    EDW1.invoice_currency_code,
                    EDW1.invoice_date,
                    EDW1.invoice_nbr,
                    EDW1.line_credit_amt_ent,
                    EDW1.sline_credit_amt_ent,
                    EDW1.item_instance_id,
                    EDW1.install_location_id,
                    COALESCE( EDW2.external_reference_nbr ,
                    ' ' ) AS auto_c036,
                    COALESCE( EDW2.last_valid_invtry_org_id ,
                    0 ) AS auto_c038,
                    COALESCE( EDW2.prev_serial_nbr ,
                    ' ' ) AS auto_c039,
                    COALESCE( EDW2.serial_nbr ,
                    ' ' ) AS auto_c040,
                    COALESCE( EDW2.inventory_item_id ,
                    ' ' ) AS auto_c037  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t46    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB + """.install_base_item_ld    AS EDW2    
                        ON trim(upper(EDW1.item_instance_id)) = trim(upper(EDW2.item_instance_id))  
                        AND  trim(upper(EDW1.instance_id)) =  trim(upper(EDW2.instance_id))    
                WHERE
                    EDW1.sline_line_style_id = 9 """
        print("Running Query: {}".format(query))
        sparkSession.sql(query)                    
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t47           
                SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.service_subline_id,
                    EDW1.sline_line_style_id,
                    EDW1.sline_covered_lvl_name,
                    EDW1.sequence_nbr,
                    EDW1.billing_stream_end_date,
                    EDW1.billing_stream_start_date,
                    EDW1.contr_bill_stream_id,
                    EDW1.interface_offset_day_nbr,
                    EDW1.period_uom_cnt,
                    EDW1.period_uom_code,
                    EDW1.sl_discount_amt_ent,
                    EDW1.sl_surcharge_amt_ent,
                    EDW1.sl_top_lvl_adj_prc_amt_ent,
                    EDW1.sl_top_lvl_oprnd_prc_amt_ent,
                    EDW1.sl_top_lvl_pricing_uom_qty,
                    EDW1.sl_unbilled_amt_ent,
                    EDW1.credit_amt_ent,
                    EDW1.interface_date,
                    EDW1.level_sequence_nbr,
                    EDW1.period_end_date,
                    EDW1.period_start_date,
                    EDW1.bill_on_date,
                    EDW1.invoice_type_code,
                    EDW1.bill_amt_ent,
                    EDW1.completed_date,
                    EDW1.level_element_amt_ent,
                    EDW1.invoice_currency_code,
                    EDW1.invoice_date,
                    EDW1.invoice_nbr,
                    EDW1.line_credit_amt_ent,
                    EDW1.sline_credit_amt_ent,
                    COALESCE( EDW1.item_instance_id ,
                    0 ) AS auto_c035,
                    EDW1.install_location_id,
                    ' ' AS auto_c036,
                    0 AS auto_c038,
                    ' ' AS auto_c039,
                    ' ' AS auto_c040,
                    ' ' AS auto_c037  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t46    AS EDW1   
                WHERE
                    EDW1.sline_line_style_id = 10"""
        print("Running Query: {}".format(query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_48_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t48"""
        print("Job: 'd8_contract_bill_sched_dn_48_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t47 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_48_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t47 COMPUTE STATISTICS  FOR COLUMNS inventory_item_id"""
        print("Job: 'd8_contract_bill_sched_dn_48_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t47 COMPUTE STATISTICS  FOR COLUMNS last_valid_invtry_org_id"""
        print("Job: 'd8_contract_bill_sched_dn_48_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT INTO TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t48 SELECT EDW1.contract_header_id, EDW1.instance_id, EDW1.service_line_id, EDW1.service_subline_id, EDW1.sline_line_style_id, EDW1.sline_covered_lvl_name, EDW1.sequence_nbr, EDW1.billing_stream_end_date, EDW1.billing_stream_start_date, EDW1.contr_bill_stream_id, EDW1.interface_offset_day_nbr, EDW1.period_uom_cnt, EDW1.period_uom_code, EDW1.sl_discount_amt_ent, EDW1.sl_surcharge_amt_ent, EDW1.sl_top_lvl_adj_prc_amt_ent, EDW1.sl_top_lvl_oprnd_prc_amt_ent, EDW1.sl_top_lvl_pricing_uom_qty, EDW1.sl_unbilled_amt_ent, EDW1.credit_amt_ent, EDW1.interface_date, EDW1.level_sequence_nbr, EDW1.period_end_date, EDW1.period_start_date, EDW1.bill_on_date, EDW1.invoice_type_code, EDW1.bill_amt_ent, EDW1.completed_date, EDW1.level_element_amt_ent, EDW1.invoice_currency_code, EDW1.invoice_date, EDW1.invoice_nbr, EDW1.line_credit_amt_ent, EDW1.sline_credit_amt_ent, EDW1.item_instance_id, EDW1.install_location_id, EDW1.external_reference_nbr, EDW1.inventory_item_id, EDW1.last_valid_invtry_org_id, EDW1.prev_serial_nbr, EDW1.serial_nbr, EDW2.offering_id_hyphenated FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t47 AS EDW1 JOIN ( SELECT a.instance_id, a.inventory_item_id, a.inventory_org_id, a.offering_id_hyphenated  FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_cfs_invtry_item_xref    AS a    JOIN ( SELECT inventory_item_id, last_valid_invtry_org_id, instance_id  FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t47     WHERE sline_line_style_id = 9  GROUP BY inventory_item_id , last_valid_invtry_org_id , instance_id      ) AS b    ON a.inventory_item_id = b.inventory_item_id  AND a.inventory_org_id = b.last_valid_invtry_org_id   AND upper(trim(a.instance_id)) = upper(trim(b.instance_id))           ) AS EDW2    ON EDW1.inventory_item_id = EDW2.inventory_item_id  AND EDW1.last_valid_invtry_org_id = EDW2.inventory_org_id   AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))    WHERE sline_line_style_id = 9"""
        print("Job: 'd8_contract_bill_sched_dn_48_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_48_2'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t47 COMPUTE STATISTICS  FOR COLUMNS install_location_id"""
        print("Job: 'd8_contract_bill_sched_dn_48_2'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t48           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.service_subline_id,
                    EDW1.sline_line_style_id,
                    EDW1.sline_covered_lvl_name,
                    EDW1.sequence_nbr,
                    EDW1.billing_stream_end_date,
                    EDW1.billing_stream_start_date,
                    EDW1.contr_bill_stream_id,
                    EDW1.interface_offset_day_nbr,
                    EDW1.period_uom_cnt,
                    EDW1.period_uom_code,
                    EDW1.sl_discount_amt_ent,
                    EDW1.sl_surcharge_amt_ent,
                    EDW1.sl_top_lvl_adj_prc_amt_ent,
                    EDW1.sl_top_lvl_oprnd_prc_amt_ent,
                    EDW1.sl_top_lvl_pricing_uom_qty,
                    EDW1.sl_unbilled_amt_ent,
                    EDW1.credit_amt_ent,
                    EDW1.interface_date,
                    EDW1.level_sequence_nbr,
                    EDW1.period_end_date,
                    EDW1.period_start_date,
                    EDW1.bill_on_date,
                    EDW1.invoice_type_code,
                    EDW1.bill_amt_ent,
                    EDW1.completed_date,
                    EDW1.level_element_amt_ent,
                    EDW1.invoice_currency_code,
                    EDW1.invoice_date,
                    EDW1.invoice_nbr,
                    EDW1.line_credit_amt_ent,
                    EDW1.sline_credit_amt_ent,
                    EDW1.item_instance_id,
                    EDW1.install_location_id,
                    EDW1.external_reference_nbr,
                    EDW1.inventory_item_id,
                    EDW1.last_valid_invtry_org_id,
                    EDW1.prev_serial_nbr,
                    EDW1.serial_nbr,
                    EDW2.customer_site_nbr  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t47    AS EDW1   ,
                    (   SELECT
                        party_site_id,
                        instance_id,
                        customer_site_nbr  
                    FROM
                        """ + db_params.EEDDWWDDBB + """.party_site_ld            )    AS EDW2   
                WHERE
                    EDW1.install_location_id = EDW2.party_site_id
                    AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id ))  
                    AND EDW1.sline_line_style_id = 10"""
        print("Job: 'd8_contract_bill_sched_dn_48_2'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_49'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t48 COMPUTE STATISTICS  FOR COLUMNS CONTRACT_HEADER_ID,INSTANCE_ID,SERVICE_LINE_ID,SERVICE_SUBLINE_ID,SEQUENCE_NBR,INTERFACE_DATE,LEVEL_SEQUENCE_NBR,PERIOD_END_DATE,PERIOD_START_DATE,BILL_ON_DATE,INVOICE_TYPE_CODE,INVOICE_NBR,ITEM_INSTANCE_ID  , SERVICE_SUBLINE_ID,ITEM_INSTANCE_ID  , CONTRACT_HEADER_ID,INSTANCE_ID,SERVICE_LINE_ID,SEQUENCE_NBR,INTERFACE_DATE,LEVEL_SEQUENCE_NBR,PERIOD_END_DATE,PERIOD_START_DATE,BILL_ON_DATE,INVOICE_TYPE_CODE,INVOICE_NBR  , INVOICE_NBR  , SERVICE_LINE_ID  , SEQUENCE_NBR  , PERIOD_START_DATE  , PERIOD_END_DATE  , LEVEL_SEQUENCE_NBR  , INVOICE_TYPE_CODE  , INTERFACE_DATE  , INSTANCE_ID  , CONTRACT_HEADER_ID  , BILL_ON_DATE"""
        print("Job: 'd8_contract_bill_sched_dn_49'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t49"""
        print("Job: 'd8_contract_bill_sched_dn_49'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    with qq1 as (
        SELECT
                     contract_header_id,
                     instance_id,
                     service_line_id,
                     service_subline_id,
                     sline_line_style_id,
                     sline_covered_lvl_name,
                     sequence_nbr,
                     billing_stream_end_date,
                     billing_stream_start_date,
                     contr_bill_stream_id,
                     interface_offset_day_nbr,
                     period_uom_cnt,
                     period_uom_code,
                     sl_discount_amt_ent,
                     sl_surcharge_amt_ent,
                     sl_top_lvl_adj_prc_amt_ent,
                     sl_top_lvl_oprnd_prc_amt_ent,
                     sl_top_lvl_pricing_uom_qty,
                     sl_unbilled_amt_ent,
                     credit_amt_ent,
                     interface_date,
                     level_sequence_nbr,
                     period_end_date,
                     period_start_date,
                     bill_on_date,
                     invoice_type_code,
                     bill_amt_ent,
                     completed_date,
                     level_element_amt_ent,
                     invoice_currency_code,
                     invoice_date,
                     invoice_nbr,
                     line_credit_amt_ent,
                     sline_credit_amt_ent,
                     item_instance_id,
                     install_location_id,
                     external_reference_nbr,
                     last_valid_invtry_org_id,
                     prev_serial_nbr,
                     serial_nbr,
                     sline_product_id_formatted,
                     cast(CONCAT (service_subline_id ,item_instance_id) as varchar(100)) as cct,
                     ROW_NUMBER() OVER (PARTITION BY bill_on_date ,contract_header_id ,instance_id ,interface_date ,invoice_type_code ,level_sequence_nbr ,period_end_date ,period_start_date ,sequence_nbr ,service_line_id ,invoice_nbr ORDER BY CONCAT (service_subline_id ,item_instance_id) DESC) AS RNK
                    FROM """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t48
        ) 
        INSERT INTO """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t49 
        SELECT 
                     EDW2.contract_header_id,
                     EDW2.instance_id,
                     EDW2.service_line_id,
                     EDW2.service_subline_id,
                     EDW2.sline_line_style_id,
                     EDW2.sline_covered_lvl_name,
                     EDW2.sequence_nbr,
                     EDW2.billing_stream_end_date,
                     EDW2.billing_stream_start_date,
                     EDW2.contr_bill_stream_id,
                     EDW2.interface_offset_day_nbr,
                     EDW2.period_uom_cnt,
                     EDW2.period_uom_code,
                     EDW2.sl_discount_amt_ent,
                     EDW2.sl_surcharge_amt_ent,
                     EDW2.sl_top_lvl_adj_prc_amt_ent,
                     EDW2.sl_top_lvl_oprnd_prc_amt_ent,
                     EDW2.sl_top_lvl_pricing_uom_qty,
                     EDW2.sl_unbilled_amt_ent,
                     EDW2.credit_amt_ent,
                     EDW2.interface_date,
                     EDW2.level_sequence_nbr,
                     EDW2.period_end_date,
                     EDW2.period_start_date,
                     EDW2.bill_on_date,
                     EDW2.invoice_type_code,
                     EDW2.bill_amt_ent,
                     EDW2.completed_date,
                     EDW2.level_element_amt_ent,
                     EDW2.invoice_currency_code,
                     EDW2.invoice_date,
                     EDW2.invoice_nbr,
                     EDW2.line_credit_amt_ent,
                     EDW2.sline_credit_amt_ent,
                     EDW2.item_instance_id,
                     EDW2.install_location_id,
                     EDW2.external_reference_nbr,
                     EDW2.last_valid_invtry_org_id,
                     EDW2.prev_serial_nbr,
                     EDW2.serial_nbr,
                     EDW2.sline_product_id_formatted,
        1 AS auto_c041  
        FROM 
          """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t48 EDW2 
          INNER JOIN (
            SELECT 
                    contract_header_id,
                     instance_id,
                     service_line_id,
                     service_subline_id,
                     sline_line_style_id,
                     sline_covered_lvl_name,
                     sequence_nbr,
                     billing_stream_end_date,
                     billing_stream_start_date,
                     contr_bill_stream_id,
                     interface_offset_day_nbr,
                     period_uom_cnt,
                     period_uom_code,
                     sl_discount_amt_ent,
                     sl_surcharge_amt_ent,
                     sl_top_lvl_adj_prc_amt_ent,
                     sl_top_lvl_oprnd_prc_amt_ent,
                     sl_top_lvl_pricing_uom_qty,
                     sl_unbilled_amt_ent,
                     credit_amt_ent,
                     interface_date,
                     level_sequence_nbr,
                     period_end_date,
                     period_start_date,
                     bill_on_date,
                     invoice_type_code,
                     bill_amt_ent,
                     completed_date,
                     level_element_amt_ent,
                     invoice_currency_code,
                     invoice_date,
                     invoice_nbr,
                     line_credit_amt_ent,
                     sline_credit_amt_ent,
                     item_instance_id,
                     install_location_id,
                     external_reference_nbr,
                     last_valid_invtry_org_id,
                     prev_serial_nbr,
                     serial_nbr,
                     sline_product_id_formatted, 
                    RNK,
                    cct
            FROM 
              qq1 
            where 
              RNK = 1
          ) IBVL 
          on upper(trim(EDW2.instance_id)) =upper(trim(IBVL.instance_id)) 
           AND EDW2.bill_on_date=IBVL.bill_on_date
           AND upper(trim(EDW2.contract_header_id)) =upper(trim(IBVL.contract_header_id)) 
           AND EDW2.interface_date=IBVL.interface_date
           AND upper(trim(EDW2.invoice_type_code)) = upper(trim(IBVL.invoice_type_code)) 
           AND upper(trim(EDW2.level_sequence_nbr)) =upper(trim(IBVL.level_sequence_nbr))  
           AND EDW2.period_end_date =IBVL.period_end_date
           AND EDW2.period_start_date=IBVL.period_start_date  
           AND EDW2.sequence_nbr = IBVL.sequence_nbr
           AND upper(trim(EDW2.service_line_id )) =upper(trim(IBVL.service_line_id )) 
           AND upper(trim(EDW2.invoice_nbr)) = upper(trim(IBVL.invoice_nbr))
           AND cct = cast(CONCAT (EDW2.service_subline_id ,EDW2.item_instance_id) as varchar(100))"""
        print("Job: 'd8_contract_bill_sched_dn_49'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB1 + """ """
        print("Job: 'd8_contract_bill_sched_dn_50'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t50"""
        print("Job: 'd8_contract_bill_sched_dn_50'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t50           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.service_subline_id,
                    EDW1.sline_line_style_id,
                    EDW1.sline_covered_lvl_name,
                    EDW1.sequence_nbr,
                    EDW1.billing_stream_end_date,
                    EDW1.billing_stream_start_date,
                    EDW1.contr_bill_stream_id,
                    EDW1.interface_offset_day_nbr,
                    EDW1.period_uom_cnt,
                    EDW1.period_uom_code,
                    EDW1.sl_discount_amt_ent,
                    EDW1.sl_surcharge_amt_ent,
                    EDW1.sl_top_lvl_adj_prc_amt_ent,
                    EDW1.sl_top_lvl_oprnd_prc_amt_ent,
                    EDW1.sl_top_lvl_pricing_uom_qty,
                    EDW1.sl_unbilled_amt_ent,
                    EDW1.credit_amt_ent,
                    EDW1.interface_date,
                    EDW1.level_sequence_nbr,
                    EDW1.period_end_date,
                    EDW1.period_start_date,
                    EDW1.bill_on_date,
                    EDW1.invoice_type_code,
                    EDW1.bill_amt_ent,
                    EDW1.completed_date,
                    EDW1.level_element_amt_ent,
                    EDW1.invoice_currency_code,
                    EDW1.invoice_date,
                    EDW1.invoice_nbr,
                    EDW1.line_credit_amt_ent,
                    EDW1.sline_credit_amt_ent,
                    EDW1.item_instance_id,
                    EDW1.install_location_id,
                    EDW1.external_reference_nbr,
                    EDW1.last_valid_invtry_org_id,
                    EDW1.prev_serial_nbr,
                    EDW1.serial_nbr,
                    EDW1.sline_product_id_formatted,
                    EDW2.subline_ind  
                FROM
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t48    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t49    AS EDW2    
                        ON EDW1.bill_on_date = EDW2.bill_on_date  
                        AND upper(trim(EDW1.contract_header_id)) = upper(trim(EDW2.contract_header_id))   
                        AND upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))   
                        AND EDW1.interface_date = EDW2.interface_date   
                        AND upper(trim(EDW1.invoice_nbr)) = upper(trim(EDW2.invoice_nbr))   
                        AND upper(trim(EDW1.invoice_type_code)) = upper(trim(EDW2.invoice_type_code))
                        AND upper(trim(EDW1.level_sequence_nbr)) = upper(trim(EDW2.level_sequence_nbr))
                        AND EDW1.period_end_date = EDW2.period_end_date   
                        AND EDW1.period_start_date = EDW2.period_start_date   
                        AND EDW1.sequence_nbr = EDW2.sequence_nbr
                        AND upper(trim(EDW1.service_line_id)) = upper(trim(EDW2.service_line_id))   
                        AND upper(trim(EDW1.service_subline_id)) = upper(trim(EDW2.service_subline_id))"""
        print("Job: 'd8_contract_bill_sched_dn_50'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.TTMMPPDDBB + """ """
        print("Job: 'd8_contract_bill_sched_dn_51'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t51"""
        print("Job: 'd8_contract_bill_sched_dn_51'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t50 COMPUTE STATISTICS  FOR COLUMNS contract_header_id"""
        print("Job: 'd8_contract_bill_sched_dn_51'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t50 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_51'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t50 COMPUTE STATISTICS  FOR COLUMNS service_line_id"""
        print("Job: 'd8_contract_bill_sched_dn_51'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t50 COMPUTE STATISTICS  FOR COLUMNS service_subline_id"""
        print("Job: 'd8_contract_bill_sched_dn_51'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t51           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.service_subline_id,
                    EDW1.sline_line_style_id,
                    EDW1.sline_covered_lvl_name,
                    EDW1.sequence_nbr,
                    EDW1.billing_stream_end_date,
                    EDW1.billing_stream_start_date,
                    EDW1.contr_bill_stream_id,
                    EDW1.interface_offset_day_nbr,
                    EDW1.period_uom_cnt,
                    EDW1.period_uom_code,
                    EDW1.sl_discount_amt_ent,
                    EDW1.sl_surcharge_amt_ent,
                    EDW1.sl_top_lvl_adj_prc_amt_ent,
                    EDW1.sl_top_lvl_oprnd_prc_amt_ent,
                    EDW1.sl_top_lvl_pricing_uom_qty,
                    EDW1.sl_unbilled_amt_ent,
                    EDW1.credit_amt_ent,
                    EDW1.interface_date,
                    EDW1.level_sequence_nbr,
                    EDW1.period_end_date,
                    EDW1.period_start_date,
                    EDW1.bill_on_date,
                    EDW1.invoice_type_code,
                    EDW1.bill_amt_ent,
                    EDW1.completed_date,
                    EDW1.level_element_amt_ent,
                    EDW1.invoice_currency_code,
                    EDW1.invoice_date,
                    EDW1.invoice_nbr,
                    EDW1.line_credit_amt_ent,
                    EDW1.sline_credit_amt_ent,
                    EDW1.item_instance_id,
                    EDW1.install_location_id,
                    EDW1.external_reference_nbr,
                    EDW1.last_valid_invtry_org_id,
                    EDW1.prev_serial_nbr,
                    EDW1.serial_nbr,
                    EDW1.sline_product_id_formatted,
                    EDW1.subline_ind,
                    EDW2.service_subline_end_date,
                    EDW2.service_subline_nbr,
                    EDW2.service_subline_start_date,
                    EDW2.service_subline_term_date,
                    EDW2.service_subline_status_code,
                    EDW2.serviced_qty  
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t50    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t21    AS EDW2    
                        ON upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))  
                        AND upper(trim(EDW1.contract_header_id)) = upper(trim(EDW2.contract_header_id))   
                        AND upper(trim(EDW1.service_line_id)) = upper(trim(EDW2.service_line_id))   
                        AND upper(trim(EDW1.service_subline_id)) = upper(trim(EDW2.service_subline_id))"""
        print("Job: 'd8_contract_bill_sched_dn_51'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t51 COMPUTE STATISTICS  FOR COLUMNS contract_header_id  , instance_id  , service_line_id"""
        print("Job: 'd8_contract_bill_sched_dn_51'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t52"""
        print("Job: 'd8_contract_bill_sched_dn_52'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t52           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.bill_to_site_use_id,
                    EDW1.bill_to_cust_indstry_code,
                    EDW2.cust_sales_channel_code,
                    EDW1.customer_nbr,
                    EDW1.country_code  
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t25    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.ttmp_cust_acct_site_use_dn    AS EDW2    
                        ON upper(trim(EDW1.instance_id)) = upper(trim(EDW2.instance_id))  
                        AND EDW1.bill_to_site_use_id = EDW2.cust_acct_site_use_id"""
        print("Job: 'd8_contract_bill_sched_dn_52'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t53"""
        print("Job: 'd8_contract_bill_sched_dn_53'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t53           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.bill_to_site_use_id,
                    EDW1.bill_to_cust_indstry_code,
                    EDW1.bill_to_cust_sales_chnl_code,
                    EDW2.parent_cust_ind_code,
                    EDW1.customer_nbr,
                    EDW1.country_code  
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t52    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB1 + """.cust_industry_supp    AS EDW2    
                        ON upper(trim( EDW1.bill_to_cust_indstry_code)) = upper(trim( EDW2.cust_industry_code))"""
        print("Job: 'd8_contract_bill_sched_dn_53'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS COUNTRY_CODE,CUSTOMER_NBR"""
        print("Job: 'd8_contract_bill_sched_dn_54'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t54"""
        print("Job: 'd8_contract_bill_sched_dn_54'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t54           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.bill_to_cust_indstry_code,
                    EDW1.bill_to_cust_sales_chnl_code,
                    EDW1.bill_to_site_use_id,
                    EDW1.bill_to_pnt_cust_ind_code,
                    EDW2.parent_cust_ind_name,
                    EDW1.country_code,
                    EDW1.customer_nbr  
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t53    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB1 + """.parent_cust_industry    AS EDW2    
                        ON upper(trim( EDW1.bill_to_pnt_cust_ind_code)) = upper(trim( EDW2.parent_cust_ind_code))"""
        print("Job: 'd8_contract_bill_sched_dn_54'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t55"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_customer_dn_tmp"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_customer_dn_tmp           SELECT
                    customer_nbr,
                    data_source_code,
                    oper_unit_country_code,
                    eff_start_date_time,
                    eff_end_date_time,
                    naics_code,
                    naics_desc  
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.customer_dn     
                WHERE
                    upper(trim(data_source_code)) = upper(trim('ERP'))  
                    AND  (
                        CURRENT_TIMESTAMP() BETWEEN eff_start_date_time AND eff_end_date_time 
                    )"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS contract_header_id"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS service_line_id"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS country_code"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t54 COMPUTE STATISTICS  FOR COLUMNS customer_nbr"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_customer_dn_tmp COMPUTE STATISTICS  FOR COLUMNS customer_nbr"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_customer_dn_tmp COMPUTE STATISTICS  FOR COLUMNS data_source_code"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_customer_dn_tmp COMPUTE STATISTICS  FOR COLUMNS oper_unit_country_code"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_customer_dn_tmp COMPUTE STATISTICS  FOR COLUMNS eff_start_date_time"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_customer_dn_tmp COMPUTE STATISTICS  FOR COLUMNS eff_end_date_time"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t55           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.bill_to_cust_indstry_code,
                    EDW1.bill_to_cust_sales_chnl_code,
                    EDW1.bill_to_pnt_cust_ind_code,
                    EDW1.bill_to_pnt_cust_ind_name,
                    EDW1.bill_to_site_use_id,
                    EDW1.country_code,
                    EDW1.customer_nbr,
                    EDW2.naics_code,
                    EDW2.naics_desc  
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t54    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_customer_dn_tmp    AS EDW2    
                        ON upper(trim( EDW1.customer_nbr)) = upper(trim( EDW2.customer_nbr))  
                        AND upper(trim( EDW1.country_code)) = upper(trim( EDW2.oper_unit_country_code))"""
        print("Job: 'd8_contract_bill_sched_dn_55'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    TRUNCATE TABLE   """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t56"""
        print("Job: 'd8_contract_bill_sched_dn_56'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t56           SELECT
                    EDW1.contract_header_id,
                    EDW1.instance_id,
                    EDW1.service_line_id,
                    EDW1.bill_to_cust_indstry_code,
                    EDW1.bill_to_cust_sales_chnl_code,
                    EDW1.bill_to_pnt_cust_ind_code,
                    EDW1.bill_to_pnt_cust_ind_name,
                    EDW1.bill_to_site_use_id,
                    EDW1.country_code,
                    EDW1.customer_nbr,
                    EDW1.naics_code,
                    EDW1.naics_desc,
                    EDW2.sub_region_code,
                    EDW2.sub_region_name  
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t55    AS EDW1   
                LEFT OUTER  JOIN
                    """ + db_params.EEDDWWDDBB1 + """.geo_rollup_xref    AS EDW2    
                        ON upper(trim(EDW1.country_code)) = upper(trim(EDW2.country_code )) 
                        AND upper(trim(EDW2.business_unit_code)) = upper(trim('B4'))"""
        print("Job: 'd8_contract_bill_sched_dn_56'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t25 COMPUTE STATISTICS  FOR COLUMNS contract_header_id"""
        print("Job: 'd8_contract_bill_sched_dn_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t25 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t25 COMPUTE STATISTICS  FOR COLUMNS service_line_id"""
        print("Job: 'd8_contract_bill_sched_dn_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t56 COMPUTE STATISTICS  FOR COLUMNS contract_header_id"""
        print("Job: 'd8_contract_bill_sched_dn_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t56 COMPUTE STATISTICS  FOR COLUMNS instance_id"""
        print("Job: 'd8_contract_bill_sched_dn_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    ANALYZE TABLE """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t56 COMPUTE STATISTICS  FOR COLUMNS service_line_id"""
        print("Job: 'd8_contract_bill_sched_dn_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    TRUNCATE TABLE   """ + db_params.EEDDWWDDBB + """.contract_bill_sched_dn_ld"""
        print("Job: 'd8_contract_bill_sched_dn_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT 
            INTO
                TABLE
                """ + db_params.EEDDWWDDBB + """.contract_bill_sched_dn_ld           SELECT
                    CASE 
                        WHEN EDW2.invoice_type_code = 'NOT BILLED'  THEN NULL  
                        ELSE EDW2.bill_on_date  
                    END AS bill_on_date,
                    EDW2.contract_header_id,
                    EDW2.instance_id,
                    EDW2.interface_date,
                    EDW2.invoice_type_code,
                    EDW2.level_sequence_nbr,
                    EDW2.period_end_date,
                    EDW2.period_start_date,
                    EDW2.sequence_nbr,
                    EDW2.service_line_id,
                    EDW2.service_subline_id,
                    EDW1.area_code,
                    EDW1.area_desc,
                    COALESCE( EDW1.author_oper_unit_country_code ,
                    ' ' ) AS auto_c02,
                    COALESCE( EDW2.bill_amt_ent ,
                    0 ) AS auto_c0101,
                    EDW1.bill_to_addr_line_1,
                    EDW1.bill_to_addr_line_2,
                    EDW1.bill_to_addr_line_3,
                    EDW1.bill_to_addr_line_4,
                    EDW1.bill_to_city_name,
                    EDW1.bill_to_collctn_org_code,
                    EDW1.bill_to_country_code,
                    EDW1.bill_to_county_name,
                    EDW1.bill_to_cs_fml_org_code,
                    EDW1.bill_to_cs_fml_org_name,
                    EDW1.bill_to_cust_indstry_code,
                    EDW1.bill_to_cust_indstry_name,
                    EDW1.bill_to_cust_name,
                    EDW1.bill_to_cust_nbr,
                    EDW1.bill_to_cust_site_name,
                    EDW1.bill_to_cust_site_nbr,
                    EDW1.bill_to_cust_type_code,
                    EDW1.bill_to_fml_org_code,
                    EDW1.bill_to_fml_org_name,
                    EDW1.bill_to_oper_unit_country_code,
                    EDW1.bill_to_party_id,
                    EDW1.bill_to_party_site_addr_id,
                    EDW1.bill_to_postal_code,
                    EDW1.bill_to_province_name,
                    COALESCE( EDW1.bill_to_site_use_id ,
                    0 ) AS auto_c03,
                    EDW1.bill_to_state_name,
                    EDW1.bill_to_store_nbr,
                    EDW1.bill_to_svc_loc_code,
                    EDW1.bill_to_svc_territory_id,
                    EDW2.billing_stream_end_date,
                    EDW2.billing_stream_start_date,
                    EDW2.completed_date,
                    EDW1.contract_end_date,
                    COALESCE( EDW1.contract_est_amt_ent ,
                    0 ) AS auto_c05,
                    EDW1.contract_group_name,
                    COALESCE( EDW1.contract_nbr ,
                    ' ' ) AS auto_c06,
                    COALESCE( EDW1.contract_nbr_modifier ,
                    ' ' ) AS auto_c07,
                    EDW1.contract_start_date,
                    EDW1.country_code,
                    EDW1.country_desc,
                    COALESCE( EDW1.curr_code ,
                    ' ' ) AS auto_c09,
                    EDW1.customer_exemption_nbr,
                    EDW1.customer_name,
                    EDW1.customer_nbr,
                    EDW1.exemption_reason_code,
                    EDW1.exemption_tax_code,
                    EDW2.external_reference_nbr,
                    EDW1.hold_billing_flag,
                    COALESCE( EDW2.install_location_id ,
                    0 ) AS auto_c0109,
                    COALESCE( EDW2.interface_offset_day_nbr ,
                    ' ' ) AS auto_c086,
                    EDW1.inventory_item_id,
                    EDW1.inventory_org_id,
                    COALESCE( EDW2.invoice_currency_code ,
                    ' ' ) AS auto_c0104,
                    EDW2.invoice_date,
                    COALESCE( EDW2.invoice_nbr ,
                    ' ' ) AS auto_c0106,
                    EDW1.invoice_rule_desc,
                    COALESCE( EDW1.invoice_rule_id ,
                    0 ) AS auto_c011,
                    EDW1.invoice_rule_name,
                    COALESCE( EDW2.item_instance_id ,
                    0 ) AS auto_c0110,
                    EDW2.last_valid_invtry_org_id,
                    COALESCE( EDW2.level_element_amt_ent ,
                    0 ) AS auto_c0103,
                    EDW1.line_product_id,
                    EDW1.line_product_id_formatted,
                    COALESCE( EDW1.migration_contract_nbr ,
                    ' ' ) AS auto_c012,
                    EDW1.party_id,
                    COALESCE( EDW2.period_uom_cnt ,
                    ' ' ) AS auto_c087,
                    COALESCE( EDW2.period_uom_code ,
                    ' ' ) AS auto_c088,
                    EDW2.prev_serial_nbr,
                    EDW1.region_code,
                    EDW1.region_desc,
                    EDW1.renew_date,
                    COALESCE( EDW1.renewal_type_code ,
                    ' ' ) AS auto_c014,
                    EDW1.renewal_type_name,
                    EDW2.serial_nbr,
                    EDW1.service_line_nbr,
                    EDW1.service_line_style_id,
                    COALESCE( EDW1.service_line_style_name ,
                    ' ' ) AS auto_c068,
                    EDW2.service_subline_end_date,
                    EDW2.service_subline_nbr,
                    EDW2.service_subline_start_date,
                    EDW2.service_subline_status_code,
                    EDW2.service_subline_term_date,
                    EDW2.sline_covered_lvl_name,
                    EDW2.sline_line_style_id,
                    EDW2.sline_product_id_formatted,
                    COALESCE( EDW1.solution_portfolio_id ,
                    ' ' ) AS auto_c015,
                    COALESCE( EDW1.status_code ,
                    ' ' ) AS auto_c016,
                    COALESCE( EDW1.tax_exempt_status_code ,
                    ' ' ) AS auto_c069,
                    COALESCE( EDW1.tax_exemption_id ,
                    0 ) AS auto_c070,
                    CURRENT_TIMESTAMP() AS auto_c0123,
                    EDW1.vat_tax_code,
                    COALESCE( EDW1.vat_tax_id ,
                    0 ) AS auto_c071,
                    COALESCE( EDW2.line_credit_amt_ent ,
                    0 ) AS auto_c0107,
                    COALESCE( EDW2.sline_credit_amt_ent ,
                    0 ) AS auto_c0108,
                    COALESCE( EDW2.sl_discount_amt_ent ,
                    0 ) AS auto_c089,
                    COALESCE( EDW2.sl_surcharge_amt_ent ,
                    0 ) AS auto_c090,
                    COALESCE( EDW2.sl_top_lvl_adj_prc_amt_ent ,
                    0 ) AS auto_c091,
                    COALESCE( EDW2.sl_top_lvl_oprnd_prc_amt_ent ,
                    0 ) AS auto_c092,
                    COALESCE( EDW2.sl_top_lvl_pricing_uom_qty ,
                    0 ) AS auto_c093,
                    COALESCE( EDW2.sl_unbilled_amt_ent ,
                    0 ) AS auto_c094,
                    EDW2.subline_ind,
                    COALESCE( EDW1.cust_po_nbr ,
                    ' ' ) AS auto_c010,
                    EDW1.cust_trx_type_id,
                    EDW1.hdr_cust_exempt_nbr,
                    EDW1.hdr_exempt_reason_code,
                    EDW1.hdr_tax_exempt_sts_code,
                    EDW1.hdr_tax_exemption_id,
                    COALESCE( EDW1.hdr_trx_type_name ,
                    ' ' ) AS auto_c023,
                    EDW2.serviced_qty,
                    COALESCE( EDW3.bill_to_cust_sales_chnl_code ,
                    ' ' ) AS auto_c0124,
                    COALESCE( EDW3.bill_to_pnt_cust_ind_code ,
                    ' ' ) AS auto_c0125,
                    COALESCE( EDW3.bill_to_pnt_cust_ind_name ,
                    ' ' ) AS auto_c0126,
                    COALESCE( EDW3.naics_code ,
                    ' ' ) AS auto_c0127,
                    COALESCE( EDW3.naics_desc ,
                    ' ' ) AS auto_c0128,
                    COALESCE( EDW3.sub_region_code ,
                    ' ' ) AS auto_c0130,
                    COALESCE( EDW3.sub_region_name ,
                    ' ' ) AS auto_c0129  
                FROM
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t51    AS EDW2   
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t25    AS EDW1    
                        ON upper(trim( EDW2.instance_id ))= upper(trim( EDW1.instance_id )) 
                        AND upper(trim( EDW2.contract_header_id)) = upper(trim( EDW1.contract_header_id ))  
                        AND upper(trim( EDW2.service_line_id)) = upper(trim( EDW1.service_line_id))    
                LEFT OUTER  JOIN
                    """ + db_params.TTMMPPDDBB + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contr_bill_sched_dn_t56    AS EDW3    
                        ON upper(trim( EDW1.service_line_id)) = upper(trim( EDW3.service_line_id )) 
                        AND upper(trim( EDW1.contract_header_id)) = upper(trim( EDW3.contract_header_id))   
                        AND upper(trim( EDW1.instance_id)) = upper(trim( EDW3.instance_id))"""
        print("Job: 'd8_contract_bill_sched_dn_ld_1'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)


def main():
    m = d8contractbillscheddn01inssql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractbillscheddn01inssql").enableHiveSupport().getOrCreate()
    main()
