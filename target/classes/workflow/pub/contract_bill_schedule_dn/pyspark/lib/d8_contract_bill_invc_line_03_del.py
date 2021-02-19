from pyspark.sql import SparkSession
from datetime import datetime
import db_params

class d8contractbillinvcline03delsql:

    def __init__(self, sparkSession1):
        global sparkSession
        sparkSession = sparkSession1

    def execute(self):
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_bill_invc_line_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld
        SELECT ld.* FROM """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld ld
        LEFT OUTER JOIN
        (SELECT 
        DISTINCT B.contract_line_bill_id,B.instance_id   
        FROM """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld B 
        INNER JOIN
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_purge_t2 C  
        ON upper(trim(B.contract_line_bill_id)) = upper(trim(C.contract_line_id))  
        AND upper(trim(B.instance_id)) = upper(trim(C.instance_id)) 
        )Q2
        ON upper(trim(ld.contract_line_bill_id)) = upper(trim(Q2.contract_line_bill_id))  
        AND upper(trim(ld.instance_id)) = upper(trim(Q2.instance_id)) 
        WHERE
        Q2.contract_line_bill_id IS NULL AND Q2.instance_id IS NULL"""
        print("Job: 'd8_contract_bill_invc_line_03'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_bill_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
        TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld 
        SELECT A.* FROM
        """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld A   
        LEFT OUTER JOIN
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_purge_t2    AS B   
        ON
        UPPER(TRIM(A.instance_id)) = UPPER(TRIM(B.instance_id))  
        AND UPPER(TRIM(A.contract_line_id)) = UPPER(TRIM(B.contract_line_id))  
        WHERE B.instance_id IS NULL  
        AND B.contract_line_id IS NULL"""
        print("Job: 'd8_contract_line_bill_04'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_subline_bill_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld SELECT
                    A.* 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld    AS A   
            LEFT OUTER JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_purge_t2    AS B   
                ON
                upper(trim(A.instance_id)) = upper(trim(B.instance_id))  
                    AND upper(trim(A.contract_line_id)) = upper(trim(B.contract_line_id  ))  
                    WHERE B.instance_id IS NULL  
                    AND B.contract_line_id IS NULL"""
        print("Job: 'd8_contract_subline_bill_05'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contr_bill_stream_19'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld SELECT
                    A.* 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld    AS A
        LEFT OUTER JOIN
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_purge_t1    AS B  
        ON (upper(trim(A.instance_id)) = upper(trim(B.instance_id)) 
                    AND upper(trim(A.alt_contract_header_id)) = upper(trim(B.contract_header_id)))
        where B.instance_id is null and B.contract_header_id is null"""
        print("Job: 'd8_contr_bill_stream_19'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld
        SELECT A.* from  """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld A
        INNER JOIN """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld qq2
        on upper(trim(qq2.contr_bill_stream_id))=upper(trim(A.rule_id))
        and upper(trim(qq2.instance_id))=upper(trim(A.instance_id))
        LEFT OUTER JOIN 
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_purge_t1 B
        ON upper(trim(B.instance_id))=upper(trim(A.instance_id)) 
        and upper(trim(B.contract_header_id))=upper(trim(A.alt_contract_header_id))
        where B.contract_header_id is null and B.instance_id is NULL"""
        print("Job: 'd8_contract_lvl_element_07'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_lvl_element_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """
        INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contract_lvl_element_ld"""
        print("Job: 'd8_contract_lvl_element_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_party_role_13'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """ INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld SELECT
                    A.* 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld    AS A   
        LEFT OUTER JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_purge_t1    AS B   
        ON (upper(trim(A.instance_id ))= upper(trim(B.instance_id ))
        AND upper(trim(A.alt_contract_header_id)) = upper(trim(B.contract_header_id)) )
                WHERE	B.instance_id IS NULL AND B.contract_header_id IS NULL"""
        print("Job: 'd8_contract_party_role_13'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_xref_22'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld SELECT
                    A.* 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld    AS A   
        LEFT OUTER JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_purge_t1    AS B  
         ON (trim(upper(A.instance_id)) = trim(upper(B.instance_id))  
                    AND trim(upper(A.alt_contract_header_id)) = trim(upper(B.contract_header_id  )))  
        WHERE B.instance_id  is null and B.contract_header_id is null"""
        print("Job: 'd8_contract_line_xref_22'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_svc_supp_24'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
                TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld SELECT
                    A.* 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld    AS A   
        LEFT OUTER JOIN
                    """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_purge_t1    AS B   
        ON (trim(upper(A.instance_id)) = trim(upper(B.instance_id))  
                    AND trim(upper(A.alt_contract_header_id)) = trim(upper(B.contract_header_id  )) )
        WHERE B.instance_id IS NULL and B.contract_header_id  IS NULL"""
        print("Job: 'd8_contract_line_svc_supp_24'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_group_xref_27'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE
               TABLE """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld SELECT
                     A.* 
                 FROM
                     """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld    AS A   
        LEFT OUTER JOIN 
                     """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_purge_t1    AS B
        ON (upper(trim( A.instance_id)) = upper(trim( B.instance_id  ))
         AND upper(trim( A.included_contract_header_id)) = upper(trim( B.contract_header_id))  ) 
        WHERE B.instance_id   is null and B.contract_header_id is null"""
        print("Job: 'd8_contract_group_xref_27'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_header_svc_supp_30'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE 
        """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld SELECT
                    A.* 
                FROM
                    """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld    AS A
        LEFT OUTER JOIN
        """ + db_params.TTMMPPDDBB1 + """.""" + db_params.AAPPLLIIDD1EENNVV + """_contract_header_purge_t1    AS B  
        ON (upper(trim( A.instance_id)) = upper(trim( B.instance_id))  
          AND upper(trim( A.contract_header_id)) = upper(trim( B.contract_header_id)) )
        WHERE B.instance_id IS NULL  AND B.contract_header_id is null"""
        print("Job: 'd8_contract_header_svc_supp_30'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_group_lk_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_group_lk SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contract_group_lk_ld"""
        print("Job: 'd8_contract_group_lk_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_group_xref_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_group_xref  SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contract_group_xref_ld """
        print("Job: 'd8_contract_group_xref_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_style_lk_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk  SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contract_line_style_lk_ld"""
        print("Job: 'd8_contract_line_style_lk_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_xref_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_xref SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contract_line_xref_ld"""
        print("Job: 'd8_contract_line_xref_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_party_role_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_party_role SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contract_party_role_ld"""
        print("Job: 'd8_contract_party_role_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_bill_invc_line_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE  """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line SELECT * FROM  """ + db_params.EEDDWWDDBB1 + """.contract_bill_invc_line_ld"""
        print("Job: 'd8_contract_bill_invc_line_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contr_bill_stream_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contr_bill_stream_ld"""
        print("Job: 'd8_contr_bill_stream_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_header_svc_supp_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contract_header_svc_supp_ld"""
        print("Job: 'd8_contract_header_svc_supp_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_bill_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_line_bill SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contract_line_bill_ld"""
        print("Job: 'd8_contract_line_bill_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_line_svc_supp_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE  """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp SELECT * FROM  """ + db_params.EEDDWWDDBB1 + """.contract_line_svc_supp_ld"""
        print("Job: 'd8_contract_line_svc_supp_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)

        query = ""
        query = """    use """ + db_params.EEDDWWDDBB1 + """ """
        print("Job: 'd8_contract_subline_bill_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """INSERT OVERWRITE TABLE """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill  SELECT * FROM """ + db_params.EEDDWWDDBB1 + """.contract_subline_bill_ld"""
        print("Job: 'd8_contract_subline_bill_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = ""
        query = """    use """ + db_params.EEDDWWDDBB + """ """
        print("Job: 'd8_contract_bill_sched_dn_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        query = """    INSERT OVERWRITE TABLE   """ + db_params.EEDDWWDDBB + """.contract_bill_sched_dn SELECT * FROM """ + db_params.EEDDWWDDBB + """.contract_bill_sched_dn_ld"""
        print("Job: 'd8_contract_bill_sched_dn_ld'. Running Query at {}: {}".format(datetime.now(), query))
        sparkSession.sql(query)
        


def main():
    m = d8contractbillinvcline03delsql(sparkSession)
    m.execute()

if __name__ == '__main__':
    global sparkSession
    sparkSession = SparkSession.builder.appName("d8contractbillinvcline03delsql").enableHiveSupport().getOrCreate()
    main()
