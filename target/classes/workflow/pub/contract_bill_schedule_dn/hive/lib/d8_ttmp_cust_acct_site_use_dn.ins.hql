----------------------------------------------------------------------------------------
--THIS FILE IS CREATED AUTOMATICALLY USING Workload Transformation @ 2019/12/18 11:14:42 
--Script Name: d8_ttmp_cust_acct_site_use_dn.ins.sql
--Inputs:  
--Outputs:

--Modification History:
--Date:     Current Date     Who:    
--Control:              
--Description:
----------------------------------------------------------------------------------------


--Original Query:
  --DELETE FROM ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn ALL  

--Translated Query: STATUS SUCCESS

    TRUNCATE TABLE   ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn;

--Original Query:
  --LOCKING TABLE ${EEDDWWDDBB}.cust_acct_site_use_dn FOR ACCESS  INSERT INTO ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn    (        cust_acct_site_use_id       ,instance_id       ,cust_sales_channel_code       ,naics_code       ,naics_desc       ,cust_acct_site_use_code   ) SELECT        cust_acct_site_use_id       ,instance_id       ,cust_sales_channel_code       ,naics_code       ,naics_desc       ,cust_acct_site_use_code  FROM ${EEDDWWDDBB}.cust_acct_site_use_dn  

--Translated Query: STATUS SUCCESS

    INSERT 
    INTO
        TABLE
        ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn           SELECT
            cust_acct_site_use_id,
            instance_id,
            cust_sales_channel_code,
            naics_code,
            naics_desc,
            cust_acct_site_use_code  
        FROM
            ${EEDDWWDDBB}.cust_acct_site_use_dn;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn COLUMN (instance_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn COMPUTE STATISTICS  FOR COLUMNS instance_id;

--Original Query:
  --COLLECT STATS ON ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn COLUMN (cust_acct_site_use_id) 

--Translated Query: STATUS SUCCESS

    ANALYZE TABLE ${TTMMPPDDBB}.ttmp_cust_acct_site_use_dn COMPUTE STATISTICS  FOR COLUMNS cust_acct_site_use_id;
