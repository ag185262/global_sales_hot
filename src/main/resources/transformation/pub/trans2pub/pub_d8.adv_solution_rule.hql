use ${hivevar:targetSchema};
DROP TABLE IF EXISTS adv_solution_rule;
CREATE TABLE IF NOT EXISTS adv_solution_rule stored  as parquet tblproperties ("parquet.compression"="snappy") AS SELECT  *  FROM trans_foundational_edw_e3_tedw.adv_solution_rule
;
