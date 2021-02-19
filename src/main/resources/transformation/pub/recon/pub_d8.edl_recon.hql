Set hive.exec.compress.output='false';
Set hive.execution.engine=tez;
Set hive.tez.container.size=16384;
Set hive.tez.java.opts=-Xmx13106m;
Set tez.grouping.max-size = 268435456;

use ${hivevar:targetSchema};

insert overwrite table EDL_RECON
select cast('Count Validation'as varchar(30)) as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType,  cast('contract_bill_invc_line' as varchar(60)) as TableName, count(1) as TotalCount from pub_d8.contract_bill_invc_line union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'uom_time_code_xref' as TableName, count(1) as TotalCount from pub_d8.uom_time_code_xref union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'contract_party_role' as TableName, count(1) as TotalCount from pub_d8.contract_party_role union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'contract_line_bill' as TableName, count(1) as TotalCount from pub_d8.contract_line_bill union all 

select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'contract_line_svc_supp' as TableName, count(1) as TotalCount from pub_d8.contract_line_svc_supp union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'contract_header' as TableName, count(1) as TotalCount from pub_d8.contract_header union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'contract_line_xref' as TableName, count(1) as TotalCount from pub_d8.contract_line_xref union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'install_base_item' as TableName, count(1) as TotalCount from pub_d8.install_base_item union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'contract_header_svc_supp' as TableName, count(1) as TotalCount from pub_d8.contract_header_svc_supp union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'contract_line_lk' as TableName, count(1) as TotalCount from pub_d8.contract_line_lk union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'contr_bill_stream' as TableName, count(1) as TotalCount from pub_d8.contr_bill_stream union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'contract_subline_bill' as TableName, count(1) as TotalCount from pub_d8.contract_subline_bill union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'contract_line' as TableName, count(1) as TotalCount from pub_d8.contract_line union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'Base Table' as TableType, 'business_process' as TableName, count(1) as TotalCount from pub_d8.business_process union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_ADDR_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_ADDR_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_BILL_SCHED_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_BILL_SCHED_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_BILL_SCHED_SUM_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_BILL_SCHED_SUM_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_DN_ARCS' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_DN_ARCS union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_DNC' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_DNC union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_ENTRPRS_CUST_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_ENTRPRS_CUST_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_HEADER_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_HEADER_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_INSTALL_BASE_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_INSTALL_BASE_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_LINE_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_LINE_DN union all 

select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_SUBLINE_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_SUBLINE_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_SUPT_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_SUPT_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_SUPT_SUM_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_SUPT_SUM_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_UNBILLED_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_UNBILLED_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'IB_ENTRPRS_CUST_DN' as TableName, count(1) as TotalCount from pub_d8.IB_ENTRPRS_CUST_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'INSTALL_BASE_ADDR_DN' as TableName, count(1) as TotalCount from pub_d8.INSTALL_BASE_ADDR_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'INSTALL_BASE_CONTRACT_DN' as TableName, count(1) as TotalCount from pub_d8.INSTALL_BASE_CONTRACT_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'INSTALL_BASE_DN' as TableName, count(1) as TotalCount from pub_d8.INSTALL_BASE_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType, 'CONTRACT_COVERAGE_DN' as TableName, count(1) as TotalCount from pub_d8.CONTRACT_COVERAGE_DN union all 
select 'Count Validation' as OperationType, current_timestamp() as as_of_date_time, 'DN Table' as TableType,  'contr_inst_base_adv_dn' as TableName, count(1) as TotalCount from pub_d8.contr_inst_base_adv_dn;
