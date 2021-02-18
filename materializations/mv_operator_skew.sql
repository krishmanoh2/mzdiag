/*

Skew by rows, batches and cpu_time by dataflow operator

*/

create or replace materialized view diag_mv_operator_skew as 
select * from (
select 
worker_thread_id,  
A.dataflow_id,
A.dataflow_name,
A.operator,
cpu_time_operator_ms_per_worker,
total_records_operator_per_worker,
total_batches_operator_per_worker,
total_records_operator_per_worker*100/(case when total_records_per_operator = 0 then 1 else total_records_per_operator end) as pct_records_skew,
total_batches_operator_per_worker*100/(case when total_batches_per_operator = 0 then 1 else total_batches_per_operator end) as pct_batches_skew,
cpu_time_operator_ms_per_worker*100/total_cpu_time_operator_ms as pct_cpu_skew,
(case
	when worker_thread_id = 0 then 1
	else 1
end)*100/total_worker_threads as expected_pct 
from (
select
mse_worker as worker_thread_id, 
(sum(mse_elapsed_ns))/1000000 as cpu_time_operator_ms_per_worker,
sum(mas_records) as total_records_operator_per_worker, 
sum(mas_batches) total_batches_operator_per_worker, 
mdo_dataflow_id as dataflow_id, 
mdo_dataflow_name as dataflow_name,
mdo_name as operator
from
diag_mz_dataflow_stats
where mse_id != mdo_dataflow_id
group by mse_worker, mdo_dataflow_id, mdo_dataflow_name,  mdo_name
    ) A,
(select
count(distinct mse_worker) as total_worker_threads, 
(sum(mse_elapsed_ns))/1000000 as total_cpu_time_operator_ms,
sum(mas_records) as total_records_per_operator, 
sum(mas_batches) total_batches_per_operator, 
mdo_dataflow_id as dataflow_id, 
mdo_dataflow_name as dataflow_name,
mdo_name as operator
from
diag_mz_dataflow_stats 
where mse_id != mdo_dataflow_id
group by mdo_dataflow_name, mdo_dataflow_id, mdo_name
     ) B
where A.dataflow_id = B.dataflow_id and A.dataflow_name = B.dataflow_name and A.operator = B.operator
);

