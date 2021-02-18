/*

Skew by worker

*/

create or replace materialized view diag_mz_worker_skew as 
select * from (
select
A.worker_thread_id ,
cpu_time_ms_per_worker,
total_records_per_worker,
total_batches_per_worker,
total_records_per_worker*100/(case when total_records = 0 then 1 else total_records end) as pct_records_skew,
total_batches_per_worker*100/(case when total_batches = 0 then 1 else total_batches end) as pct_batches_skew,
cpu_time_ms_per_worker*100/total_cpu_time_ms as pct_cpu_skew,
(case
        when A.worker_thread_id = 0 then 1
        else 1
end)*100/total_worker_threads as expected_pct
from
(
select
mse_worker as worker_thread_id, 
(sum(mse_elapsed_ns))/1000000 as cpu_time_ms_per_worker
from
diag_mz_dataflow_stats 
where mse_id = mdo_dataflow_id  
group by mse_worker
    ) A,
(select
count(distinct mse_worker) as total_worker_threads, 
(sum(mse_elapsed_ns))/1000000 as total_cpu_time_ms
from
diag_mz_dataflow_stats 
where mse_id = mdo_dataflow_id  -- for only dataflows
     ) B,
(
select
mse_worker as worker_thread_id, 
sum(mas_records) as total_records_per_worker, 
sum(mas_batches) total_batches_per_worker
from
diag_mz_dataflow_stats 
group by mse_worker
    ) C,
(select
sum(mas_records) as total_records, 
sum(mas_batches) total_batches
from
diag_mz_dataflow_stats 
     ) D
where 
A.worker_thread_id = C.worker_thread_id 
);



