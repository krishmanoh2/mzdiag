
/*

Skew by rows, batches and cpu_time by dataflow

*/

create or replace materialized view diag_mv_dataflow_skew as 
select * from (
select 
A.worker_thread_id,  
A.dataflow_id,
A.dataflow_name,
cpu_time_dataflow_ms_per_worker,
total_records_dataflow_per_worker,
total_batches_dataflow_per_worker,
total_records_per_dataflow,
total_batches_per_dataflow,
total_records_dataflow_per_worker*100/(case when total_records_per_dataflow = 0 then 1 when total_records_per_dataflow is null then 100 else total_records_per_dataflow end) as pct_records_skew,
total_batches_dataflow_per_worker*100/(case when total_batches_per_dataflow = 0 then 1 when total_batches_per_dataflow is null then 100 else total_batches_per_dataflow end) as pct_batches_skew,
cpu_time_dataflow_ms_per_worker*100/total_cpu_time_dataflow_ms as pct_cpu_skew,
(case
    when A.worker_thread_id = 0 then 1
    else 1
end)*100/total_worker_threads as expected_pct 
from (
select
mse_worker as worker_thread_id, 
(sum(mse_elapsed_ns))/1000000 as cpu_time_dataflow_ms_per_worker,
mdo_dataflow_id as dataflow_id, 
mdo_dataflow_name as dataflow_name
from
diag_mz_dataflow_stats 
where mse_id = mdo_dataflow_id  -- for only dataflows
group by mse_worker, mdo_dataflow_id, mdo_dataflow_name --,  mdo.name
    ) A,
(select
count(distinct mse_worker) as total_worker_threads, 
(sum(mse_elapsed_ns))/1000000 as total_cpu_time_dataflow_ms,
mdo_dataflow_id as dataflow_id, 
mdo_dataflow_name as dataflow_name 
from
diag_mz_dataflow_stats 
where mse_id = mdo_dataflow_id  -- for only dataflows
group by mdo_dataflow_name, mdo_dataflow_id -- , mdo.name
     ) B,
(
select
mse_worker as worker_thread_id, 
sum(mas_records) as total_records_dataflow_per_worker, 
sum(mas_batches) total_batches_dataflow_per_worker, 
mdo_dataflow_id as dataflow_id, 
mdo_dataflow_name as dataflow_name -- ,
from
diag_mz_dataflow_stats 
group by mse_worker, mdo_dataflow_id, mdo_dataflow_name --,  mdo.name
    ) C,
(select
sum(mas_records) as total_records_per_dataflow, 
sum(mas_batches) total_batches_per_dataflow, 
mdo_dataflow_id as dataflow_id, 
mdo_dataflow_name as dataflow_name 
from
diag_mz_dataflow_stats 
group by mdo_dataflow_name, mdo_dataflow_id 
     ) D
where 
A.dataflow_id = B.dataflow_id and 
A.dataflow_name = B.dataflow_name and
A.dataflow_id = C.dataflow_id and 
A.dataflow_name = C.dataflow_name and
A.worker_thread_id = C.worker_thread_id and
A.dataflow_name = D.dataflow_name and
A.dataflow_id = D.dataflow_id
)

