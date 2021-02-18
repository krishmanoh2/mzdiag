/*

Skew by rows, batches and cpu_time by dataflow operator

*/


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
mse.worker as worker_thread_id, 
(sum(elapsed_ns))/1000000 as cpu_time_operator_ms_per_worker,
sum(mas.records) as total_records_operator_per_worker, 
sum(mas.batches) total_batches_operator_per_worker, 
mdo.dataflow_id, 
mdo.dataflow_name,
mdo.name as operator
-- * 
from 
  mz_scheduling_elapsed mse
  join mz_dataflow_operator_dataflows as mdo on mse.id = mdo.id and mse.worker = mdo.worker
  left join mz_arrangement_sizes as mas on  mas.operator = mdo.id and mas.worker = mdo.worker
  left join  mz_records_per_dataflow_operator mdr on mdr.id = mdo.id and mdr.worker = mdo.worker 
  left join  mz_records_per_dataflow mdi on mdr.dataflow_id = mdi.id and mdi.worker = mdr.worker 
where 
mse.id != mdo.dataflow_id    
group by mse.worker, mdo.dataflow_id, mdo.dataflow_name,  mdo.name
    ) A,
(select
count(distinct mse.worker) as total_worker_threads, 
(sum(elapsed_ns))/1000000 as total_cpu_time_operator_ms,
sum(mas.records) as total_records_per_operator, 
sum(mas.batches) total_batches_per_operator, 
mdo.dataflow_id, 
mdo.dataflow_name,
mdo.name as operator
from 
mz_scheduling_elapsed mse
  join mz_dataflow_operator_dataflows as mdo on mse.id = mdo.id and mse.worker = mdo.worker
  left join mz_arrangement_sizes as mas on  mas.operator = mdo.id and mas.worker = mdo.worker
  left join  mz_records_per_dataflow_operator mdr on mdr.id = mdo.id and mdr.worker = mdo.worker
  left join  mz_records_per_dataflow mdi on mdr.dataflow_id = mdi.id and mdi.worker = mdr.worker 
where
mse.id != mdo.dataflow_id
group by mdo.dataflow_name, mdo.dataflow_id, mdo.name
     ) B
WHERE A.dataflow_id = B.dataflow_id 
and A.dataflow_name = B.dataflow_name 
and A.operator = B.operator
)
-- where pct_cpu_skew > 55 or pct_cpu_skew < 45
order by  dataflow_name, worker_thread_id, operator;


