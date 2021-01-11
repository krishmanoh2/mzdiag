/*

Skew by rows, batches and cpu_time by dataflow operator

*/

create materialized view diag_mv_operator_skew as 
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
dataflow_id, 
mdi.name as dataflow_name,
mdo.name as operator
-- * 
from 
	 mz_scheduling_elapsed mse,
     mz_dataflow_operators as mdo,  
     mz_arrangement_sizes as mas, 
     mz_records_per_dataflow_operator mdr,
     mz_records_per_dataflow mdi 
where 
    -- mas.records > 0 and
    mse.id = mdo.id and
    mse.worker = mdo.worker and
    mas.operator = mdo.id and
    mas.worker = mdo.worker
    and mdr.id = mdo.id
    and mdr.worker = mdo.worker 
    and mdr.dataflow_id = mdi.id
    and mdi.name not like '%mz_catalog%' 
    and mdi.worker = mdr.worker
    group by mse.worker, dataflow_id, mdi.name,  mdo.name
    ) A,
(select
count(distinct mse.worker) as total_worker_threads, 
(sum(elapsed_ns))/1000000 as total_cpu_time_operator_ms,
sum(mas.records) as total_records_per_operator, 
sum(mas.batches) total_batches_per_operator, 
dataflow_id, 
mdi.name as dataflow_name,
mdo.name as operator
from 
	 mz_scheduling_elapsed mse,
     mz_dataflow_operators as mdo,  
     mz_arrangement_sizes as mas, 
     mz_records_per_dataflow_operator mdr,
     mz_records_per_dataflow mdi 
where 
    -- mas.records > 0 and
    mse.id = mdo.id and
    mse.worker = mdo.worker and
    mas.operator = mdo.id and
    mas.worker = mdo.worker
    and mdr.id = mdo.id
    and mdr.worker = mdo.worker 
    and mdr.dataflow_id = mdi.id
    and mdi.name not like '%mz_catalog%' 
    and mdi.worker = mdr.worker
    group by mdi.name, dataflow_id, mdo.name
     ) B
where A.dataflow_id = B.dataflow_id and A.dataflow_name = B.dataflow_name and A.operator = B.operator
)
where pct_cpu_skew > 55 or pct_cpu_skew < 45
order by  dataflow_name, worker_thread_id, operator;


