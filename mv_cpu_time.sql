/*

Materializations by cpu_time

*/

select
mdo.dataflow_id as dataflow_id,
mdo.dataflow_name as dataflow_name,
(sum(mse.elapsed_ns))/1000000 as cpu_time_milliseconds
from 
  mz_scheduling_elapsed mse
  join mz_dataflow_operator_dataflows as mdo on mse.id = mdo.id and mse.worker = mdo.worker
  left join mz_arrangement_sizes as mas on  mas.operator = mdo.id and mas.worker = mdo.worker
  left join  mz_records_per_dataflow_operator mdr on mdr.id = mdo.id and mdr.worker = mdo.worker 
  left join  mz_records_per_dataflow mdi on mdr.dataflow_id = mdi.id and mdi.worker = mdr.worker 
where mse.id = mdo.dataflow_id
group by mdo.dataflow_id, mdo.dataflow_name
order by cpu_time_milliseconds desc;
