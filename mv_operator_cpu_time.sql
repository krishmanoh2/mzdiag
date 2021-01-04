/*

Operators in materializations by cpu_time

*/


select
dataflow_id, 
mdi.name as dataflow_name, 
mdo.name as operator, 
operator as operator_id, 
sum(mas.records) as total_records_per_operator, 
sum(mas.batches) as total_batches_per_operator, 
(sum(elapsed_ns))/1000000 as cpu_time_milliseconds
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
    group by dataflow_id, mdi.name, mdo.name, operator
 order by cpu_time_milliseconds desc;




