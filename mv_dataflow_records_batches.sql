/*
Which Dataflow has most records and corresponding batches
*/

select 
mdo.dataflow_id, 
mdo.dataflow_name , 
sum(mas.records) as total_records_dataflow, 
sum(mas.batches) total_batches_dataflow 
from 
    mz_arrangement_sizes as mas,
    mz_dataflow_operator_dataflows as mdo
where 
    mas.operator = mdo.id and
    mas.worker = mdo.worker
    group by mdo.dataflow_id, mdo.dataflow_name
 order by total_records_dataflow desc;

