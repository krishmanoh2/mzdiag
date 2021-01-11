/*
 
Dataflows - count(distinct operators)

*/
create materialized view diag_mv_dataflow_count_operators as 
select
dataflow_id, 
mdi.name as dataflow_name,
mdo.name as operator_name, 
count(mdo.name)/count(distinct mdo.worker) as operator_count_per_worker
-- * 
from 
     mz_scheduling_elapsed mse,
     mz_dataflow_operators as mdo,  
     mz_arrangement_sizes as mas, 
     mz_records_per_dataflow_operator mdr,
     mz_records_per_dataflow mdi 
where 
--  mas.records > 0 and
    mse.id = mdo.id and
    mse.worker = mdo.worker and
    mas.operator = mdo.id and
    mas.worker = mdo.worker
    and mdr.id = mdo.id
    and mdr.worker = mdo.worker 
    and mdr.dataflow_id = mdi.id
    and mdi.name not like '%mz_catalog%' 
    and mdi.worker = mdr.worker
    group by dataflow_id, mdi.name, mdo.name
 order by dataflow_name, operator_count_per_worker desc;

