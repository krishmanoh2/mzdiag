/*

Operators running for higher scheduled intervals (expensive operators)

*/
create or replace materialized view diag_mv_operator_cpu_hog as 
select 
mdr_dataflow_id, 
mdi_name as dataflow_name,
mdo_name as operator, 
mdo_id as operator_id, 
duration_ns as scheduled_duration_ns, 
 sum(msh.count) as scheduled_times_count
-- *                                                   
-- mdo.id, mdo.name, mdo.worker, msh.duration_ns, count
from mz_scheduling_histogram as msh,
diag_mz_dataflow_stats mdo
--  mz_scheduling_elapsed mse,
--     mz_dataflow_operators as mdo,  
--     mz_arrangement_sizes as mas, 
--     mz_records_per_dataflow_operator mdr,
--     mz_records_per_dataflow mdi 
where                   
     msh.id = mdo.mdo_id and
    msh.worker = mdo.mdo_worker -- and
    -- mas.records > 0 and
--     mse.id = mdo.id and
 --    mse.worker = mdo.worker and
 --    mas.operator = mdo.id and
 --    mas.worker = mdo.worker
 --    and mdr.id = mdo.id
--     and mdr.worker = mdo.worker 
--     and mdr.dataflow_id = mdi.id
--     and mdi.name not like '%mz_catalog%' 
--     and mdi.worker = mdr.worker
    group by mdr_dataflow_id, mdi_name, mdo_name, mdo_id, scheduled_duration_ns
;
