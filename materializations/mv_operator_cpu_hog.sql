/*

Operators running for higher scheduled intervals (expensive operators)

*/
create or replace materialized view diag_mv_operator_cpu_hog as 
select 
mdo_dataflow_id, 
mdo_dataflow_name as dataflow_name,
mdo_name as operator, 
mdo_id as operator_id, 
duration_ns as scheduled_duration_ns, 
 sum(msh.count) as scheduled_times_count
from mz_scheduling_histogram as msh,
diag_mz_dataflow_stats mdo
where                   
     msh.id = mdo.mdo_id and
    msh.worker = mdo.mdo_worker 
and mse_id != mdo_dataflow_id
    group by mdo_dataflow_id, mdo_dataflow_name, mdo_name, mdo_id, scheduled_duration_ns
;
