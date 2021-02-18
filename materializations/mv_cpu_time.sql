/*

Materializations by cpu_time

*/
create or replace materialized view diag_mv_cpu_time as
select
mdo_dataflow_id, 
mdo_dataflow_name as dataflow_name, 
(sum(mse_elapsed_ns))/1000000 as cpu_time_milliseconds
from 
diag_mz_dataflow_stats
where mse_id = mdo_dataflow_id
group by mdo_dataflow_id, mdo_dataflow_name
