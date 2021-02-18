/*

Operators in materializations by cpu_time

*/

create or replace materialized view diag_mv_operator_cpu_time as 
select
mdo_dataflow_id, 
mdo_dataflow_name as dataflow_name, 
mdo_name as operator, 
mdo_id as operator_id, 
sum(mas_records) as total_records_per_operator, 
sum(mas_batches) as total_batches_per_operator, 
(sum(mse_elapsed_ns))/1000000 as cpu_time_milliseconds
-- * 
from 
diag_mz_dataflow_stats
where mse_id != mdo_dataflow_id
group by mdo_dataflow_id, mdo_dataflow_name, mdo_name, mdo_id
;




