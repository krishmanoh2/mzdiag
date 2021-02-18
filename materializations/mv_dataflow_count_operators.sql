/*
 
Dataflows - count(distinct operators)

*/
create or replace materialized view diag_mv_dataflow_count_operators as 
select
mdo_dataflow_id, 
mdo_dataflow_name as dataflow_name,
mdo_name as operator_name, 
count(mdo_name)/count(distinct mdo_worker) as operator_count_per_worker
-- * 
from
diag_mz_dataflow_stats 
--     mz_scheduling_elapsed mse,
--     mz_dataflow_operators as mdo,  
--     mz_arrangement_sizes as mas, 
--     mz_records_per_dataflow_operator mdr,
--     mz_records_per_dataflow mdi 
where
mse_id != mdo_dataflow_id 
group by mdo_dataflow_id, mdo_dataflow_name, mdo_name
;
