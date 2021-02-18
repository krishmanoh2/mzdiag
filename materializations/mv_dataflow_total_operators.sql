/*

Dataflows - count of operators 

*/

create or replace materialized view diag_mv_dataflow_total_operators as 
select
mdo_dataflow_id, 
mdo_dataflow_name as dataflow_name, 
count(1)/count(distinct mdo_worker) as total_operators_per_worker
-- * 
from 
diag_mz_dataflow_stats
 where 
mse_id != mdo_dataflow_id
group by mdo_dataflow_id, mdo_dataflow_name
;
