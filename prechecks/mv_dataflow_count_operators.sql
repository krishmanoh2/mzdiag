/*

Dataflows - count of operators 

*/


select
dataflow_id, 
mdo.dataflow_name as dataflow_name,
mdo.name as operator, 
count(1)/count(distinct mdo.worker) as total_operators_per_worker
-- * 
from 
     mz_dataflow_operator_dataflows as mdo  
where 
mdo.name != mdo.dataflow_name
    group by dataflow_id, mdo.dataflow_name, mdo.name
 order by total_operators_per_worker desc;
