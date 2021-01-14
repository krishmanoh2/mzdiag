/*

Materializations by cpu_time

*/
create or replace materialized view diag_mv_cpu_time as
select
mdr_dataflow_id, 
mdi_name as dataflow_name, 
(sum(mse_elapsed_ns))/1000000 as cpu_time_milliseconds
-- * 
from 
diag_mz_dataflow_stats
--	 mz_scheduling_elapsed mse,
--     mz_dataflow_operators as mdo,  
--     mz_arrangement_sizes as mas, 
--     mz_records_per_dataflow_operator mdr,
--     mz_records_per_dataflow mdi 
-- where 
    -- mas.records > 0 and
 --   mse.id = mdo.id and
  --  mse.worker = mdo.worker and
--    mas.operator = mdo.id and
--    mas.worker = mdo.worker
--    and mdr.id = mdo.id
--    and mdr.worker = mdo.worker 
--    and mdr.dataflow_id = mdi.id
-- mdi.name not like '%mz_catalog%' 
--    and mdi.worker = mdr.worker
    group by mdr_dataflow_id, mdi_name
-- order by cpu_time_milliseconds desc;
