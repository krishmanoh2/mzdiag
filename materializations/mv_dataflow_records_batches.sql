/*
Which Dataflow has most records and corresponding batches
*/
create or replace materialized view diag_mv_dataflow_records_batches as 
select 
mdr_dataflow_id, 
mdi_name as dataflow_name, 
sum(mas_records) as total_records_dataflow, 
sum(mas_batches) total_batches_dataflow 
from
diag_mz_dataflow_stats 
--	 mz_arrangement_sizes as mas,
--     mz_dataflow_operators as mdo,
--     mz_records_per_dataflow_operator mdr,
--     mz_records_per_dataflow mdi
-- where 
	-- mas.records > 0 and
--    mas.operator = mdo.id and
--    mas.worker = mdo.worker
--    and mdr.id = mdo.id
--    and mdr.worker = mdo.worker 
--    and mdr.dataflow_id = mdi.id
--    and mdi.name not like '%mz_catalog%' 
--    and mdi.worker = mdr.worker
    group by mdr_dataflow_id, mdi_name;

