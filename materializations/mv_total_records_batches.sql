/*
Total records and batches  maintained in mz
*/
create or replace materialized view diag_mv_total_records_batches as 
select
-- now() as current_date, 
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
--     mas.operator = mdo.id and
--    mas.worker = mdo.worker
--    and mdr.id = mdo.id
--    and mdr.worker = mdo.worker 
--    and mdr.dataflow_id = mdi.id
--    and mdi.name not like '%mz_catalog%' 
--    and mdi.worker = mdr.worker
;

