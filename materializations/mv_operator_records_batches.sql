/*

Dataflow rows/batches by Operators 

*/

create or replace materialized view diag_mv_operator_records_batches as 
select mdr_dataflow_id, 
mdi_name as dataflow_name,
mdo_name as operator,
sum(mas_records) as total_records_per_operator, 
sum(mas_batches) total_batches_per_operator 
from 
diag_mz_dataflow_stats
--	 mz_arrangement_sizes as mas, -- for batches 
--     mz_dataflow_operators as mdo, -- for operators
--     mz_records_per_dataflow_operator mdr, -- Glue table
--     mz_records_per_dataflow mdi -- for dataflow names
-- where 
--	mas.records > 0 and
--    mas.operator = mdo.id and
--    mas.worker = mdo.worker
--    and mdr.id = mdo.id
--    and mdr.worker = mdo.worker 
--    and mdr.dataflow_id = mdi.id
--    and mdi.name not like '%mz_catalog%' 
--    and mdi.worker = mdr.worker
    group by mdo_name, mdr_dataflow_id, mdi_name
;

