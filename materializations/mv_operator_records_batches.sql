/*

Dataflow rows/batches by Operators 

*/

create materialized view diag_mv_operator_records_batches as 
select dataflow_id, 
mdi.name as dataflow_name,
mdo.name as operator,
sum(mas.records) as total_records_per_operator, 
sum(mas.batches) total_batches_per_operator 
from 
	 mz_arrangement_sizes as mas, -- for batches 
     mz_dataflow_operators as mdo, -- for operators
     mz_records_per_dataflow_operator mdr, -- Glue table
     mz_records_per_dataflow mdi -- for dataflow names
where 
	mas.records > 0 and
    mas.operator = mdo.id and
    mas.worker = mdo.worker
    and mdr.id = mdo.id
    and mdr.worker = mdo.worker 
    and mdr.dataflow_id = mdi.id
    and mdi.name not like '%mz_catalog%' 
    and mdi.worker = mdr.worker
    group by mdo.name, dataflow_id, mdi.name
 order by  total_records_per_operator desc, dataflow_id;

