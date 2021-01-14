/* 
parent view for most queries
*/

create materialized view diag_mz_dataflow_stats as 
select 
mse.id as mse_id,
mse.worker as mse_worker,
mse.elapsed_ns as mse_elapsed_ns,
mdo.id as mdo_id,
mdo.worker as mdo_worker,
mdo.name as mdo_name,
mas.operator as mas_operator,
mas.worker as mas_worker,
mas.records as mas_records,
mas.batches as mas_batches,
mdr.id as mdr_id,
mdr.name as mdr_name,
mdr.worker as mdr_worker,
mdr.dataflow_id as mdr_dataflow_id,
mdr.records as mdr_records,
mdi.id as mdi_id,
mdi.name as mdi_name,
mdi.worker as mdi_worker,
mdi.records as mdi_records
from 
	 mz_scheduling_elapsed mse,
     mz_dataflow_operators as mdo,  
     mz_arrangement_sizes as mas, 
     mz_records_per_dataflow_operator mdr,
     mz_records_per_dataflow mdi 
where 
    -- mas.records > 0 and
    mse.id = mdo.id and
    mse.worker = mdo.worker and
    mas.operator = mdo.id and
    mas.worker = mdo.worker
    and mdr.id = mdo.id
    and mdr.worker = mdo.worker 
    and mdr.dataflow_id = mdi.id
    -- and mdi.name not like '%mz_catalog%' 
    and mdi.worker = mdr.worker
;
