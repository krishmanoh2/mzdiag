/* 
parent view for most queries
*/

create or replace materialized view diag_mz_dataflow_stats as 
select 
mse.id as mse_id,
mse.worker as mse_worker,
mse.elapsed_ns as mse_elapsed_ns,
mdo.id as mdo_id,
mdo.worker as mdo_worker,
mdo.name as mdo_name,
mdo.dataflow_id as mdo_dataflow_id,
mdo.dataflow_name as mdo_dataflow_name,
mas.operator as mas_operator,
mas.worker as mas_worker,
mas.records as mas_records,
mas.batches as mas_batches,
mdr.id as mdr_id,
mdr.name as mdr_name,
mdr.worker as mdr_worker,
mdr.records as mdr_records,
mdi.id as mdi_id,
mdi.worker as mdi_worker,
mdi.records as mdi_records
from 
  mz_scheduling_elapsed mse
  join mz_dataflow_operator_dataflows as mdo on mse.id = mdo.id and mse.worker = mdo.worker
  left join mz_arrangement_sizes as mas on  mas.operator = mdo.id and mas.worker = mdo.worker
  left join  mz_records_per_dataflow_operator mdr on mdr.id = mdo.id and mdr.worker = mdo.worker 
  left join  mz_records_per_dataflow mdi on mdr.dataflow_id = mdi.id and mdi.worker = mdr.worker 
-- where mse.id != mdo.dataflow_id
;
