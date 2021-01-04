/*

Shared arrangments - reuse count - may not be indicative of delta query joins

*/


select
dataflow_id,
mdi.name as dataflow_name,
mdo.name as operator,
msa.operator as operator_id,
sum(msa.count)/count(distinct mdo.worker) as count_reused_worker
from 
     mz_arrangement_sharing as msa,
     mz_arrangement_sizes as mas,
     mz_dataflow_operators as mdo,
     mz_records_per_dataflow_operator mdr,
     mz_records_per_dataflow mdi
where
    mas.records > 0 and 
    msa.operator = mdo.id and
    msa.worker = mdo.worker and
    mas.operator = mdo.id and
    mas.worker = mdo.worker
    and mdr.id = mdo.id
    and mdr.worker = mdo.worker 
    and mdr.dataflow_id = mdi.id
    and mdi.name not like '%mz_catalog%' 
    and mdi.worker = mdr.worker
    group by dataflow_id, dataflow_name, mdo.name, msa.operator;
