/*

Dataflow rows/batches by Operators 

*/

create or replace materialized view diag_mv_operator_records_batches as 
select mdo_dataflow_id, 
mdo_dataflow_name as dataflow_name,
mdo_name as operator,
sum(mas_records) as total_records_per_operator, 
sum(mas_batches) total_batches_per_operator 
from 
diag_mz_dataflow_stats
    group by mdo_name, mdo_dataflow_id, mdo_dataflow_name
;

