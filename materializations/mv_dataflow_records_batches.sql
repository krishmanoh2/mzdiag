/*
Which Dataflow has most records and corresponding batches
*/
create or replace materialized view diag_mv_dataflow_records_batches as 
select 
mdo_dataflow_id, 
mdo_dataflow_name as dataflow_name, 
sum(mas_records) as total_records_dataflow, 
sum(mas_batches) total_batches_dataflow 
from
diag_mz_dataflow_stats 
group by mdo_dataflow_id, mdo_dataflow_name;

