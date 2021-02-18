/*
Total records and batches  maintained in mz
*/
create or replace materialized view diag_mv_total_records_batches as 
select
sum(mas_records) as total_records_dataflow, 
sum(mas_batches) total_batches_dataflow 
from 
diag_mz_dataflow_stats
;

