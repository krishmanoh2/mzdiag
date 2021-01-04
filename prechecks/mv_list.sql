/*
List of indexes, mv and tables in mz
*/

select 
extract(epoch from now()) - (time/1000) as time_behind_seconds, 
-- now(), 
-- to_timestamp(time/1000), 
b.name as index_name,
c.name as mv_name, 
d.name as tab_name  from 
mz_materialization_frontiers a 
full outer join mz_indexes b on a.global_id = b.id 
full outer join mz_views c on c.id = b.on_id 
full outer join mz_tables d on d.id = b.on_id  
where b.name not like 'mz%';
