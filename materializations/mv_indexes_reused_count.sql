/*

Looks like this is more accurate representation of shared arrangements

*/


create materialized view diag_mv_indexes_reused_count as 
select source_name, count(1) as reuse_count from (SELECT DISTINCT 
source_info.source_info as external_source, 
mcn_source.name AS source_name,
mcn.name as target_dataflow, 
to_timestamp(source_info.time/1000) as last_updated_source_timestamp,
to_timestamp(frontier_df.time/1000) as last_updated_dataflow_timestamp,
CASE 
WHEN source_info.time < frontier_df.time THEN 0 
ELSE source_info.time - frontier_df.time 
END AS lag_source_dataflow_ms 
FROM 
mz_materialization_dependencies AS index_deps 
JOIN 
(select case when source_id is null then id else source_id end as source_id,  time, case when source_info is null then B.name else source_info end as source_info   from 
(SELECT source_id, max(timestamp) AS time, source_name as source_info FROM mz_source_info GROUP BY source_id, source_name) A
RIGHT join
(select id, name from mz_sources ) B
on A.source_id = B.id
where B.name not like 'mz%'
)AS source_info ON index_deps.source = source_info.source_id 
LEFT JOIN mz_materialization_frontiers AS frontier_df ON index_deps.dataflow = frontier_df.global_id
JOIN mz_catalog_names AS mcn ON mcn.global_id = index_deps.dataflow 
JOIN mz_catalog_names AS mcn_source ON mcn_source.global_id = source_info.source_id
UNION
SELECT DISTINCT 
null as external_source,
mcn_source.name AS source_name, 
mcn.name AS target_dataflow, 
to_timestamp(frontier_source.time/1000) as last_updated_source_timestamp,
to_timestamp(frontier_df.time/1000) as last_updated_target_dataflow_timestamp,
frontier_source.time - frontier_df.time AS lag_source_dataflow_ms 
FROM 
mz_materialization_dependencies AS index_deps 
JOIN mz_materialization_frontiers AS frontier_source ON index_deps.source = frontier_source.global_id 
JOIN mz_materialization_frontiers AS frontier_df ON index_deps.dataflow = frontier_df.global_id 
JOIN mz_catalog_names AS mcn ON mcn.global_id = index_deps.dataflow 
JOIN mz_catalog_names AS mcn_source ON mcn_source.global_id = frontier_source.global_id order by lag_source_dataflow_ms desc) group by 1;
