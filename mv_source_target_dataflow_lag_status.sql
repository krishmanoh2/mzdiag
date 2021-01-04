/*

A more complete picture of source to dataflow - lag

*/


SELECT DISTINCT 
source_info.source_info as external_source, 
mcn_source.name AS source_name,
to_timestamp(source_info.time/1000) as source_last_updated_timestamp,
extract(epoch from now()) - source_info.time/1000 as current_source_lag_ms,
mcn.name as target_dataflow, 
to_timestamp(frontier_df.time/1000) as target_dataflow_last_updated_timestamp,
extract(epoch from now()) - frontier_df.time/1000 as current_dataflow_lag_ms,
CASE 
WHEN source_info.time < frontier_df.time THEN 0 
ELSE source_info.time - frontier_df.time 
END AS source_dataflow_lag_ms 
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
to_timestamp(frontier_source.time/1000) as source_last_updated_timestamp,
extract(epoch from now()) - frontier_source.time/1000 as current_source_lag_ms,
mcn.name AS target_dataflow, 
to_timestamp(frontier_df.time/1000) as target_dataflow_last_updated_timestamp,
extract(epoch from now()) - frontier_df.time/1000 as current_dataflow_lag_ms,
frontier_source.time - frontier_df.time AS source_dataflow_lag_ms 
FROM 
mz_materialization_dependencies AS index_deps 
JOIN mz_materialization_frontiers AS frontier_source ON index_deps.source = frontier_source.global_id 
JOIN mz_materialization_frontiers AS frontier_df ON index_deps.dataflow = frontier_df.global_id 
JOIN mz_catalog_names AS mcn ON mcn.global_id = index_deps.dataflow 
JOIN mz_catalog_names AS mcn_source ON mcn_source.global_id = frontier_source.global_id order by current_source_lag_ms desc;
