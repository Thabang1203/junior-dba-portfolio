USE master;
GO

-- 1. Top Missing Indexes by Potential Improvement Impact
SELECT TOP 20
    DB_NAME(mid.database_id) as DatabaseName,
    OBJECT_NAME(mid.object_id, mid.database_id) as TableName,
    migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) as ImprovementMeasure,
    migs.user_seeks as UserSeeks,
    migs.user_scans as UserScans,
    migs.avg_total_user_cost as AvgQueryCostReduction,
    migs.avg_user_impact as AvgImpactPct,
    mid.equality_columns as EqualityColumns,
    mid.inequality_columns as InequalityColumns,
    mid.included_columns as IncludedColumns,
    'CREATE INDEX IX_' + OBJECT_NAME(mid.object_id, mid.database_id) + '_' 
    + REPLACE(REPLACE(ISNULL(mid.equality_columns, '') + ISNULL('_' + mid.inequality_columns, ''), '[', ''), ']', '')
    + ' ON ' + mid.statement + ' (' + ISNULL(mid.equality_columns, '')
    + CASE WHEN mid.inequality_columns IS NOT NULL 
           THEN (CASE WHEN mid.equality_columns IS NOT NULL THEN ',' ELSE '' END) + mid.inequality_columns 
           ELSE '' END + ')' + ISNULL(' INCLUDE (' + mid.included_columns + ')', '') + ';' as CreateIndexStatement
FROM sys.dm_db_missing_index_groups mig
INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
ORDER BY ImprovementMeasure DESC;
GO

-- 2. Unused Indexes (candidates for removal)
SELECT
    DB_NAME() as DatabaseName,
    OBJECT_NAME(i.object_id) as TableName,
    i.name as IndexName,
    i.type_desc as IndexType,
    ius.user_seeks as UserSeeks,
    ius.user_scans as UserScans,
    ius.user_lookups as UserLookups,
    ius.user_updates as UserUpdates,
    ius.last_user_seek as LastUserSeek,
    ius.last_user_scan as LastUserScan
FROM sys.indexes i
LEFT JOIN sys.dm_db_index_usage_stats ius
    ON i.object_id = ius.object_id
    AND i.index_id = ius.index_id
    AND ius.database_id = DB_ID()
WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
  AND i.index_id > 1
  AND (ius.user_seeks = 0 OR ius.user_seeks IS NULL)
  AND (ius.user_scans = 0 OR ius.user_scans IS NULL)
  AND (ius.user_lookups = 0 OR ius.user_lookups IS NULL)
ORDER BY ius.user_updates DESC;
GO

-- 3. Index Fragmentation Analysis
SELECT
    DB_NAME() as DatabaseName,
    OBJECT_NAME(ips.object_id) as TableName,
    i.name as IndexName,
    ips.index_type_desc as IndexType,
    ROUND(ips.avg_fragmentation_in_percent, 2) as FragmentationPct,
    ips.page_count as PageCount,
    CASE
        WHEN ips.avg_fragmentation_in_percent < 10 THEN 'OK - No action needed'
        WHEN ips.avg_fragmentation_in_percent < 30 THEN 'REORGANIZE recommended'
        ELSE 'REBUILD recommended'
    END as RecommendedAction,
    CASE
        WHEN ips.avg_fragmentation_in_percent >= 30
        THEN 'ALTER INDEX [' + i.name + '] ON [' + OBJECT_NAME(ips.object_id) + '] REBUILD WITH (ONLINE = ON);'
        WHEN ips.avg_fragmentation_in_percent >= 10
        THEN 'ALTER INDEX [' + i.name + '] ON [' + OBJECT_NAME(ips.object_id) + '] REORGANIZE;'
        ELSE '-- No action required'
    END as MaintenanceScript
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE ips.page_count > 1000
ORDER BY ips.avg_fragmentation_in_percent DESC;
GO

-- 4. Top 10 Most Expensive Queries (by CPU)
SELECT TOP 10
    qs.total_worker_time / qs.execution_count as AvgCPU_microseconds,
    qs.total_worker_time as TotalCPU,
    qs.execution_count as ExecutionCount,
    qs.total_elapsed_time / qs.execution_count as AvgDuration_microseconds,
    qs.total_logical_reads / qs.execution_count as AvgLogicalReads,
    SUBSTRING(st.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset
            WHEN -1 THEN DATALENGTH(st.text)
            ELSE qs.statement_end_offset
          END - qs.statement_start_offset)/2)+1) as QueryText,
    qp.query_plan as QueryPlan
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
ORDER BY AvgCPU_microseconds DESC;
GO
```
