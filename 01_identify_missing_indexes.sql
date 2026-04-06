-- ============================================================
-- Script: 01_identify_missing_indexes.sql
-- Author: Norman Mathe | Junior DBA Portfolio
-- Purpose: Identify missing indexes using SQL Server DMVs
--          that could improve query performance
-- Environment: SQL Server 2016+ / Azure SQL
-- ============================================================

USE master;
GO

-- -------------------------------------------------------
-- 1. Top Missing Indexes by Potential Improvement Impact
-- -------------------------------------------------------
SELECT TOP 20
    DB_NAME(mid.database_id)                          AS [Database],
    OBJECT_NAME(mid.object_id, mid.database_id)       AS [Table],
    migs.avg_total_user_cost * migs.avg_user_impact
        * (migs.user_seeks + migs.user_scans)         AS [ImprovementMeasure],
    migs.user_seeks                                   AS [UserSeeks],
    migs.user_scans                                   AS [UserScans],
    migs.avg_total_user_cost                          AS [AvgQueryCostReduction],
    migs.avg_user_impact                              AS [AvgImpactPct],
    mid.equality_columns                              AS [EqualityColumns],
    mid.inequality_columns                            AS [InequalityColumns],
    mid.included_columns                              AS [IncludedColumns],
    -- Auto-generate CREATE INDEX statement
    'CREATE INDEX IX_' 
        + OBJECT_NAME(mid.object_id, mid.database_id) 
        + '_' + REPLACE(REPLACE(ISNULL(mid.equality_columns, '') 
        + ISNULL('_' + mid.inequality_columns, ''), '[', ''), ']', '')
        + ' ON ' + mid.statement 
        + ' (' + ISNULL(mid.equality_columns, '')
        + CASE WHEN mid.inequality_columns IS NOT NULL 
               THEN (CASE WHEN mid.equality_columns IS NOT NULL 
                          THEN ',' ELSE '' END) + mid.inequality_columns 
               ELSE '' END + ')'
        + ISNULL(' INCLUDE (' + mid.included_columns + ')', '')
        + ';'                                          AS [CreateIndexStatement]
FROM sys.dm_db_missing_index_groups AS mig
INNER JOIN sys.dm_db_missing_index_group_stats AS migs
    ON migs.group_handle = mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details AS mid
    ON mig.index_handle = mid.index_handle
ORDER BY [ImprovementMeasure] DESC;
GO

-- -------------------------------------------------------
-- 2. Unused Indexes (candidates for removal)
-- -------------------------------------------------------
SELECT
    DB_NAME()                                         AS [Database],
    OBJECT_NAME(i.object_id)                          AS [Table],
    i.name                                            AS [IndexName],
    i.type_desc                                       AS [IndexType],
    ius.user_seeks                                    AS [UserSeeks],
    ius.user_scans                                    AS [UserScans],
    ius.user_lookups                                  AS [UserLookups],
    ius.user_updates                                  AS [UserUpdates],
    ius.last_user_seek                                AS [LastUserSeek],
    ius.last_user_scan                                AS [LastUserScan]
FROM sys.indexes AS i
LEFT JOIN sys.dm_db_index_usage_stats AS ius
    ON i.object_id = ius.object_id
    AND i.index_id = ius.index_id
    AND ius.database_id = DB_ID()
WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
  AND i.index_id > 1                                  -- Exclude heaps & clustered
  AND (ius.user_seeks = 0 OR ius.user_seeks IS NULL)
  AND (ius.user_scans = 0 OR ius.user_scans IS NULL)
  AND (ius.user_lookups = 0 OR ius.user_lookups IS NULL)
ORDER BY ius.user_updates DESC;
GO

-- -------------------------------------------------------
-- 3. Index Fragmentation Analysis
-- -------------------------------------------------------
SELECT
    DB_NAME()                                         AS [Database],
    OBJECT_NAME(ips.object_id)                        AS [Table],
    i.name                                            AS [IndexName],
    ips.index_type_desc                               AS [IndexType],
    ROUND(ips.avg_fragmentation_in_percent, 2)        AS [FragmentationPct],
    ips.page_count                                    AS [PageCount],
    CASE
        WHEN ips.avg_fragmentation_in_percent < 10   THEN 'OK - No action needed'
        WHEN ips.avg_fragmentation_in_percent < 30   THEN 'REORGANIZE recommended'
        ELSE                                              'REBUILD recommended'
    END                                               AS [RecommendedAction],
    CASE
        WHEN ips.avg_fragmentation_in_percent >= 30
        THEN 'ALTER INDEX [' + i.name + '] ON [' 
             + OBJECT_NAME(ips.object_id) + '] REBUILD WITH (ONLINE = ON);'
        WHEN ips.avg_fragmentation_in_percent >= 10
        THEN 'ALTER INDEX [' + i.name + '] ON [' 
             + OBJECT_NAME(ips.object_id) + '] REORGANIZE;'
        ELSE '-- No action required'
    END                                               AS [MaintenanceScript]
FROM sys.dm_db_index_physical_stats(
        DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ips
INNER JOIN sys.indexes AS i
    ON ips.object_id = i.object_id
    AND ips.index_id = i.index_id
WHERE ips.page_count > 1000                          -- Ignore tiny indexes
ORDER BY ips.avg_fragmentation_in_percent DESC;
GO

-- -------------------------------------------------------
-- 4. Top 10 Most Expensive Queries (CPU)
-- -------------------------------------------------------
SELECT TOP 10
    qs.total_worker_time / qs.execution_count        AS [AvgCPU_microseconds],
    qs.total_worker_time                             AS [TotalCPU],
    qs.execution_count                               AS [ExecutionCount],
    qs.total_elapsed_time / qs.execution_count       AS [AvgDuration_microseconds],
    qs.total_logical_reads / qs.execution_count      AS [AvgLogicalReads],
    SUBSTRING(st.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset
            WHEN -1 THEN DATALENGTH(st.text)
            ELSE qs.statement_end_offset
          END - qs.statement_start_offset)/2)+1)     AS [QueryText],
    qp.query_plan                                    AS [QueryPlan]
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
ORDER BY [AvgCPU_microseconds] DESC;
GO
