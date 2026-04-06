-- ============================================================
-- Script: 01_health_monitoring.sql
-- Author: Norman Mathe | Junior DBA Portfolio
-- Purpose: Comprehensive SQL Server health dashboard using
--          DMVs - CPU, memory, I/O, blocking, wait stats
-- Environment: SQL Server 2016+
-- ============================================================

USE master;
GO

-- -------------------------------------------------------
-- 1. SQL Server CPU Utilisation (Ring Buffer)
-- -------------------------------------------------------
DECLARE @ts_now BIGINT = (SELECT cpu_ticks / (cpu_ticks/ms_ticks)
                          FROM sys.dm_os_sys_info);

SELECT TOP 15
    DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) AS [SampleTime],
    SQLProcessUtilization                                 AS [SQL_CPU_Pct],
    SystemIdle                                            AS [Idle_Pct],
    100 - SystemIdle - SQLProcessUtilization              AS [Other_CPU_Pct]
FROM (
    SELECT 
        record.value('(./Record/@id)[1]', 'int')              AS record_id,
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQLProcessUtilization,
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle,
        [timestamp]
    FROM (
        SELECT [timestamp], CONVERT(XML, record) AS record
        FROM sys.dm_os_ring_buffers
        WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
          AND record LIKE N'%<SystemHealth>%'
    ) AS x
) AS y
ORDER BY record_id DESC;
GO

-- -------------------------------------------------------
-- 2. Memory Usage Overview
-- -------------------------------------------------------
SELECT
    physical_memory_in_use_kb / 1024              AS [MemInUse_MB],
    page_fault_count                              AS [PageFaults],
    memory_utilization_percentage                 AS [MemUtilPct]
FROM sys.dm_os_process_memory;

-- Buffer pool usage
SELECT
    COUNT(*) * 8 / 1024                           AS [BufferPool_MB],
    SUM(CASE WHEN is_modified = 1 THEN 1 ELSE 0 END) * 8 / 1024 AS [DirtyPages_MB]
FROM sys.dm_os_buffer_descriptors
WHERE database_id = DB_ID();
GO

-- -------------------------------------------------------
-- 3. Top Wait Statistics
-- -------------------------------------------------------
SELECT TOP 15
    wait_type                                     AS [WaitType],
    waiting_tasks_count                           AS [WaitCount],
    ROUND(wait_time_ms / 1000.0, 1)               AS [WaitTime_s],
    ROUND(max_wait_time_ms / 1000.0, 1)           AS [MaxWait_s],
    ROUND(signal_wait_time_ms / 1000.0, 1)        AS [SignalWait_s],
    ROUND(100.0 * wait_time_ms / 
        SUM(wait_time_ms) OVER(), 2)              AS [WaitPct],
    CASE
        WHEN wait_type LIKE 'LCK%'    THEN 'Locking - check for blocking queries'
        WHEN wait_type LIKE 'PAGEIO%' THEN 'Disk I/O - consider faster storage'
        WHEN wait_type = 'CXPACKET'   THEN 'Parallelism - review MAXDOP setting'
        WHEN wait_type = 'SOS_SCHEDULER_YIELD' THEN 'CPU pressure'
        WHEN wait_type = 'ASYNC_NETWORK_IO'    THEN 'Network/Client slowness'
        WHEN wait_type LIKE 'LATCH%'  THEN 'Memory contention'
        ELSE 'Review Microsoft docs'
    END                                           AS [Diagnosis]
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    'SLEEP_TASK','LAZYWRITER_SLEEP','SQLTRACE_BUFFER_FLUSH','CLR_AUTO_EVENT',
    'REQUEST_FOR_DEADLOCK_MONITOR','DISPATCHER_QUEUE_SEMAPHORE',
    'CHECKPOINT_QUEUE','DBMIRROR_EVENTS_QUEUE','SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
    'WAIT_XTP_OFFLINE_CKPT_NEW_LOG','XE_DISPATCHER_WAIT','XE_TIMER_EVENT',
    'BROKER_TO_FLUSH','BROKER_TASK_STOP','CLR_MANUAL_EVENT','SNI_HTTP_ACCEPT',
    'SLEEP_DBSTARTUP','SLEEP_DBRECOVER','SLEEP_MASTERDBREADY','SLEEP_MASTERMDREADY',
    'SLEEP_MASTERUPGRADED','SLEEP_MSDBSTARTUP','SLEEP_SYSTEMTASK','SLEEP_TEMPDBSTARTUP',
    'SNI_HTTP_ACCEPT','SP_SERVER_DIAGNOSTICS_SLEEP','WAITFOR','HADR_WORK_QUEUE',
    'ONDEMAND_TASK_QUEUE','REQUEST_FOR_DEADLOCK_MONITOR','RESOURCE_QUEUE',
    'SERVER_IDLE_CHECK','SLEEP_DBSTARTUP','SLEEP_DBRECOVER','WAITFOR',
    'BROKER_EVENTHANDLER','BROKER_RECEIVE_WAITFOR','DISPATCHER_QUEUE_SEMAPHORE'
)
ORDER BY WaitTime_s DESC;
GO

-- -------------------------------------------------------
-- 4. Active Blocking Chains
-- -------------------------------------------------------
WITH BlockingChain AS (
    SELECT 
        s.session_id                              AS [SessionID],
        s.blocking_session_id                     AS [BlockedBy],
        r.wait_type                               AS [WaitType],
        r.wait_time / 1000                        AS [WaitSec],
        r.status                                  AS [Status],
        SUBSTRING(st.text, (r.statement_start_offset/2)+1,
            ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
              ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1)
                                                  AS [CurrentSQL],
        s.login_name                              AS [Login],
        s.host_name                               AS [Host],
        s.program_name                            AS [Program],
        s.cpu_time                                AS [CPU_ms],
        s.total_elapsed_time / 1000               AS [ElapsedSec],
        DB_NAME(r.database_id)                    AS [Database]
    FROM sys.dm_exec_sessions AS s
    LEFT JOIN sys.dm_exec_requests AS r ON s.session_id = r.session_id
    OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
    WHERE s.is_user_process = 1
)
SELECT 
    REPLICATE('  ', 
        (SELECT COUNT(*) FROM BlockingChain b2 
         WHERE b2.SessionID = bc.BlockedBy)) + 
    CAST(bc.SessionID AS VARCHAR)                 AS [SessionTree],
    bc.BlockedBy,
    bc.WaitType,
    bc.WaitSec,
    bc.Status,
    bc.Login,
    bc.Host,
    bc.Database,
    LEFT(bc.CurrentSQL, 150)                      AS [SQLPreview]
FROM BlockingChain AS bc
WHERE bc.BlockedBy > 0 OR EXISTS (
    SELECT 1 FROM BlockingChain b3 WHERE b3.BlockedBy = bc.SessionID
)
ORDER BY bc.BlockedBy, bc.SessionID;
GO

-- -------------------------------------------------------
-- 5. I/O Performance by Database File
-- -------------------------------------------------------
SELECT
    DB_NAME(vfs.database_id)                      AS [Database],
    mf.physical_name                              AS [File],
    mf.type_desc                                  AS [FileType],
    vfs.io_stall_read_ms  / 
        NULLIF(vfs.num_of_reads, 0)               AS [AvgReadStall_ms],
    vfs.io_stall_write_ms / 
        NULLIF(vfs.num_of_writes, 0)              AS [AvgWriteStall_ms],
    vfs.num_of_reads                              AS [TotalReads],
    vfs.num_of_writes                             AS [TotalWrites],
    ROUND(vfs.num_of_bytes_read / 1073741824.0, 2)  AS [ReadGB],
    ROUND(vfs.num_of_bytes_written / 1073741824.0, 2) AS [WriteGB],
    CASE
        WHEN vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads, 0) > 100
        THEN '⚠ HIGH READ LATENCY'
        WHEN vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes, 0) > 100
        THEN '⚠ HIGH WRITE LATENCY'
        ELSE '✓ OK'
    END                                           AS [IOStatus]
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
INNER JOIN sys.master_files AS mf
    ON vfs.database_id = mf.database_id
    AND vfs.file_id = mf.file_id
WHERE vfs.database_id > 4
ORDER BY vfs.io_stall_read_ms DESC;
GO

-- -------------------------------------------------------
-- 6. SQL Server Error Log Summary (last 24h)
-- -------------------------------------------------------
CREATE TABLE #ErrorLog (
    LogDate DATETIME, ProcessInfo VARCHAR(50), [Text] VARCHAR(MAX)
);
INSERT INTO #ErrorLog EXEC xp_readerrorlog 0, 1;

SELECT
    LogDate                                       AS [Time],
    ProcessInfo                                   AS [Process],
    LEFT([Text], 200)                             AS [Message],
    CASE 
        WHEN [Text] LIKE '%Error%'    THEN '🔴 ERROR'
        WHEN [Text] LIKE '%Warning%'  THEN '🟡 WARNING'
        WHEN [Text] LIKE '%Failed%'   THEN '🔴 FAILED'
        ELSE                               'ℹ INFO'
    END                                           AS [Severity]
FROM #ErrorLog
WHERE LogDate > DATEADD(HOUR, -24, GETDATE())
  AND [Text] NOT LIKE '%This is an informational message%'
ORDER BY LogDate DESC;

DROP TABLE #ErrorLog;
GO
