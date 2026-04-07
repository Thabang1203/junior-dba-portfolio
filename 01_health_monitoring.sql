USE master;
GO

-- 1. SQL Server CPU Utilisation
DECLARE @ts_now BIGINT = (SELECT cpu_ticks / (cpu_ticks/ms_ticks) FROM sys.dm_os_sys_info);

SELECT TOP 15
    DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) as SampleTime,
    SQLProcessUtilization as SQL_CPU_Pct,
    SystemIdle as Idle_Pct,
    100 - SystemIdle - SQLProcessUtilization as Other_CPU_Pct
FROM (
    SELECT 
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') as SQLProcessUtilization,
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') as SystemIdle,
        [timestamp]
    FROM (
        SELECT [timestamp], CONVERT(XML, record) as record
        FROM sys.dm_os_ring_buffers
        WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR'
          AND record LIKE '%<SystemHealth>%'
    ) x
) y
ORDER BY record_id DESC;
GO

-- 2. Memory Usage Overview
SELECT
    physical_memory_in_use_kb / 1024 as MemInUse_MB,
    page_fault_count as PageFaults,
    memory_utilization_percentage as MemUtilPct
FROM sys.dm_os_process_memory;

-- Buffer pool usage
SELECT
    COUNT(*) * 8 / 1024 as BufferPool_MB,
    SUM(CASE WHEN is_modified = 1 THEN 1 ELSE 0 END) * 8 / 1024 as DirtyPages_MB
FROM sys.dm_os_buffer_descriptors
WHERE database_id = DB_ID();
GO

-- 3. Top Wait Statistics
SELECT TOP 15
    wait_type as WaitType,
    waiting_tasks_count as WaitCount,
    ROUND(wait_time_ms / 1000.0, 1) as WaitTime_s,
    ROUND(max_wait_time_ms / 1000.0, 1) as MaxWait_s,
    ROUND(signal_wait_time_ms / 1000.0, 1) as SignalWait_s,
    ROUND(100.0 * wait_time_ms / SUM(wait_time_ms) OVER(), 2) as WaitPct,
    CASE
        WHEN wait_type LIKE 'LCK%' THEN 'Locking - check for blocking queries'
        WHEN wait_type LIKE 'PAGEIO%' THEN 'Disk I/O - consider faster storage'
        WHEN wait_type = 'CXPACKET' THEN 'Parallelism - review MAXDOP setting'
        WHEN wait_type = 'SOS_SCHEDULER_YIELD' THEN 'CPU pressure'
        WHEN wait_type = 'ASYNC_NETWORK_IO' THEN 'Network or client slowness'
        WHEN wait_type LIKE 'LATCH%' THEN 'Memory contention'
        ELSE 'Review documentation'
    END as Diagnosis
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    'SLEEP_TASK','LAZYWRITER_SLEEP','SQLTRACE_BUFFER_FLUSH','CLR_AUTO_EVENT',
    'REQUEST_FOR_DEADLOCK_MONITOR','DISPATCHER_QUEUE_SEMAPHORE','CHECKPOINT_QUEUE',
    'DBMIRROR_EVENTS_QUEUE','SQLTRACE_INCREMENTAL_FLUSH_SLEEP','WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
    'XE_DISPATCHER_WAIT','XE_TIMER_EVENT','BROKER_TO_FLUSH','BROKER_TASK_STOP',
    'CLR_MANUAL_EVENT','SNI_HTTP_ACCEPT','SLEEP_DBSTARTUP','SLEEP_DBRECOVER',
    'SLEEP_MASTERDBREADY','SLEEP_MASTERMDREADY','SLEEP_MASTERUPGRADED','SLEEP_MSDBSTARTUP',
    'SLEEP_SYSTEMTASK','SLEEP_TEMPDBSTARTUP','SP_SERVER_DIAGNOSTICS_SLEEP','WAITFOR',
    'HADR_WORK_QUEUE','ONDEMAND_TASK_QUEUE','RESOURCE_QUEUE','SERVER_IDLE_CHECK',
    'BROKER_EVENTHANDLER','BROKER_RECEIVE_WAITFOR','DISPATCHER_QUEUE_SEMAPHORE'
)
ORDER BY WaitTime_s DESC;
GO

-- 4. Active Blocking Chains
WITH BlockingChain AS (
    SELECT 
        s.session_id as SessionID,
        s.blocking_session_id as BlockedBy,
        r.wait_type as WaitType,
        r.wait_time / 1000 as WaitSec,
        r.status as Status,
        SUBSTRING(st.text, (r.statement_start_offset/2)+1,
            ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
              ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) as CurrentSQL,
        s.login_name as Login,
        s.host_name as Host,
        s.program_name as Program,
        s.cpu_time as CPU_ms,
        s.total_elapsed_time / 1000 as ElapsedSec,
        DB_NAME(r.database_id) as DatabaseName
    FROM sys.dm_exec_sessions s
    LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
    OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
    WHERE s.is_user_process = 1
)
SELECT 
    REPLICATE('  ', (SELECT COUNT(*) FROM BlockingChain b2 WHERE b2.SessionID = bc.BlockedBy)) + 
    CAST(bc.SessionID AS VARCHAR) as SessionTree,
    bc.BlockedBy,
    bc.WaitType,
    bc.WaitSec,
    bc.Status,
    bc.Login,
    bc.Host,
    bc.DatabaseName,
    LEFT(bc.CurrentSQL, 150) as SQLPreview
FROM BlockingChain bc
WHERE bc.BlockedBy > 0 OR EXISTS (SELECT 1 FROM BlockingChain b3 WHERE b3.BlockedBy = bc.SessionID)
ORDER BY bc.BlockedBy, bc.SessionID;
GO

-- 5. I/O Performance by Database File
SELECT
    DB_NAME(vfs.database_id) as DatabaseName,
    mf.physical_name as FilePath,
    mf.type_desc as FileType,
    vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads, 0) as AvgReadStall_ms,
    vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes, 0) as AvgWriteStall_ms,
    vfs.num_of_reads as TotalReads,
    vfs.num_of_writes as TotalWrites,
    ROUND(vfs.num_of_bytes_read / 1073741824.0, 2) as ReadGB,
    ROUND(vfs.num_of_bytes_written / 1073741824.0, 2) as WriteGB,
    CASE
        WHEN vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads, 0) > 100 THEN 'High read latency'
        WHEN vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes, 0) > 100 THEN 'High write latency'
        ELSE 'OK'
    END as IOStatus
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
INNER JOIN sys.master_files mf
    ON vfs.database_id = mf.database_id
    AND vfs.file_id = mf.file_id
WHERE vfs.database_id > 4
ORDER BY vfs.io_stall_read_ms DESC;
GO

-- 6. SQL Server Error Log Summary (last 24 hours)
CREATE TABLE #ErrorLog (
    LogDate DATETIME, 
    ProcessInfo VARCHAR(50), 
    [Text] VARCHAR(MAX)
);
INSERT INTO #ErrorLog EXEC xp_readerrorlog 0, 1;

SELECT
    LogDate as Time,
    ProcessInfo as Process,
    LEFT([Text], 200) as Message,
    CASE 
        WHEN [Text] LIKE '%Error%' THEN 'ERROR'
        WHEN [Text] LIKE '%Warning%' THEN 'WARNING'
        WHEN [Text] LIKE '%Failed%' THEN 'FAILED'
        ELSE 'INFO'
    END as Severity
FROM #ErrorLog
WHERE LogDate > DATEADD(HOUR, -24, GETDATE())
  AND [Text] NOT LIKE '%This is an informational message%'
ORDER BY LogDate DESC;

DROP TABLE #ErrorLog;
GO
