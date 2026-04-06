-- ============================================================
-- Script: 01_troubleshooting_toolkit.sql
-- Author: Norman Mathe | Junior DBA Portfolio
-- Purpose: SQL Server incident response and troubleshooting
--          toolkit - deadlock analysis, long-running queries,
--          corruption detection
-- Environment: SQL Server 2016+
-- ============================================================

USE master;
GO

-- -------------------------------------------------------
-- 1. Active Sessions & Long-Running Queries
-- -------------------------------------------------------
SELECT
    r.session_id                                  AS [SPID],
    r.status                                      AS [Status],
    r.blocking_session_id                         AS [BlockedBy],
    DB_NAME(r.database_id)                        AS [Database],
    r.wait_type                                   AS [WaitType],
    r.wait_time / 1000                            AS [WaitSec],
    r.total_elapsed_time / 1000                   AS [ElapsedSec],
    r.cpu_time                                    AS [CPU_ms],
    r.logical_reads                               AS [LogicalReads],
    r.reads                                       AS [PhysicalReads],
    r.writes                                      AS [Writes],
    s.login_name                                  AS [Login],
    s.host_name                                   AS [Host],
    s.program_name                                AS [Program],
    LEFT(SUBSTRING(st.text, 
        (r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
          ELSE r.statement_end_offset END 
          - r.statement_start_offset)/2)+1), 200) AS [CurrentSQL]
FROM sys.dm_exec_requests AS r
INNER JOIN sys.dm_exec_sessions AS s ON r.session_id = s.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
WHERE s.is_user_process = 1
ORDER BY r.total_elapsed_time DESC;
GO

-- -------------------------------------------------------
-- 2. Deadlock Detection & Analysis
-- -------------------------------------------------------
-- Extract deadlock XML from system_health extended event
SELECT
    xdr.value('@timestamp', 'datetime2')          AS [DeadlockTime],
    xdr.query('.')                                AS [DeadlockGraph]
FROM (
    SELECT CAST(target_data AS XML) AS target_data
    FROM sys.dm_xe_session_targets AS t
    INNER JOIN sys.dm_xe_sessions AS s ON t.event_session_address = s.address
    WHERE s.name = 'system_health'
      AND t.target_name = 'ring_buffer'
) AS data
CROSS APPLY target_data.nodes('//RingBufferTarget/event[@name="xml_deadlock_report"]') 
    AS xEventData(xdr)
ORDER BY [DeadlockTime] DESC;
GO

-- -------------------------------------------------------
-- 3. Database Integrity Check (DBCC CHECKDB Wrapper)
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_IntegrityCheck
    @DatabaseName   NVARCHAR(128),
    @RepairMode     NVARCHAR(30) = NULL,      -- NULL, REPAIR_REBUILD, REPAIR_ALLOW_DATA_LOSS
    @PrintOnly      BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);

    -- Check last known clean DBCC result
    SELECT TOP 1
        bs.database_name,
        DATABASEPROPERTY(@DatabaseName, 'IsSuspect')  AS [IsSuspect],
        DATABASEPROPERTY(@DatabaseName, 'IsInRecovery') AS [InRecovery]
    FROM sys.databases bs WHERE bs.name = @DatabaseName;

    -- Build CHECKDB command
    SET @SQL = 'DBCC CHECKDB ([' + @DatabaseName + ']) WITH NO_INFOMSGS, ALL_ERRORMSGS'
        + CASE WHEN @RepairMode IS NOT NULL 
               THEN ', ' + @RepairMode 
               ELSE '' END + ';';

    IF @PrintOnly = 1
        PRINT 'Would execute: ' + @SQL;
    ELSE
    BEGIN
        PRINT 'Running integrity check on: ' + @DatabaseName;
        PRINT 'Start: ' + CONVERT(VARCHAR, SYSDATETIME(), 120);
        EXEC sp_executesql @SQL;
        PRINT 'Complete: ' + CONVERT(VARCHAR, SYSDATETIME(), 120);
    END;
END;
GO

-- -------------------------------------------------------
-- 4. Kill Long-Running Sessions (with safeguards)
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_KillSession
    @SPID           INT,
    @MaxElapsedMin  INT = 60,         -- Only kill if older than this
    @PrintOnly      BIT = 1           -- Safety: default print-only
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ElapsedMin INT;
    DECLARE @Login      NVARCHAR(128);
    DECLARE @Host       NVARCHAR(128);

    SELECT 
        @ElapsedMin = total_elapsed_time / 60000,
        @Login      = s.login_name,
        @Host       = s.host_name
    FROM sys.dm_exec_requests AS r
    INNER JOIN sys.dm_exec_sessions AS s ON r.session_id = s.session_id
    WHERE r.session_id = @SPID;

    IF @ElapsedMin IS NULL
    BEGIN
        PRINT 'SPID ' + CAST(@SPID AS VARCHAR) + ' not found or not active.';
        RETURN;
    END;

    -- Safety checks
    IF @SPID < 51
    BEGIN
        PRINT 'BLOCKED: Cannot kill system SPID ' + CAST(@SPID AS VARCHAR);
        RETURN;
    END;

    IF @ElapsedMin < @MaxElapsedMin
    BEGIN
        PRINT 'SKIPPED: SPID ' + CAST(@SPID AS VARCHAR) 
              + ' has only been running ' + CAST(@ElapsedMin AS VARCHAR) 
              + ' min (threshold: ' + CAST(@MaxElapsedMin AS VARCHAR) + ' min)';
        RETURN;
    END;

    PRINT 'Target: SPID ' + CAST(@SPID AS VARCHAR) 
          + ' | Login: ' + ISNULL(@Login, 'N/A')
          + ' | Host: ' + ISNULL(@Host, 'N/A')
          + ' | Elapsed: ' + CAST(@ElapsedMin AS VARCHAR) + ' min';

    IF @PrintOnly = 1
        PRINT 'Would execute: KILL ' + CAST(@SPID AS VARCHAR);
    ELSE
    BEGIN
        DECLARE @KillSQL NVARCHAR(50) = 'KILL ' + CAST(@SPID AS VARCHAR);
        EXEC sp_executesql @KillSQL;
        PRINT 'Killed SPID: ' + CAST(@SPID AS VARCHAR);
    END;
END;
GO

-- -------------------------------------------------------
-- 5. Patch Management - Version & CU Check
-- -------------------------------------------------------
SELECT
    @@SERVERNAME                                  AS [ServerName],
    @@VERSION                                     AS [FullVersion],
    SERVERPROPERTY('ProductVersion')              AS [Version],
    SERVERPROPERTY('ProductLevel')                AS [SP_Level],
    SERVERPROPERTY('ProductUpdateLevel')          AS [CU_Level],
    SERVERPROPERTY('Edition')                     AS [Edition],
    SERVERPROPERTY('EngineEdition')               AS [EngineEditionID],
    SERVERPROPERTY('IsIntegratedSecurityOnly')    AS [WindowsAuthOnly],
    SERVERPROPERTY('Collation')                   AS [Collation],
    SERVERPROPERTY('IsClustered')                 AS [IsClustered],
    SERVERPROPERTY('IsHadrEnabled')               AS [AlwaysOnEnabled];
GO

-- -------------------------------------------------------
-- 6. Pre-Patch Checklist Script
-- -------------------------------------------------------
PRINT '=== PRE-PATCH CHECKLIST ===';
PRINT 'Server: ' + @@SERVERNAME;
PRINT 'Current Version: ' + CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR);
PRINT 'Patch Date: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';

-- Check for active jobs
SELECT 
    j.name                                        AS [RunningJob],
    ja.start_execution_date                       AS [StartedAt],
    DATEDIFF(MINUTE, ja.start_execution_date, GETDATE()) AS [RunningMin]
FROM msdb.dbo.sysjobactivity AS ja
INNER JOIN msdb.dbo.sysjobs AS j ON ja.job_id = j.job_id
WHERE ja.start_execution_date IS NOT NULL
  AND ja.stop_execution_date IS NULL
  AND ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.sysjobactivity);

-- Active user connections
SELECT 
    COUNT(*) AS [ActiveUserConnections],
    CASE WHEN COUNT(*) > 0 THEN '⚠ NOTIFY USERS BEFORE PATCHING' 
         ELSE '✓ No active users' END AS [Status]
FROM sys.dm_exec_sessions
WHERE is_user_process = 1;

-- AG sync state
SELECT
    ag.name,
    ars.role_desc,
    ars.synchronization_health_desc,
    CASE WHEN ars.synchronization_health_desc = 'HEALTHY' 
         THEN '✓ HEALTHY' ELSE '⚠ CHECK BEFORE PATCHING' END AS [PatchReady]
FROM sys.availability_groups AS ag
INNER JOIN sys.dm_hadr_availability_replica_states AS ars 
    ON ag.group_id = ars.group_id;
GO
