USE master;
GO

-- 1. Active Sessions & Long-Running Queries
SELECT
    r.session_id as SPID,
    r.status as Status,
    r.blocking_session_id as BlockedBy,
    DB_NAME(r.database_id) as DatabaseName,
    r.wait_type as WaitType,
    r.wait_time / 1000 as WaitSec,
    r.total_elapsed_time / 1000 as ElapsedSec,
    r.cpu_time as CPU_ms,
    r.logical_reads as LogicalReads,
    r.reads as PhysicalReads,
    r.writes as Writes,
    s.login_name as Login,
    s.host_name as Host,
    s.program_name as Program,
    LEFT(SUBSTRING(st.text, (r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
          ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1), 200) as CurrentSQL
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
WHERE s.is_user_process = 1
ORDER BY r.total_elapsed_time DESC;
GO

-- 2. Deadlock Detection
SELECT
    xdr.value('@timestamp', 'datetime2') as DeadlockTime,
    xdr.query('.') as DeadlockGraph
FROM (
    SELECT CAST(target_data AS XML) as target_data
    FROM sys.dm_xe_session_targets t
    INNER JOIN sys.dm_xe_sessions s ON t.event_session_address = s.address
    WHERE s.name = 'system_health'
      AND t.target_name = 'ring_buffer'
) data
CROSS APPLY target_data.nodes('//RingBufferTarget/event[@name="xml_deadlock_report"]') as xEventData(xdr)
ORDER BY DeadlockTime DESC;
GO

-- 3. Database Integrity Check
CREATE OR ALTER PROCEDURE usp_IntegrityCheck
    @DatabaseName   NVARCHAR(128),
    @RepairMode     NVARCHAR(30) = NULL,
    @PrintOnly      BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);

    SELECT TOP 1
        bs.database_name,
        DATABASEPROPERTY(@DatabaseName, 'IsSuspect') as IsSuspect,
        DATABASEPROPERTY(@DatabaseName, 'IsInRecovery') as InRecovery
    FROM sys.databases bs WHERE bs.name = @DatabaseName;

    SET @SQL = 'DBCC CHECKDB ([' + @DatabaseName + ']) WITH NO_INFOMSGS, ALL_ERRORMSGS'
        + CASE WHEN @RepairMode IS NOT NULL THEN ', ' + @RepairMode ELSE '' END + ';';

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

-- 4. Kill Long-Running Sessions with safeguards
CREATE OR ALTER PROCEDURE usp_KillSession
    @SPID           INT,
    @MaxElapsedMin  INT = 60,
    @PrintOnly      BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ElapsedMin INT;
    DECLARE @Login NVARCHAR(128);
    DECLARE @Host NVARCHAR(128);

    SELECT 
        @ElapsedMin = total_elapsed_time / 60000,
        @Login = s.login_name,
        @Host = s.host_name
    FROM sys.dm_exec_requests r
    INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
    WHERE r.session_id = @SPID;

    IF @ElapsedMin IS NULL
    BEGIN
        PRINT 'SPID ' + CAST(@SPID AS VARCHAR) + ' not found or not active.';
        RETURN;
    END;

    IF @SPID < 51
    BEGIN
        PRINT 'Cannot kill system SPID ' + CAST(@SPID AS VARCHAR);
        RETURN;
    END;

    IF @ElapsedMin < @MaxElapsedMin
    BEGIN
        PRINT 'Skipped: SPID ' + CAST(@SPID AS VARCHAR) + ' has only been running ' + CAST(@ElapsedMin AS VARCHAR) + ' min (threshold: ' + CAST(@MaxElapsedMin AS VARCHAR) + ' min)';
        RETURN;
    END;

    PRINT 'Target: SPID ' + CAST(@SPID AS VARCHAR) + ' | Login: ' + ISNULL(@Login, 'N/A') + ' | Host: ' + ISNULL(@Host, 'N/A') + ' | Elapsed: ' + CAST(@ElapsedMin AS VARCHAR) + ' min';

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

-- 5. Patch Management - Version Check
SELECT
    @@SERVERNAME as ServerName,
    @@VERSION as FullVersion,
    SERVERPROPERTY('ProductVersion') as Version,
    SERVERPROPERTY('ProductLevel') as SP_Level,
    SERVERPROPERTY('ProductUpdateLevel') as CU_Level,
    SERVERPROPERTY('Edition') as Edition,
    SERVERPROPERTY('EngineEdition') as EngineEditionID,
    SERVERPROPERTY('IsIntegratedSecurityOnly') as WindowsAuthOnly,
    SERVERPROPERTY('Collation') as Collation,
    SERVERPROPERTY('IsClustered') as IsClustered,
    SERVERPROPERTY('IsHadrEnabled') as AlwaysOnEnabled;
GO

-- 6. Pre-Patch Checklist
PRINT 'Pre-patch checklist';
PRINT 'Server: ' + @@SERVERNAME;
PRINT 'Current Version: ' + CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR);
PRINT 'Patch Date: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';

-- Check for active jobs
SELECT 
    j.name as RunningJob,
    ja.start_execution_date as StartedAt,
    DATEDIFF(MINUTE, ja.start_execution_date, GETDATE()) as RunningMin
FROM msdb.dbo.sysjobactivity ja
INNER JOIN msdb.dbo.sysjobs j ON ja.job_id = j.job_id
WHERE ja.start_execution_date IS NOT NULL
  AND ja.stop_execution_date IS NULL
  AND ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.sysjobactivity);

-- Active user connections
SELECT 
    COUNT(*) as ActiveUserConnections,
    CASE WHEN COUNT(*) > 0 THEN 'Notify users before patching' ELSE 'No active users' END as Status
FROM sys.dm_exec_sessions
WHERE is_user_process = 1;

-- AG sync state
SELECT
    ag.name,
    ars.role_desc,
    ars.synchronization_health_desc,
    CASE WHEN ars.synchronization_health_desc = 'HEALTHY' THEN 'Healthy' ELSE 'Check before patching' END as PatchReady
FROM sys.availability_groups ag
INNER JOIN sys.dm_hadr_availability_replica_states ars ON ag.group_id = ars.group_id;
GO
