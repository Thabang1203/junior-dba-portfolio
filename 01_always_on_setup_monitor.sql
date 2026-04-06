-- ============================================================
-- Script: 01_always_on_setup_monitor.sql
-- Author: Norman Mathe | Junior DBA Portfolio
-- Purpose: Always On Availability Groups setup guidance,
--          health monitoring, and failover management
-- Environment: SQL Server 2016+ Enterprise/Developer
-- ============================================================

-- NOTE: Steps 1-3 require Windows Server Failover Cluster (WSFC)
-- and are run in sequence on Primary then Secondary replicas.
-- This script documents AND automates the process.

-- ============================================================
-- PHASE 1: PRIMARY REPLICA SETUP
-- ============================================================

-- 1a. Enable AlwaysOn (run on PRIMARY - requires restart)
/*
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
-- Enable via SQL Server Configuration Manager or PowerShell:
-- Enable-SqlAlwaysOn -Path SQLSERVER:\SQL\PRIMARY_SERVER\DEFAULT -Force
*/

-- 1b. Set databases to FULL recovery model
USE master;
GO

-- Set recovery model for AG candidate databases
DECLARE @DBName NVARCHAR(128);
DECLARE db_cursor CURSOR FOR
    SELECT name FROM sys.databases 
    WHERE name IN ('AdventureWorks2019')
      AND recovery_model_desc != 'FULL';

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @DBName;
WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @SQL NVARCHAR(200) = 
        'ALTER DATABASE [' + @DBName + '] SET RECOVERY FULL;';
    EXEC sp_executesql @SQL;
    PRINT 'Set FULL recovery: ' + @DBName;
    FETCH NEXT FROM db_cursor INTO @DBName;
END;
CLOSE db_cursor; DEALLOCATE db_cursor;
GO

-- 1c. Create Availability Group (PRIMARY)
/*
CREATE AVAILABILITY GROUP [AG_Production]
WITH (
    AUTOMATED_BACKUP_PREFERENCE = SECONDARY,
    FAILURE_CONDITION_LEVEL = 3,
    HEALTH_CHECK_TIMEOUT = 30000,
    DB_FAILOVER = ON,
    DTC_SUPPORT = NONE
)
FOR DATABASE [AdventureWorks2019]
REPLICA ON 
    'PRIMARY-SQL01' WITH (
        ENDPOINT_URL = 'TCP://PRIMARY-SQL01.domain.local:5022',
        AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
        FAILOVER_MODE = AUTOMATIC,
        SEEDING_MODE = AUTOMATIC,
        SESSION_TIMEOUT = 10,
        BACKUP_PRIORITY = 50,
        SECONDARY_ROLE(ALLOW_CONNECTIONS = NO)
    ),
    'SECONDARY-SQL02' WITH (
        ENDPOINT_URL = 'TCP://SECONDARY-SQL02.domain.local:5022',
        AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
        FAILOVER_MODE = AUTOMATIC,
        SEEDING_MODE = AUTOMATIC,
        SESSION_TIMEOUT = 10,
        BACKUP_PRIORITY = 50,
        SECONDARY_ROLE(ALLOW_CONNECTIONS = READ_ONLY)
    );
GO

-- Create listener
ALTER AVAILABILITY GROUP [AG_Production]
ADD LISTENER 'AG-Listener' (
    WITH IP ((N'10.0.0.50', N'255.255.255.0')),
    PORT = 1433
);
*/

-- ============================================================
-- PHASE 2: SECONDARY REPLICA SETUP (run on SECONDARY)
-- ============================================================
/*
-- Join secondary to AG
ALTER AVAILABILITY GROUP [AG_Production] JOIN;
ALTER AVAILABILITY GROUP [AG_Production] GRANT CREATE ANY DATABASE;
*/

-- ============================================================
-- PHASE 3: MONITORING & HEALTH CHECKS
-- ============================================================
USE master;
GO

-- 3a. AG Overview Dashboard
SELECT
    ag.name                                       AS [AvailabilityGroup],
    ar.replica_server_name                        AS [Replica],
    ar.availability_mode_desc                     AS [SyncMode],
    ar.failover_mode_desc                         AS [FailoverMode],
    ars.role_desc                                 AS [Role],
    ars.connected_state_desc                      AS [ConnectionState],
    ars.synchronization_health_desc               AS [SyncHealth],
    ars.operational_state_desc                    AS [OperationalState],
    arl.last_commit_time                          AS [LastCommit],
    arl.last_redone_time                          AS [LastRedone]
FROM sys.availability_groups AS ag
INNER JOIN sys.availability_replicas AS ar 
    ON ag.group_id = ar.group_id
INNER JOIN sys.dm_hadr_availability_replica_states AS ars 
    ON ar.replica_id = ars.replica_id
LEFT JOIN sys.dm_hadr_database_replica_states AS arl 
    ON ars.replica_id = arl.replica_id
ORDER BY ag.name, ars.role_desc;
GO

-- 3b. Log Send & Redo Queue (data loss potential)
SELECT
    ag.name                                       AS [AG],
    DB_NAME(drs.database_id)                      AS [Database],
    ar.replica_server_name                        AS [Replica],
    drs.synchronization_state_desc               AS [SyncState],
    drs.log_send_queue_size                       AS [LogSendQueue_KB],
    drs.log_send_rate                             AS [LogSendRate_KB_s],
    drs.redo_queue_size                           AS [RedoQueue_KB],
    drs.redo_rate                                 AS [RedoRate_KB_s],
    CASE 
        WHEN drs.log_send_queue_size > 51200  THEN '🔴 SEND QUEUE > 50MB'
        WHEN drs.redo_queue_size > 51200      THEN '🔴 REDO QUEUE > 50MB'
        ELSE '✓ OK'
    END                                           AS [DataLagStatus]
FROM sys.dm_hadr_database_replica_states AS drs
INNER JOIN sys.availability_replicas AS ar 
    ON drs.replica_id = ar.replica_id
INNER JOIN sys.availability_groups AS ag 
    ON ar.group_id = ag.group_id
ORDER BY drs.log_send_queue_size DESC;
GO

-- 3c. Estimated Data Loss (RPO) and Recovery Time (RTO)
SELECT
    ag.name                                       AS [AG],
    DB_NAME(drs.database_id)                      AS [Database],
    ar.replica_server_name                        AS [Replica],
    CASE WHEN drs.log_send_rate > 0
        THEN CAST(drs.log_send_queue_size / drs.log_send_rate AS VARCHAR) + 's'
        ELSE 'Unknown'
    END                                           AS [EstimatedRPO],
    CASE WHEN drs.redo_rate > 0
        THEN CAST(drs.redo_queue_size / drs.redo_rate AS VARCHAR) + 's'
        ELSE 'Unknown'
    END                                           AS [EstimatedRTO_Catchup]
FROM sys.dm_hadr_database_replica_states AS drs
INNER JOIN sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
INNER JOIN sys.availability_groups AS ag ON ar.group_id = ag.group_id;
GO

-- ============================================================
-- PHASE 4: MANUAL FAILOVER PROCEDURE
-- ============================================================
CREATE OR ALTER PROCEDURE dbo.usp_AGManualFailover
    @AGName          NVARCHAR(128),
    @TargetReplica   NVARCHAR(128),
    @Planned         BIT = 1           -- 1=planned, 0=forced (data loss risk)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);

    -- Check current primary
    DECLARE @CurrentPrimary NVARCHAR(128);
    SELECT @CurrentPrimary = ar.replica_server_name
    FROM sys.dm_hadr_availability_replica_states AS ars
    INNER JOIN sys.availability_replicas AS ar ON ars.replica_id = ar.replica_id
    INNER JOIN sys.availability_groups AS ag ON ar.group_id = ag.group_id
    WHERE ag.name = @AGName AND ars.role_desc = 'PRIMARY';

    PRINT '=== AVAILABILITY GROUP FAILOVER ===';
    PRINT 'AG: ' + @AGName;
    PRINT 'Current Primary: ' + ISNULL(@CurrentPrimary, 'UNKNOWN');
    PRINT 'Target Replica: ' + @TargetReplica;
    PRINT 'Failover Type: ' + CASE WHEN @Planned = 1 THEN 'PLANNED (no data loss)' ELSE '⚠ FORCED (potential data loss)' END;

    IF @Planned = 1
    BEGIN
        -- Planned failover: must run on TARGET replica
        PRINT 'Run this on ' + @TargetReplica + ':';
        PRINT 'ALTER AVAILABILITY GROUP [' + @AGName + '] FAILOVER;';
    END
    ELSE
    BEGIN
        PRINT '⚠ WARNING: Forced failover may cause data loss!';
        PRINT 'Run this on ' + @TargetReplica + ':';
        PRINT 'ALTER AVAILABILITY GROUP [' + @AGName + '] FORCE_FAILOVER_ALLOW_DATA_LOSS;';
    END;

    PRINT '';
    PRINT 'Post-failover: Update connection strings to point to listener.';
END;
GO
