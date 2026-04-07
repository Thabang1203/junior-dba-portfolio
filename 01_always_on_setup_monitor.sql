-- All SQL Server DBA Scripts
-- Written by Norman Mathe

USE master;
GO

-- ALWAYS ON AVAILABILITY GROUPS

-- Set database to FULL recovery mode
ALTER DATABASE AdventureWorks2019 SET RECOVERY FULL;
GO

-- Check AG status
SELECT 
    ag.name,
    ar.replica_server_name,
    ar.availability_mode_desc,
    ar.failover_mode_desc,
    ars.role_desc,
    ars.connected_state_desc,
    ars.synchronization_health_desc
FROM sys.availability_groups ag
INNER JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
INNER JOIN sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id;
GO

-- Check data sync status
SELECT 
    DB_NAME(drs.database_id) as DatabaseName,
    ar.replica_server_name,
    drs.synchronization_state_desc,
    drs.log_send_queue_size,
    drs.redo_queue_size
FROM sys.dm_hadr_database_replica_states drs
INNER JOIN sys.availability_replicas ar ON drs.replica_id = ar.replica_id;
GO

-- Stored procedure for manual failover
CREATE OR ALTER PROCEDURE usp_AGManualFailover
    @AGName NVARCHAR(128),
    @TargetReplica NVARCHAR(128),
    @Planned BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentPrimary NVARCHAR(128);

    SELECT @CurrentPrimary = ar.replica_server_name
    FROM sys.dm_hadr_availability_replica_states ars
    INNER JOIN sys.availability_replicas ar ON ars.replica_id = ar.replica_id
    INNER JOIN sys.availability_groups ag ON ar.group_id = ag.group_id
    WHERE ag.name = @AGName AND ars.role_desc = 'PRIMARY';

    PRINT 'Current Primary: ' + ISNULL(@CurrentPrimary, 'Unknown');
    PRINT 'Target Replica: ' + @TargetReplica;

    IF @Planned = 1
    BEGIN
        PRINT 'Run on ' + @TargetReplica + ': ALTER AVAILABILITY GROUP [' + @AGName + '] FAILOVER;';
    END
    ELSE
    BEGIN
        PRINT 'WARNING - Run on ' + @TargetReplica + ': ALTER AVAILABILITY GROUP [' + @AGName + '] FORCE_FAILOVER_ALLOW_DATA_LOSS;';
    END;
END;
GO

-- BACKUP AND RECOVERY

-- Full backup procedure
CREATE OR ALTER PROCEDURE usp_FullBackup
    @DatabaseName NVARCHAR(128),
    @BackupPath NVARCHAR(512) = 'C:\Backups\'
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @FileName NVARCHAR(512);
    DECLARE @Timestamp VARCHAR(20) = FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
    
    SET @FileName = @BackupPath + @DatabaseName + '_FULL_' + @Timestamp + '.bak';
    
    BACKUP DATABASE @DatabaseName
    TO DISK = @FileName
    WITH COMPRESSION, CHECKSUM, STATS = 10;
    
    PRINT 'Backup completed: ' + @FileName;
    
    RESTORE VERIFYONLY FROM DISK = @FileName;
    PRINT 'Backup verified';
END;
GO

-- Log backup procedure
CREATE OR ALTER PROCEDURE usp_LogBackup
    @DatabaseName NVARCHAR(128),
    @BackupPath NVARCHAR(512) = 'C:\Backups\'
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @FileName NVARCHAR(512);
    DECLARE @Timestamp VARCHAR(20) = FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
    
    SET @FileName = @BackupPath + @DatabaseName + '_LOG_' + @Timestamp + '.trn';
    
    BACKUP LOG @DatabaseName
    TO DISK = @FileName
    WITH COMPRESSION, CHECKSUM;
    
    PRINT 'Log backup completed: ' + @FileName;
END;
GO

-- Check last backup times
SELECT 
    database_name,
    MAX(backup_finish_date) as LastBackup
FROM msdb.dbo.backupset
WHERE type = 'D'
GROUP BY database_name
ORDER BY LastBackup DESC;
GO

-- HEALTH MONITORING

-- Check CPU usage
SELECT TOP 10
    SQLProcessUtilization as SQL_CPU_Percent,
    SystemIdle as Idle_Percent
FROM (
    SELECT 
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') as SQLProcessUtilization,
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') as SystemIdle
    FROM (
        SELECT CONVERT(XML, record) as record
        FROM sys.dm_os_ring_buffers
        WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR'
    ) x
) y;
GO

-- Check memory usage
SELECT 
    physical_memory_in_use_kb / 1024 as MemoryUsedMB,
    memory_utilization_percentage as MemoryPercent
FROM sys.dm_os_process_memory;
GO

-- Find long running queries
SELECT TOP 10
    r.session_id,
    r.total_elapsed_time / 1000 as ElapsedSeconds,
    r.cpu_time as CPUMs,
    r.logical_reads,
    SUBSTRING(t.text, (r.statement_start_offset/2)+1, 
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text) ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) as QueryText
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE r.status = 'running'
ORDER BY r.total_elapsed_time DESC;
GO

-- Check blocking
SELECT 
    blocking.session_id as BlockingSPID,
    blocked.session_id as BlockedSPID,
    blocking.wait_time as WaitTimeMs,
    blocked.wait_type
FROM sys.dm_exec_requests blocking
INNER JOIN sys.dm_exec_requests blocked ON blocking.session_id = blocked.blocking_session_id;
GO

-- MISSING INDEXES

-- Top 10 missing indexes
SELECT TOP 10
    OBJECT_NAME(mid.object_id) as TableName,
    migs.avg_user_impact,
    migs.user_seeks,
    mid.equality_columns,
    mid.inequality_columns,
    mid.included_columns
FROM sys.dm_db_missing_index_groups mig
INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
WHERE mid.database_id = DB_ID()
ORDER BY migs.avg_user_impact DESC;
GO

-- Check index fragmentation
SELECT 
    OBJECT_NAME(ips.object_id) as TableName,
    i.name as IndexName,
    ips.avg_fragmentation_in_percent,
    ips.page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE ips.avg_fragmentation_in_percent > 10
AND ips.page_count > 1000
ORDER BY ips.avg_fragmentation_in_percent DESC;
GO

-- Rebuild fragmented indexes
CREATE OR ALTER PROCEDURE usp_RebuildIndexes
    @DatabaseName NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @FragPercent FLOAT;
    
    DECLARE idx_cursor CURSOR FOR
        SELECT OBJECT_NAME(ips.object_id), i.name, ips.avg_fragmentation_in_percent
        FROM sys.dm_db_index_physical_stats(DB_ID(@DatabaseName), NULL, NULL, NULL, 'LIMITED') ips
        INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
        WHERE ips.avg_fragmentation_in_percent > 30
        AND ips.page_count > 1000;
    
    OPEN idx_cursor;
    FETCH NEXT FROM idx_cursor INTO @TableName, @IndexName, @FragPercent;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT 'Rebuilding index: ' + @TableName + '.' + @IndexName;
        EXEC ('ALTER INDEX [' + @IndexName + '] ON [' + @DatabaseName + '].[dbo].[' + @TableName + '] REBUILD');
        FETCH NEXT FROM idx_cursor INTO @TableName, @IndexName, @FragPercent;
    END;
    
    CLOSE idx_cursor;
    DEALLOCATE idx_cursor;
END;
GO

-- SECURITY MANAGEMENT

-- Create read-only user
CREATE OR ALTER PROCEDURE usp_CreateReadOnlyUser
    @UserName NVARCHAR(128),
    @Password NVARCHAR(256),
    @DatabaseName NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    
    SET @SQL = 'CREATE LOGIN ' + QUOTENAME(@UserName) + ' WITH PASSWORD = ''' + @Password + ''', CHECK_POLICY = ON, CHECK_EXPIRATION = ON';
    EXEC sp_executesql @SQL;
    
    SET @SQL = 'USE ' + QUOTENAME(@DatabaseName) + '; CREATE USER ' + QUOTENAME(@UserName) + ' FOR LOGIN ' + QUOTENAME(@UserName) + '; EXEC sp_addrolemember ''db_datareader'', ' + QUOTENAME(@UserName);
    EXEC sp_executesql @SQL;
    
    PRINT 'Read only user ' + @UserName + ' created';
END;
GO

-- Check sysadmin logins
SELECT 
    p.name as LoginName,
    p.type_desc as Type,
    p.create_date
FROM sys.server_principals p
INNER JOIN sys.server_role_members rm ON p.principal_id = rm.member_principal_id
INNER JOIN sys.server_principals r ON rm.role_principal_id = r.principal_id
WHERE r.name = 'sysadmin'
AND p.type NOT IN ('R', 'G');
GO

-- Check for enabled dangerous features
SELECT 
    name as Feature,
    value_in_use as IsEnabled,
    CASE WHEN value_in_use = 1 THEN 'CHECK THIS' ELSE 'OK' END as Status
FROM sys.configurations
WHERE name IN ('xp_cmdshell', 'Ole Automation Procedures', 'Ad Hoc Distributed Queries');
GO

-- TROUBLESHOOTING TOOLKIT

-- Kill a specific session
CREATE OR ALTER PROCEDURE usp_KillSession
    @SPID INT,
    @Force BIT = 0
AS
BEGIN
    DECLARE @LoginName NVARCHAR(128);
    DECLARE @HostName NVARCHAR(128);
    
    SELECT @LoginName = login_name, @HostName = host_name
    FROM sys.dm_exec_sessions
    WHERE session_id = @SPID;
    
    PRINT 'Killing SPID: ' + CAST(@SPID AS VARCHAR) + ' (' + @LoginName + ' - ' + @HostName + ')';
    
    IF @Force = 1
    BEGIN
        DECLARE @KillCmd NVARCHAR(20) = 'KILL ' + CAST(@SPID AS VARCHAR);
        EXEC sp_executesql @KillCmd;
        PRINT 'Session killed';
    END
    ELSE
    BEGIN
        PRINT 'Use @Force=1 to actually kill the session';
    END;
END;
GO

-- Database integrity check
CREATE OR ALTER PROCEDURE usp_CheckDB
    @DatabaseName NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;
    
    PRINT 'Checking database: ' + @DatabaseName;
    DBCC CHECKDB (@DatabaseName) WITH NO_INFOMSGS;
    PRINT 'Check completed';
END;
GO

-- Check server version and patch level
SELECT 
    @@SERVERNAME as ServerName,
    @@VERSION as Version,
    SERVERPROPERTY('ProductLevel') as ServicePack,
    SERVERPROPERTY('Edition') as Edition;
GO

-- Find active user sessions
SELECT 
    session_id,
    login_name,
    host_name,
    program_name,
    status,
    last_request_start_time
FROM sys.dm_exec_sessions
WHERE is_user_process = 1
ORDER BY last_request_start_time DESC;
GO

-- INDEX MAINTENANCE

CREATE OR ALTER PROCEDURE usp_IndexMaintenance
    @DatabaseName NVARCHAR(128) = NULL,
    @RebuildThreshold FLOAT = 30,
    @ReorganizeThreshold FLOAT = 10,
    @PrintOnly BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @DBName NVARCHAR(128) = ISNULL(@DatabaseName, DB_NAME());
    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @FragPercent FLOAT;
    DECLARE @SQL NVARCHAR(MAX);
    
    DECLARE idx_cursor CURSOR FOR
        SELECT 
            OBJECT_NAME(ips.object_id),
            i.name,
            ips.avg_fragmentation_in_percent
        FROM sys.dm_db_index_physical_stats(DB_ID(@DBName), NULL, NULL, NULL, 'LIMITED') ips
        INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
        WHERE ips.avg_fragmentation_in_percent >= @ReorganizeThreshold
        AND ips.page_count > 1000
        AND i.index_id > 0;
    
    OPEN idx_cursor;
    FETCH NEXT FROM idx_cursor INTO @TableName, @IndexName, @FragPercent;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @FragPercent >= @RebuildThreshold
        BEGIN
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @DBName + '].[dbo].[' + @TableName + '] REBUILD';
            PRINT 'REBUILD: ' + @TableName + '.' + @IndexName + ' (' + CAST(@FragPercent AS VARCHAR) + '% fragmented)';
        END
        ELSE
        BEGIN
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @DBName + '].[dbo].[' + @TableName + '] REORGANIZE';
            PRINT 'REORGANIZE: ' + @TableName + '.' + @IndexName + ' (' + CAST(@FragPercent AS VARCHAR) + '% fragmented)';
        END;
        
        IF @PrintOnly = 0
        BEGIN
            EXEC sp_executesql @SQL;
            PRINT 'Completed';
        END;
        
        FETCH NEXT FROM idx_cursor INTO @TableName, @IndexName, @FragPercent;
    END;
    
    CLOSE idx_cursor;
    DEALLOCATE idx_cursor;
    
    PRINT 'Index maintenance finished';
END;
GO

-- CAPACITY PLANNING

-- Current database sizes
SELECT 
    DB_NAME(database_id) as DatabaseName,
    SUM(size * 8 / 1024) as SizeMB
FROM sys.master_files
GROUP BY database_id
ORDER BY SizeMB DESC;
GO

-- Table sizes
CREATE OR ALTER PROCEDURE usp_TableSize
    @DatabaseName NVARCHAR(128)
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX) = '
    USE [' + @DatabaseName + '];
    SELECT 
        t.name as TableName,
        p.rows as RowCount,
        SUM(a.total_pages) * 8 / 1024 as SizeMB
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    WHERE i.index_id IN (0,1)
    GROUP BY t.name, p.rows
    ORDER BY SizeMB DESC;';
    
    EXEC sp_executesql @SQL;
END;
GO

-- Disk space
CREATE TABLE #drives (Drive CHAR(1), FreeMB INT);
INSERT INTO #drives EXEC xp_fixeddrives;
SELECT Drive, FreeMB, FreeMB / 1024 as FreeGB FROM #drives;
DROP TABLE #drives;
GO

PRINT 'All scripts created successfully';
