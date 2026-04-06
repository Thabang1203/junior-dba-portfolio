-- ============================================================
-- Script: 01_backup_strategy.sql
-- Author: Norman Mathe | Junior DBA Portfolio
-- Purpose: Full/Differential/Log backup strategy with
--          verification, compression, and history logging
-- Environment: SQL Server 2016+ 
-- ============================================================

USE master;
GO

-- -------------------------------------------------------
-- 1. Full Database Backup with Verification
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_FullBackup
    @DatabaseName   NVARCHAR(128),
    @BackupPath     NVARCHAR(512) = 'C:\DBA_Backups\Full\',
    @Compression    BIT = 1,
    @Verify         BIT = 1,
    @RetentionDays  INT = 14
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FileName    NVARCHAR(512);
    DECLARE @BackupName  NVARCHAR(256);
    DECLARE @SQL         NVARCHAR(MAX);
    DECLARE @Timestamp   VARCHAR(20) = FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
    DECLARE @StartTime   DATETIME2   = SYSDATETIME();

    -- Build file name
    SET @FileName   = @BackupPath + @DatabaseName + '_FULL_' + @Timestamp + '.bak';
    SET @BackupName = @DatabaseName + ' Full Backup ' + @Timestamp;

    PRINT '=== FULL BACKUP: ' + @DatabaseName + ' ===';
    PRINT 'Target: ' + @FileName;

    -- Perform backup
    SET @SQL = 'BACKUP DATABASE [' + @DatabaseName + ']
    TO DISK = ''' + @FileName + '''
    WITH 
        NAME = ''' + @BackupName + ''',
        DESCRIPTION = ''Full backup by usp_FullBackup'',
        COMPRESSION' + CASE WHEN @Compression = 1 THEN '' ELSE ' = OFF' END + ',
        CHECKSUM,
        STATS = 10;';

    BEGIN TRY
        EXEC sp_executesql @SQL;
        PRINT 'Backup completed in ' 
              + CAST(DATEDIFF(SECOND, @StartTime, SYSDATETIME()) AS VARCHAR) + 's';

        -- Verify backup integrity
        IF @Verify = 1
        BEGIN
            PRINT 'Verifying backup...';
            RESTORE VERIFYONLY FROM DISK = @FileName WITH CHECKSUM;
            PRINT 'Verification PASSED.';
        END;

        -- Log to backup history table
        IF OBJECT_ID('msdb..dba_BackupHistory') IS NOT NULL
            INSERT INTO msdb.dbo.dba_BackupHistory 
                ([Database], BackupType, FileName, BackupSize_MB, Duration_s, BackupTime, [Status])
            SELECT 
                @DatabaseName, 'FULL', @FileName,
                backup_size / 1048576,
                DATEDIFF(SECOND, @StartTime, SYSDATETIME()),
                @StartTime, 'SUCCESS'
            FROM msdb.dbo.backupset
            WHERE database_name = @DatabaseName
            ORDER BY backup_finish_date DESC
            OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

    END TRY
    BEGIN CATCH
        PRINT 'ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;
END;
GO

-- -------------------------------------------------------
-- 2. Differential Backup
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_DifferentialBackup
    @DatabaseName   NVARCHAR(128),
    @BackupPath     NVARCHAR(512) = 'C:\DBA_Backups\Diff\'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @FileName  NVARCHAR(512);
    DECLARE @Timestamp VARCHAR(20) = FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');

    SET @FileName = @BackupPath + @DatabaseName + '_DIFF_' + @Timestamp + '.bak';

    DECLARE @SQL NVARCHAR(MAX) = 
        'BACKUP DATABASE [' + @DatabaseName + ']
         TO DISK = ''' + @FileName + '''
         WITH DIFFERENTIAL, COMPRESSION, CHECKSUM, STATS = 10;';

    PRINT '=== DIFFERENTIAL BACKUP: ' + @DatabaseName + ' ===';
    EXEC sp_executesql @SQL;
    PRINT 'Differential backup complete: ' + @FileName;
END;
GO

-- -------------------------------------------------------
-- 3. Transaction Log Backup
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_LogBackup
    @DatabaseName   NVARCHAR(128),
    @BackupPath     NVARCHAR(512) = 'C:\DBA_Backups\Log\'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @FileName  NVARCHAR(512);
    DECLARE @Timestamp VARCHAR(20) = FORMAT(GETDATE(), 'yyyyMMdd_HHmm');

    SET @FileName = @BackupPath + @DatabaseName + '_LOG_' + @Timestamp + '.trn';

    -- Verify database is in FULL or BULK_LOGGED recovery model
    IF (SELECT recovery_model_desc FROM sys.databases WHERE name = @DatabaseName) = 'SIMPLE'
    BEGIN
        PRINT 'WARNING: ' + @DatabaseName + ' is in SIMPLE recovery model. Log backup skipped.';
        RETURN;
    END;

    DECLARE @SQL NVARCHAR(MAX) = 
        'BACKUP LOG [' + @DatabaseName + ']
         TO DISK = ''' + @FileName + '''
         WITH COMPRESSION, CHECKSUM, STATS = 10;';

    EXEC sp_executesql @SQL;
    PRINT 'Log backup complete: ' + @FileName;
END;
GO

-- -------------------------------------------------------
-- 4. Point-In-Time Recovery Script
-- -------------------------------------------------------
/*
SCENARIO: Restore AdventureWorks to a specific point in time
after accidental data deletion at 14:32:00

STEP 1 - Restore Full Backup (NORECOVERY)
RESTORE DATABASE [AdventureWorks_Restored]
FROM DISK = 'C:\DBA_Backups\Full\AdventureWorks_FULL_20240115_020000.bak'
WITH 
    MOVE 'AdventureWorks2019'     TO 'C:\Data\AdventureWorks_Restored.mdf',
    MOVE 'AdventureWorks2019_log' TO 'C:\Logs\AdventureWorks_Restored_log.ldf',
    NORECOVERY, REPLACE, STATS = 10;

STEP 2 - Restore Differential Backup (NORECOVERY)
RESTORE DATABASE [AdventureWorks_Restored]
FROM DISK = 'C:\DBA_Backups\Diff\AdventureWorks_DIFF_20240115_120000.bak'
WITH NORECOVERY, STATS = 10;

STEP 3 - Restore Transaction Logs up to the point in time
RESTORE LOG [AdventureWorks_Restored]
FROM DISK = 'C:\DBA_Backups\Log\AdventureWorks_LOG_20240115_1400.trn'
WITH NORECOVERY;

RESTORE LOG [AdventureWorks_Restored]
FROM DISK = 'C:\DBA_Backups\Log\AdventureWorks_LOG_20240115_1430.trn'
WITH 
    STOPAT = '2024-01-15 14:31:59',
    RECOVERY;  -- Final restore brings DB online

STEP 4 - Verify
SELECT TOP 10 * FROM [AdventureWorks_Restored].[HumanResources].[Employee];
*/

-- -------------------------------------------------------
-- 5. SQL Agent Schedule (3-2-1 Backup Strategy)
-- Full: Daily @ 02:00 | Diff: Every 6h | Log: Every 15min
-- -------------------------------------------------------
/*
-- Full Backup Job
EXEC msdb.dbo.sp_add_job @job_name = N'DBA - Full Backup (Daily)';
EXEC msdb.dbo.sp_add_jobstep @job_name = N'DBA - Full Backup (Daily)',
    @step_name = N'Full Backup All DBs',
    @command = N'EXEC master.dbo.usp_FullBackup @DatabaseName = ''AdventureWorks2019'';';
EXEC msdb.dbo.sp_add_schedule @schedule_name = N'Daily_2AM',
    @freq_type = 4, @freq_interval = 1, @active_start_time = 020000;

-- Log Backup Job
EXEC msdb.dbo.sp_add_job @job_name = N'DBA - Log Backup (15min)';
EXEC msdb.dbo.sp_add_jobstep @job_name = N'DBA - Log Backup (15min)',
    @step_name = N'Log Backup',
    @command = N'EXEC master.dbo.usp_LogBackup @DatabaseName = ''AdventureWorks2019'';';
EXEC msdb.dbo.sp_add_schedule @schedule_name = N'Every15Min',
    @freq_type = 4, @freq_interval = 1,
    @freq_subday_type = 4, @freq_subday_interval = 15;
*/
GO

-- -------------------------------------------------------
-- 6. Backup Health Check
-- -------------------------------------------------------
SELECT
    bs.database_name                              AS [Database],
    MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END) AS [LastFullBackup],
    MAX(CASE WHEN bs.type = 'I' THEN bs.backup_finish_date END) AS [LastDiffBackup],
    MAX(CASE WHEN bs.type = 'L' THEN bs.backup_finish_date END) AS [LastLogBackup],
    DATEDIFF(HOUR, 
        MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END),
        GETDATE())                                AS [HoursSinceFullBackup],
    CASE 
        WHEN MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END) IS NULL 
            THEN '⚠ NEVER BACKED UP'
        WHEN DATEDIFF(HOUR, 
            MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END), GETDATE()) > 25
            THEN '⚠ OVERDUE'
        ELSE '✓ OK'
    END                                           AS [BackupStatus]
FROM msdb.dbo.backupset AS bs
WHERE bs.backup_finish_date > DATEADD(DAY, -30, GETDATE())
GROUP BY bs.database_name
ORDER BY [HoursSinceFullBackup] DESC;
GO
