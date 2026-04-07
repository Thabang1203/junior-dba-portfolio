USE master;
GO

-- 1. Full Database Backup with Verification
CREATE OR ALTER PROCEDURE usp_FullBackup
    @DatabaseName   NVARCHAR(128),
    @BackupPath     NVARCHAR(512) = 'C:\Backups\Full\',
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

    SET @FileName   = @BackupPath + @DatabaseName + '_FULL_' + @Timestamp + '.bak';
    SET @BackupName = @DatabaseName + ' Full Backup ' + @Timestamp;

    PRINT 'Full backup: ' + @DatabaseName;
    PRINT 'Target: ' + @FileName;

    SET @SQL = 'BACKUP DATABASE [' + @DatabaseName + ']
    TO DISK = ''' + @FileName + '''
    WITH 
        NAME = ''' + @BackupName + ''',
        COMPRESSION' + CASE WHEN @Compression = 1 THEN '' ELSE ' = OFF' END + ',
        CHECKSUM,
        STATS = 10;';

    BEGIN TRY
        EXEC sp_executesql @SQL;
        PRINT 'Backup completed in ' + CAST(DATEDIFF(SECOND, @StartTime, SYSDATETIME()) AS VARCHAR) + ' seconds';

        IF @Verify = 1
        BEGIN
            PRINT 'Verifying backup...';
            RESTORE VERIFYONLY FROM DISK = @FileName WITH CHECKSUM;
            PRINT 'Verification passed';
        END;

    END TRY
    BEGIN CATCH
        PRINT 'Error: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;
END;
GO

-- 2. Differential Backup
CREATE OR ALTER PROCEDURE usp_DifferentialBackup
    @DatabaseName   NVARCHAR(128),
    @BackupPath     NVARCHAR(512) = 'C:\Backups\Diff\'
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

    PRINT 'Differential backup: ' + @DatabaseName;
    EXEC sp_executesql @SQL;
    PRINT 'Differential backup complete: ' + @FileName;
END;
GO

-- 3. Transaction Log Backup
CREATE OR ALTER PROCEDURE usp_LogBackup
    @DatabaseName   NVARCHAR(128),
    @BackupPath     NVARCHAR(512) = 'C:\Backups\Log\'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @FileName  NVARCHAR(512);
    DECLARE @Timestamp VARCHAR(20) = FORMAT(GETDATE(), 'yyyyMMdd_HHmm');

    SET @FileName = @BackupPath + @DatabaseName + '_LOG_' + @Timestamp + '.trn';

    IF (SELECT recovery_model_desc FROM sys.databases WHERE name = @DatabaseName) = 'SIMPLE'
    BEGIN
        PRINT 'Warning: ' + @DatabaseName + ' is in SIMPLE recovery model. Log backup skipped.';
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

-- 4. Backup Health Check
SELECT
    bs.database_name as DatabaseName,
    MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END) as LastFullBackup,
    MAX(CASE WHEN bs.type = 'I' THEN bs.backup_finish_date END) as LastDiffBackup,
    MAX(CASE WHEN bs.type = 'L' THEN bs.backup_finish_date END) as LastLogBackup,
    DATEDIFF(HOUR, MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END), GETDATE()) as HoursSinceFullBackup,
    CASE 
        WHEN MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END) IS NULL THEN 'Never backed up'
        WHEN DATEDIFF(HOUR, MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END), GETDATE()) > 25 THEN 'Overdue'
        ELSE 'OK'
    END as BackupStatus
FROM msdb.dbo.backupset bs
WHERE bs.backup_finish_date > DATEADD(DAY, -30, GETDATE())
GROUP BY bs.database_name
ORDER BY HoursSinceFullBackup DESC;
GO
