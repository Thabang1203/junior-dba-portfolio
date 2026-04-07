USE master;
GO

-- 1. Current Database Size & File Usage
SELECT
    DB_NAME(database_id) as DatabaseName,
    SUM(CASE WHEN type = 0 THEN size * 8.0 / 1024 ELSE 0 END) as DataFileSizeMB,
    SUM(CASE WHEN type = 1 THEN size * 8.0 / 1024 ELSE 0 END) as LogFileSizeMB,
    SUM(size * 8.0 / 1024) as TotalSizeMB,
    SUM(CASE WHEN type = 0 THEN (size - FILEPROPERTY(name, 'SpaceUsed')) * 8.0 / 1024 ELSE 0 END) as DataFreeSpaceMB,
    SUM(size * 8.0 / 1024 / 1024) as TotalSizeGB
FROM sys.master_files
GROUP BY database_id
ORDER BY TotalSizeMB DESC;
GO

-- 2. Table-Level Size & Row Count
CREATE OR ALTER PROCEDURE usp_TableSizeReport
    @DatabaseName NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @DB NVARCHAR(128) = ISNULL(@DatabaseName, DB_NAME());
    DECLARE @SQL NVARCHAR(MAX);

    SET @SQL = '
    USE [' + @DB + '];
    SELECT
        s.name + ''.'' + t.name as TableName,
        p.rows as RowCount,
        ROUND(SUM(a.total_pages) * 8.0 / 1024, 2) as TotalSizeMB,
        ROUND(SUM(a.used_pages) * 8.0 / 1024, 2) as UsedSizeMB,
        ROUND((SUM(a.total_pages) - SUM(a.used_pages)) * 8.0 / 1024, 2) as UnusedMB
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    WHERE i.index_id IN (0, 1)
    GROUP BY s.name, t.name, p.rows
    ORDER BY TotalSizeMB DESC;';

    EXEC sp_executesql @SQL;
END;
GO

-- 3. Database Growth History from backup history
SELECT
    bs.database_name as DatabaseName,
    CAST(bs.backup_start_date AS DATE) as BackupDate,
    ROUND(MAX(bs.backup_size) / 1073741824.0, 2) as BackupSizeGB,
    bs.recovery_model as RecoveryModel
FROM msdb.dbo.backupset bs
WHERE bs.type = 'D'
  AND bs.backup_start_date > DATEADD(MONTH, -6, GETDATE())
GROUP BY bs.database_name, CAST(bs.backup_start_date AS DATE), bs.recovery_model
ORDER BY bs.database_name, BackupDate;
GO

-- 4. Disk Space Monitoring
CREATE TABLE #DriveSpace (Drive CHAR(1), FreeSpaceMB INT);
INSERT INTO #DriveSpace EXEC master..xp_fixeddrives;

SELECT
    Drive as DriveLetter,
    FreeSpaceMB as FreeMB,
    ROUND(FreeSpaceMB / 1024.0, 1) as FreeGB,
    CASE
        WHEN FreeSpaceMB < 5120 THEN 'CRITICAL (less than 5GB)'
        WHEN FreeSpaceMB < 20480 THEN 'WARNING (less than 20GB)'
        ELSE 'OK'
    END as Status
FROM #DriveSpace
ORDER BY FreeSpaceMB ASC;

DROP TABLE #DriveSpace;
GO

-- 5. Auto-Growth Settings
SELECT
    DB_NAME(vfs.database_id) as DatabaseName,
    mf.name as FileName,
    mf.type_desc as FileType,
    mf.physical_name as FilePath,
    ROUND(mf.size * 8.0 / 1024, 1) as CurrentSizeMB,
    mf.growth as GrowthSetting,
    CASE mf.is_percent_growth
        WHEN 1 THEN CAST(mf.growth AS VARCHAR) + '%'
        ELSE CAST(mf.growth * 8 / 1024 AS VARCHAR) + 'MB'
    END as AutoGrowthAmount,
    CASE WHEN mf.max_size = -1 THEN 'Unlimited'
         ELSE CAST(ROUND(mf.max_size * 8.0 / 1024 / 1024, 1) AS VARCHAR) + 'GB'
    END as MaxSize
FROM sys.master_files mf
LEFT JOIN sys.dm_io_virtual_file_stats(NULL, NULL) vfs
    ON mf.database_id = vfs.database_id
    AND mf.file_id = vfs.file_id
WHERE mf.database_id > 4
ORDER BY DB_NAME(mf.database_id), mf.type;
GO

-- 6. Capacity Forecast Stored Procedure
CREATE OR ALTER PROCEDURE usp_CapacityForecast
    @DatabaseName     NVARCHAR(128),
    @ForecastMonths   INT = 6
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        bs.database_name as DatabaseName,
        CAST(bs.backup_start_date AS DATE) as BackupDate,
        ROUND(bs.backup_size / 1073741824.0, 2) as SizeGB
    INTO #GrowthData
    FROM msdb.dbo.backupset bs
    WHERE bs.database_name = @DatabaseName
      AND bs.type = 'D'
      AND bs.backup_start_date > DATEADD(DAY, -90, GETDATE())
    ORDER BY bs.backup_start_date;

    DECLARE @MinSize FLOAT, @MaxSize FLOAT, @Days INT;
    SELECT 
        @MinSize = MIN(SizeGB), 
        @MaxSize = MAX(SizeGB),
        @Days = DATEDIFF(DAY, MIN(BackupDate), MAX(BackupDate))
    FROM #GrowthData;

    DECLARE @DailyGrowthGB FLOAT = 
        CASE WHEN @Days > 0 THEN (@MaxSize - @MinSize) / @Days ELSE 0 END;

    DECLARE @MonthlyGrowthGB FLOAT = @DailyGrowthGB * 30;

    DECLARE @Counter INT = 1;
    CREATE TABLE #Forecast (Month INT, ProjectedSizeGB FLOAT);

    WHILE @Counter <= @ForecastMonths
    BEGIN
        INSERT INTO #Forecast VALUES (@Counter, @MaxSize + (@MonthlyGrowthGB * @Counter));
        SET @Counter = @Counter + 1;
    END;

    SELECT
        @DatabaseName as DatabaseName,
        @MaxSize as CurrentSizeGB,
        ROUND(@DailyGrowthGB, 4) as AvgDailyGrowthGB,
        ROUND(@MonthlyGrowthGB, 2) as AvgMonthlyGrowthGB,
        f.Month as MonthsFromNow,
        ROUND(f.ProjectedSizeGB, 2) as ProjectedSizeGB,
        DATENAME(MONTH, DATEADD(MONTH, f.Month, GETDATE())) + ' ' + CAST(YEAR(DATEADD(MONTH, f.Month, GETDATE())) AS VARCHAR) as ForecastMonth
    FROM #Forecast f
    ORDER BY f.Month;

    DROP TABLE #GrowthData;
    DROP TABLE #Forecast;
END;
GO
