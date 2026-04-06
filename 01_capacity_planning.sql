-- ============================================================
-- Script: 01_capacity_planning.sql
-- Author: Norman Mathe | Junior DBA Portfolio
-- Purpose: Database capacity forecasting, growth tracking,
--          storage analysis and scalability planning
-- Environment: SQL Server 2016+
-- ============================================================

USE master;
GO

-- -------------------------------------------------------
-- 1. Current Database Size & File Usage
-- -------------------------------------------------------
SELECT
    DB_NAME(database_id)                          AS [Database],
    SUM(CASE WHEN type = 0 
        THEN size * 8.0 / 1024 ELSE 0 END)        AS [DataFileSizeMB],
    SUM(CASE WHEN type = 1 
        THEN size * 8.0 / 1024 ELSE 0 END)        AS [LogFileSizeMB],
    SUM(size * 8.0 / 1024)                        AS [TotalSizeMB],
    SUM(CASE WHEN type = 0 
        THEN (size - FILEPROPERTY(name, 'SpaceUsed')) * 8.0 / 1024 
        ELSE 0 END)                               AS [DataFreeSpaceMB],
    SUM(size * 8.0 / 1024 / 1024)                 AS [TotalSizeGB]
FROM sys.master_files
GROUP BY database_id
ORDER BY TotalSizeMB DESC;
GO

-- -------------------------------------------------------
-- 2. Table-Level Size & Row Count
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_TableSizeReport
    @DatabaseName NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @DB NVARCHAR(128) = ISNULL(@DatabaseName, DB_NAME());
    DECLARE @SQL NVARCHAR(MAX);

    SET @SQL = '
    USE [' + @DB + '];
    SELECT
        s.name + ''.'' + t.name                   AS [Table],
        p.rows                                    AS [RowCount],
        ROUND(SUM(a.total_pages) * 8.0 / 1024, 2)AS [TotalSizeMB],
        ROUND(SUM(a.used_pages) * 8.0 / 1024, 2) AS [UsedSizeMB],
        ROUND((SUM(a.total_pages) - SUM(a.used_pages)) * 8.0 / 1024, 2) AS [UnusedMB]
    FROM sys.tables AS t
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    INNER JOIN sys.indexes AS i ON t.object_id = i.object_id
    INNER JOIN sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units AS a ON p.partition_id = a.container_id
    WHERE i.index_id IN (0, 1)
    GROUP BY s.name, t.name, p.rows
    ORDER BY TotalSizeMB DESC;';

    EXEC sp_executesql @SQL;
END;
GO

-- -------------------------------------------------------
-- 3. Database Growth History (from msdb backup history)
-- -------------------------------------------------------
SELECT
    bs.database_name                              AS [Database],
    CAST(bs.backup_start_date AS DATE)            AS [BackupDate],
    ROUND(MAX(bs.backup_size) / 1073741824.0, 2)  AS [BackupSizeGB],
    bs.recovery_model                             AS [RecoveryModel]
FROM msdb.dbo.backupset AS bs
WHERE bs.type = 'D'                               -- Full backups only
  AND bs.backup_start_date > DATEADD(MONTH, -6, GETDATE())
GROUP BY bs.database_name, CAST(bs.backup_start_date AS DATE), bs.recovery_model
ORDER BY bs.database_name, BackupDate;
GO

-- -------------------------------------------------------
-- 4. Disk Space Monitoring (using xp_fixeddrives)
-- -------------------------------------------------------
CREATE TABLE #DriveSpace (Drive CHAR(1), FreeSpaceMB INT);
INSERT INTO #DriveSpace EXEC master..xp_fixeddrives;

SELECT
    Drive                                         AS [DriveLetter],
    FreeSpaceMB                                   AS [FreeMB],
    ROUND(FreeSpaceMB / 1024.0, 1)               AS [FreeGB],
    CASE
        WHEN FreeSpaceMB < 5120   THEN '🔴 CRITICAL (<5GB)'
        WHEN FreeSpaceMB < 20480  THEN '🟡 WARNING (<20GB)'
        ELSE                           '🟢 OK'
    END                                           AS [Status]
FROM #DriveSpace
ORDER BY FreeSpaceMB ASC;

DROP TABLE #DriveSpace;
GO

-- -------------------------------------------------------
-- 5. Auto-Growth Events Tracking (VLF & Growth Log)
-- -------------------------------------------------------
SELECT
    DB_NAME(vfs.database_id)                      AS [Database],
    mf.name                                       AS [FileName],
    mf.type_desc                                  AS [FileType],
    mf.physical_name                              AS [FilePath],
    ROUND(mf.size * 8.0 / 1024, 1)               AS [CurrentSizeMB],
    mf.growth                                     AS [GrowthSetting],
    CASE mf.is_percent_growth
        WHEN 1 THEN CAST(mf.growth AS VARCHAR) + '%'
        ELSE CAST(mf.growth * 8 / 1024 AS VARCHAR) + 'MB'
    END                                           AS [AutoGrowthAmount],
    CASE WHEN mf.max_size = -1 THEN 'Unlimited'
         ELSE CAST(ROUND(mf.max_size * 8.0 / 1024 / 1024, 1) AS VARCHAR) + 'GB'
    END                                           AS [MaxSize]
FROM sys.master_files AS mf
LEFT JOIN sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
    ON mf.database_id = vfs.database_id
    AND mf.file_id = vfs.file_id
WHERE mf.database_id > 4
ORDER BY DB_NAME(mf.database_id), mf.type;
GO

-- -------------------------------------------------------
-- 6. Capacity Forecast Stored Procedure
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_CapacityForecast
    @DatabaseName     NVARCHAR(128),
    @ForecastMonths   INT = 6
AS
BEGIN
    SET NOCOUNT ON;

    -- Get backup size trend (last 90 days)
    SELECT
        bs.database_name                          AS [Database],
        CAST(bs.backup_start_date AS DATE)        AS [Date],
        ROUND(bs.backup_size / 1073741824.0, 2)   AS [SizeGB]
    INTO #GrowthData
    FROM msdb.dbo.backupset AS bs
    WHERE bs.database_name = @DatabaseName
      AND bs.type = 'D'
      AND bs.backup_start_date > DATEADD(DAY, -90, GETDATE())
    ORDER BY bs.backup_start_date;

    -- Compute average daily growth
    DECLARE @MinSize FLOAT, @MaxSize FLOAT, @Days INT;
    SELECT 
        @MinSize = MIN(SizeGB), 
        @MaxSize = MAX(SizeGB),
        @Days    = DATEDIFF(DAY, MIN([Date]), MAX([Date]))
    FROM #GrowthData;

    DECLARE @DailyGrowthGB FLOAT = 
        CASE WHEN @Days > 0 THEN (@MaxSize - @MinSize) / @Days ELSE 0 END;

    DECLARE @MonthlyGrowthGB FLOAT = @DailyGrowthGB * 30;

    -- Project forward
    DECLARE @Counter INT = 1;
    CREATE TABLE #Forecast (Month INT, ProjectedSizeGB FLOAT);

    WHILE @Counter <= @ForecastMonths
    BEGIN
        INSERT INTO #Forecast VALUES (
            @Counter,
            @MaxSize + (@MonthlyGrowthGB * @Counter)
        );
        SET @Counter = @Counter + 1;
    END;

    SELECT
        @DatabaseName                             AS [Database],
        @MaxSize                                  AS [CurrentSizeGB],
        ROUND(@DailyGrowthGB, 4)                  AS [AvgDailyGrowthGB],
        ROUND(@MonthlyGrowthGB, 2)                AS [AvgMonthlyGrowthGB],
        f.Month                                   AS [MonthsFromNow],
        ROUND(f.ProjectedSizeGB, 2)               AS [ProjectedSizeGB],
        DATENAME(MONTH, DATEADD(MONTH, f.Month, GETDATE())) 
            + ' ' + CAST(YEAR(DATEADD(MONTH, f.Month, GETDATE())) AS VARCHAR) AS [ForecastMonth]
    FROM #Forecast AS f
    ORDER BY f.Month;

    DROP TABLE #GrowthData;
    DROP TABLE #Forecast;
END;
GO
