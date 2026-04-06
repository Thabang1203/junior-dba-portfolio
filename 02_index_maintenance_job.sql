-- ============================================================
-- Script: 02_index_maintenance_job.sql
-- Author: Norman Mathe | Junior DBA Portfolio
-- Purpose: Automated index rebuild/reorganize based on
--          fragmentation thresholds. Safe for production use.
-- Environment: SQL Server 2016+ 
-- ============================================================

USE master;
GO

-- -------------------------------------------------------
-- Stored Procedure: usp_IndexMaintenance
-- Parameters:
--   @DatabaseName  - Target database (default: current)
--   @MinPageCount  - Minimum pages to consider (default: 1000)
--   @ReorgThreshold - Fragmentation % to REORGANIZE (default: 10)
--   @RebuildThreshold - Fragmentation % to REBUILD (default: 30)
--   @OnlineRebuild - Use ONLINE=ON if edition supports it (default: 1)
--   @PrintOnly     - 1 = print scripts only, 0 = execute (default: 0)
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_IndexMaintenance
    @DatabaseName     NVARCHAR(128) = NULL,
    @MinPageCount     INT           = 1000,
    @ReorgThreshold   FLOAT         = 10.0,
    @RebuildThreshold FLOAT         = 30.0,
    @OnlineRebuild    BIT           = 1,
    @PrintOnly        BIT           = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DbID       INT          = ISNULL(DB_ID(@DatabaseName), DB_ID());
    DECLARE @DbName     NVARCHAR(128)= ISNULL(@DatabaseName, DB_NAME());
    DECLARE @SQL        NVARCHAR(MAX);
    DECLARE @TableName  NVARCHAR(256);
    DECLARE @IndexName  NVARCHAR(256);
    DECLARE @Frag       FLOAT;
    DECLARE @Action     NVARCHAR(20);
    DECLARE @StartTime  DATETIME2 = SYSDATETIME();
    DECLARE @LogMsg     NVARCHAR(500);

    -- Log table (create if not exists)
    IF OBJECT_ID('tempdb..#IndexLog') IS NULL
    BEGIN
        CREATE TABLE #IndexLog (
            LogID       INT IDENTITY(1,1),
            LogTime     DATETIME2       DEFAULT SYSDATETIME(),
            [Database]  NVARCHAR(128),
            [Table]     NVARCHAR(256),
            [Index]     NVARCHAR(256),
            Action      NVARCHAR(20),
            FragBefore  FLOAT,
            Duration_ms INT,
            [Status]    NVARCHAR(20)
        );
    END;

    PRINT '========================================================';
    PRINT 'Index Maintenance Job Started: ' + CONVERT(VARCHAR, @StartTime, 120);
    PRINT 'Database: ' + @DbName;
    PRINT '========================================================';

    -- Cursor over fragmented indexes
    DECLARE idx_cursor CURSOR FOR
        SELECT
            OBJECT_NAME(ips.object_id, @DbID),
            i.name,
            ROUND(ips.avg_fragmentation_in_percent, 2)
        FROM sys.dm_db_index_physical_stats(@DbID, NULL, NULL, NULL, 'LIMITED') AS ips
        INNER JOIN sys.indexes AS i
            ON ips.object_id = i.object_id
            AND ips.index_id = i.index_id
        WHERE ips.avg_fragmentation_in_percent >= @ReorgThreshold
          AND ips.page_count >= @MinPageCount
          AND i.index_id > 0
        ORDER BY ips.avg_fragmentation_in_percent DESC;

    OPEN idx_cursor;
    FETCH NEXT FROM idx_cursor INTO @TableName, @IndexName, @Frag;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @OpStart DATETIME2 = SYSDATETIME();

        SET @Action = CASE 
            WHEN @Frag >= @RebuildThreshold THEN 'REBUILD'
            ELSE 'REORGANIZE'
        END;

        SET @SQL = CASE @Action
            WHEN 'REBUILD' THEN
                'USE [' + @DbName + ']; ALTER INDEX [' + @IndexName + '] ON [dbo].[' 
                + @TableName + '] REBUILD WITH (ONLINE = ' 
                + CASE WHEN @OnlineRebuild = 1 THEN 'ON' ELSE 'OFF' END + ', SORT_IN_TEMPDB = ON);'
            ELSE
                'USE [' + @DbName + ']; ALTER INDEX [' + @IndexName + '] ON [dbo].[' 
                + @TableName + '] REORGANIZE;'
        END;

        SET @LogMsg = @Action + ' | ' + @DbName + '.' + @TableName + '.' 
                      + @IndexName + ' (' + CAST(@Frag AS VARCHAR) + '% frag)';

        IF @PrintOnly = 1
        BEGIN
            PRINT @SQL;
        END
        ELSE
        BEGIN
            BEGIN TRY
                EXEC sp_executesql @SQL;
                PRINT 'SUCCESS: ' + @LogMsg;

                INSERT INTO #IndexLog ([Database],[Table],[Index],Action,FragBefore,Duration_ms,[Status])
                VALUES (@DbName, @TableName, @IndexName, @Action, @Frag,
                        DATEDIFF(MILLISECOND, @OpStart, SYSDATETIME()), 'SUCCESS');
            END TRY
            BEGIN CATCH
                PRINT 'ERROR: ' + @LogMsg + ' | ' + ERROR_MESSAGE();
                INSERT INTO #IndexLog ([Database],[Table],[Index],Action,FragBefore,Duration_ms,[Status])
                VALUES (@DbName, @TableName, @IndexName, @Action, @Frag,
                        DATEDIFF(MILLISECOND, @OpStart, SYSDATETIME()), 'FAILED');
            END CATCH;
        END;

        FETCH NEXT FROM idx_cursor INTO @TableName, @IndexName, @Frag;
    END;

    CLOSE idx_cursor;
    DEALLOCATE idx_cursor;

    -- Summary
    SELECT * FROM #IndexLog ORDER BY LogTime;

    PRINT '========================================================';
    PRINT 'Maintenance Complete. Duration: ' 
          + CAST(DATEDIFF(SECOND, @StartTime, SYSDATETIME()) AS VARCHAR) + 's';
    PRINT '========================================================';
END;
GO

-- -------------------------------------------------------
-- Usage Examples
-- -------------------------------------------------------
-- Print-only mode (safe to run in prod to preview):
-- EXEC dbo.usp_IndexMaintenance @DatabaseName = 'AdventureWorks2019', @PrintOnly = 1;

-- Full execution:
-- EXEC dbo.usp_IndexMaintenance @DatabaseName = 'AdventureWorks2019', @PrintOnly = 0;

-- SQL Server Agent Job (weekly Saturday 02:00 AM):
/*
USE msdb;
EXEC sp_add_job @job_name = N'DBA - Weekly Index Maintenance';
EXEC sp_add_jobstep 
    @job_name  = N'DBA - Weekly Index Maintenance',
    @step_name = N'Rebuild & Reorganize Indexes',
    @command   = N'EXEC master.dbo.usp_IndexMaintenance @DatabaseName = NULL, @PrintOnly = 0;';
EXEC sp_add_schedule 
    @schedule_name = N'WeeklySaturday_2AM',
    @freq_type = 8, @freq_interval = 64,
    @active_start_time = 020000;
EXEC sp_attach_schedule 
    @job_name = N'DBA - Weekly Index Maintenance',
    @schedule_name = N'WeeklySaturday_2AM';
EXEC sp_add_jobserver @job_name = N'DBA - Weekly Index Maintenance';
*/
GO
