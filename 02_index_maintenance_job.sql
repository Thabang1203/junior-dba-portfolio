USE master;
GO

-- Stored Procedure for Index Maintenance
CREATE OR ALTER PROCEDURE usp_IndexMaintenance
    @DatabaseName     NVARCHAR(128) = NULL,
    @MinPageCount     INT           = 1000,
    @ReorgThreshold   FLOAT         = 10.0,
    @RebuildThreshold FLOAT         = 30.0,
    @OnlineRebuild    BIT           = 1,
    @PrintOnly        BIT           = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DbID INT = ISNULL(DB_ID(@DatabaseName), DB_ID());
    DECLARE @DbName NVARCHAR(128) = ISNULL(@DatabaseName, DB_NAME());
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @TableName NVARCHAR(256);
    DECLARE @IndexName NVARCHAR(256);
    DECLARE @Frag FLOAT;
    DECLARE @Action NVARCHAR(20);
    DECLARE @StartTime DATETIME2 = SYSDATETIME();

    IF OBJECT_ID('tempdb..#IndexLog') IS NULL
    BEGIN
        CREATE TABLE #IndexLog (
            LogID INT IDENTITY(1,1),
            LogTime DATETIME2 DEFAULT SYSDATETIME(),
            DatabaseName NVARCHAR(128),
            TableName NVARCHAR(256),
            IndexName NVARCHAR(256),
            Action NVARCHAR(20),
            FragBefore FLOAT,
            Duration_ms INT,
            Status NVARCHAR(20)
        );
    END;

    PRINT 'Index Maintenance Job Started: ' + CONVERT(VARCHAR, @StartTime, 120);
    PRINT 'Database: ' + @DbName;

    DECLARE idx_cursor CURSOR FOR
        SELECT
            OBJECT_NAME(ips.object_id, @DbID),
            i.name,
            ROUND(ips.avg_fragmentation_in_percent, 2)
        FROM sys.dm_db_index_physical_stats(@DbID, NULL, NULL, NULL, 'LIMITED') ips
        INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
        WHERE ips.avg_fragmentation_in_percent >= @ReorgThreshold
          AND ips.page_count >= @MinPageCount
          AND i.index_id > 0
        ORDER BY ips.avg_fragmentation_in_percent DESC;

    OPEN idx_cursor;
    FETCH NEXT FROM idx_cursor INTO @TableName, @IndexName, @Frag;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @OpStart DATETIME2 = SYSDATETIME();

        SET @Action = CASE WHEN @Frag >= @RebuildThreshold THEN 'REBUILD' ELSE 'REORGANIZE' END;

        IF @Action = 'REBUILD'
        BEGIN
            SET @SQL = 'USE [' + @DbName + ']; ALTER INDEX [' + @IndexName + '] ON [dbo].[' + @TableName + '] REBUILD WITH (ONLINE = ' + CASE WHEN @OnlineRebuild = 1 THEN 'ON' ELSE 'OFF' END + ', SORT_IN_TEMPDB = ON);';
        END
        ELSE
        BEGIN
            SET @SQL = 'USE [' + @DbName + ']; ALTER INDEX [' + @IndexName + '] ON [dbo].[' + @TableName + '] REORGANIZE;';
        END

        IF @PrintOnly = 1
        BEGIN
            PRINT @SQL;
        END
        ELSE
        BEGIN
            BEGIN TRY
                EXEC sp_executesql @SQL;
                PRINT 'Success: ' + @Action + ' ' + @DbName + '.' + @TableName + '.' + @IndexName + ' (' + CAST(@Frag AS VARCHAR) + '% fragmented)';

                INSERT INTO #IndexLog (DatabaseName, TableName, IndexName, Action, FragBefore, Duration_ms, Status)
                VALUES (@DbName, @TableName, @IndexName, @Action, @Frag, DATEDIFF(MILLISECOND, @OpStart, SYSDATETIME()), 'SUCCESS');
            END TRY
            BEGIN CATCH
                PRINT 'Error: ' + @Action + ' ' + @DbName + '.' + @TableName + '.' + @IndexName + ' | ' + ERROR_MESSAGE();
                INSERT INTO #IndexLog (DatabaseName, TableName, IndexName, Action, FragBefore, Duration_ms, Status)
                VALUES (@DbName, @TableName, @IndexName, @Action, @Frag, DATEDIFF(MILLISECOND, @OpStart, SYSDATETIME()), 'FAILED');
            END CATCH;
        END;

        FETCH NEXT FROM idx_cursor INTO @TableName, @IndexName, @Frag;
    END;

    CLOSE idx_cursor;
    DEALLOCATE idx_cursor;

    SELECT * FROM #IndexLog ORDER BY LogTime;

    PRINT 'Maintenance Complete. Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, SYSDATETIME()) AS VARCHAR) + ' seconds';
END;
GO

-- Usage examples
-- EXEC usp_IndexMaintenance @DatabaseName = 'AdventureWorks2019', @PrintOnly = 1;
-- EXEC usp_IndexMaintenance @DatabaseName = 'AdventureWorks2019', @PrintOnly = 0;
```
