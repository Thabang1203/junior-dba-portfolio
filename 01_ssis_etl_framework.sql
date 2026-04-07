USE master;
GO

-- 1. ETL CONTROL DATABASE SETUP

-- Create ETL control database
IF DB_ID('ETL_Control') IS NULL
    CREATE DATABASE ETL_Control;
GO

USE ETL_Control;
GO

-- ETL Job Metadata Table
CREATE TABLE IF NOT EXISTS dbo.ETL_Jobs (
    JobID           INT IDENTITY(1,1) PRIMARY KEY,
    JobName         NVARCHAR(256) NOT NULL,
    SourceSystem    NVARCHAR(128),
    TargetSystem    NVARCHAR(128),
    SourceObject    NVARCHAR(256),
    TargetObject    NVARCHAR(256),
    LoadType        NVARCHAR(20) CHECK (LoadType IN ('FULL','INCREMENTAL','CDC','DELTA')),
    IncrementalKey  NVARCHAR(128),
    IsActive        BIT DEFAULT 1,
    CreatedDate     DATETIME2 DEFAULT SYSDATETIME(),
    ModifiedDate    DATETIME2
);

-- ETL Execution Log
CREATE TABLE dbo.ETL_ExecutionLog (
    LogID           BIGINT IDENTITY(1,1) PRIMARY KEY,
    JobID           INT REFERENCES dbo.ETL_Jobs(JobID),
    JobName         NVARCHAR(256),
    RunDate         DATETIME2 DEFAULT SYSDATETIME(),
    StartTime       DATETIME2,
    EndTime         DATETIME2,
    Duration_s      AS DATEDIFF(SECOND, StartTime, EndTime) PERSISTED,
    RowsExtracted   BIGINT DEFAULT 0,
    RowsInserted    BIGINT DEFAULT 0,
    RowsUpdated     BIGINT DEFAULT 0,
    RowsDeleted     BIGINT DEFAULT 0,
    RowsRejected    BIGINT DEFAULT 0,
    WatermarkOld    NVARCHAR(128),
    WatermarkNew    NVARCHAR(128),
    [Status]        NVARCHAR(20) DEFAULT 'RUNNING',
    ErrorMessage    NVARCHAR(MAX),
    SSISPackage     NVARCHAR(256),
    ExecutedBy      NVARCHAR(128) DEFAULT SUSER_NAME()
);

-- Error Staging Table
CREATE TABLE dbo.ETL_ErrorLog (
    ErrorID         BIGINT IDENTITY(1,1) PRIMARY KEY,
    LogID           BIGINT REFERENCES dbo.ETL_ExecutionLog(LogID),
    JobName         NVARCHAR(256),
    ErrorTime       DATETIME2 DEFAULT SYSDATETIME(),
    ErrorNumber     INT,
    ErrorSeverity   INT,
    ErrorMessage    NVARCHAR(MAX),
    SourceRow       NVARCHAR(MAX),
    ColumnName      NVARCHAR(128),
    ColumnValue     NVARCHAR(500)
);
GO

-- 2. ETL CONTROL STORED PROCEDURES

-- Start ETL Run
CREATE OR ALTER PROCEDURE usp_ETL_StartRun
    @JobName    NVARCHAR(256),
    @LogID      BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.ETL_ExecutionLog (JobName, StartTime, [Status])
    VALUES (@JobName, SYSDATETIME(), 'RUNNING');

    SET @LogID = SCOPE_IDENTITY();
    PRINT 'ETL Run started. LogID: ' + CAST(@LogID AS VARCHAR);
END;
GO

-- End ETL Run (success)
CREATE OR ALTER PROCEDURE usp_ETL_EndRun
    @LogID          BIGINT,
    @RowsInserted   BIGINT = 0,
    @RowsUpdated    BIGINT = 0,
    @RowsDeleted    BIGINT = 0,
    @RowsRejected   BIGINT = 0,
    @WatermarkNew   NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.ETL_ExecutionLog
    SET EndTime = SYSDATETIME(),
        RowsInserted = @RowsInserted,
        RowsUpdated = @RowsUpdated,
        RowsDeleted = @RowsDeleted,
        RowsRejected = @RowsRejected,
        WatermarkNew = @WatermarkNew,
        [Status] = 'SUCCESS'
    WHERE LogID = @LogID;

    PRINT 'ETL completed. Inserted: ' + CAST(@RowsInserted AS VARCHAR) + ' Updated: ' + CAST(@RowsUpdated AS VARCHAR);
END;
GO

-- Log ETL Failure
CREATE OR ALTER PROCEDURE usp_ETL_LogError
    @LogID          BIGINT,
    @ErrorMessage   NVARCHAR(MAX),
    @SourceRow      NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.ETL_ExecutionLog
    SET EndTime = SYSDATETIME(), [Status] = 'FAILED', ErrorMessage = @ErrorMessage
    WHERE LogID = @LogID;

    INSERT INTO dbo.ETL_ErrorLog (LogID, ErrorMessage, SourceRow)
    VALUES (@LogID, @ErrorMessage, @SourceRow);
END;
GO

-- 3. INCREMENTAL LOAD WITH WATERMARK PATTERN
CREATE OR ALTER PROCEDURE usp_ETL_IncrementalLoad
    @SourceDB       NVARCHAR(128),
    @SourceTable    NVARCHAR(256),
    @TargetTable    NVARCHAR(256),
    @WatermarkCol   NVARCHAR(128),
    @LogID          BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LastWatermark NVARCHAR(128);
    DECLARE @NewWatermark NVARCHAR(128);
    DECLARE @RowsInserted BIGINT;
    DECLARE @SQL NVARCHAR(MAX);

    SELECT TOP 1 @LastWatermark = WatermarkNew
    FROM dbo.ETL_ExecutionLog
    WHERE JobName = @SourceTable + '_LOAD'
      AND [Status] = 'SUCCESS'
    ORDER BY EndTime DESC;

    SET @LastWatermark = ISNULL(@LastWatermark, '1900-01-01');

    PRINT 'Loading changes since: ' + @LastWatermark;

    SET @SQL = '
    MERGE INTO [' + @TargetTable + '] tgt
    USING (
        SELECT * FROM [' + @SourceDB + ']..' + @SourceTable + '
        WHERE [' + @WatermarkCol + '] > ''' + @LastWatermark + '''
    ) src ON tgt.[ID] = src.[ID]
    WHEN MATCHED THEN UPDATE SET tgt.[' + @WatermarkCol + '] = src.[' + @WatermarkCol + ']
    WHEN NOT MATCHED BY TARGET THEN INSERT SELECT src.*;';

    BEGIN TRY
        EXEC sp_executesql @SQL;
        SET @RowsInserted = @@ROWCOUNT;
        SET @NewWatermark = CAST(GETDATE() AS NVARCHAR(128));

        EXEC usp_ETL_EndRun 
            @LogID = @LogID,
            @RowsInserted = @RowsInserted,
            @WatermarkNew = @NewWatermark;
    END TRY
    BEGIN CATCH
        EXEC usp_ETL_LogError 
            @LogID = @LogID, 
            @ErrorMessage = ERROR_MESSAGE();
        THROW;
    END CATCH;
END;
GO

-- 4. SSIS CATALOG MONITORING QUERIES
USE SSISDB;
GO

-- Recent SSIS Package Executions
SELECT TOP 20
    e.execution_id as ExecutionID,
    e.folder_name as Folder,
    e.project_name as Project,
    e.package_name as Package,
    e.start_time as StartTime,
    e.end_time as EndTime,
    DATEDIFF(SECOND, e.start_time, e.end_time) as Duration_s,
    CASE e.status
        WHEN 1 THEN 'Running'
        WHEN 2 THEN 'Success'
        WHEN 3 THEN 'Cancelled'
        WHEN 4 THEN 'Failed'
        WHEN 5 THEN 'Pending'
        ELSE CAST(e.status AS VARCHAR)
    END as Status,
    e.executed_as_name as ExecutedBy
FROM catalog.executions e
ORDER BY e.start_time DESC;
GO

-- Failed SSIS Package Messages
SELECT TOP 50
    om.operation_id as ExecutionID,
    om.message_time as Time,
    om.package_name as Package,
    om.task_name as Task,
    om.message as ErrorMessage
FROM catalog.operation_messages om
WHERE om.message_type = 120
ORDER BY om.message_time DESC;
GO
```
