-- ============================================================
-- Script: 01_ssis_etl_framework.sql
-- Author: Norman Mathe | Junior DBA Portfolio
-- Purpose: ETL framework - staging, audit logging, error
--          handling, and SSIS support stored procedures
-- Environment: SQL Server 2016+ with SSIS catalog (SSISDB)
-- ============================================================

USE master;
GO

-- ============================================================
-- 1. ETL CONTROL DATABASE SETUP
-- ============================================================
-- Create dedicated ETL control database
IF DB_ID('ETL_Control') IS NULL
    CREATE DATABASE ETL_Control;
GO

USE ETL_Control;
GO

-- -------------------------------------------------------
-- 1a. ETL Job Metadata Table
-- -------------------------------------------------------
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

-- -------------------------------------------------------
-- 1b. ETL Execution Log
-- -------------------------------------------------------
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

-- -------------------------------------------------------
-- 1c. Error Staging Table
-- -------------------------------------------------------
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

-- ============================================================
-- 2. ETL CONTROL STORED PROCEDURES
-- ============================================================

-- 2a. Start ETL Run
CREATE OR ALTER PROCEDURE dbo.usp_ETL_StartRun
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

-- 2b. End ETL Run (success)
CREATE OR ALTER PROCEDURE dbo.usp_ETL_EndRun
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
    SET EndTime       = SYSDATETIME(),
        RowsInserted  = @RowsInserted,
        RowsUpdated   = @RowsUpdated,
        RowsDeleted   = @RowsDeleted,
        RowsRejected  = @RowsRejected,
        WatermarkNew  = @WatermarkNew,
        [Status]      = 'SUCCESS'
    WHERE LogID = @LogID;

    PRINT 'ETL completed. Inserted: ' + CAST(@RowsInserted AS VARCHAR)
        + ' Updated: ' + CAST(@RowsUpdated AS VARCHAR);
END;
GO

-- 2c. Log ETL Failure
CREATE OR ALTER PROCEDURE dbo.usp_ETL_LogError
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

-- ============================================================
-- 3. INCREMENTAL LOAD WITH WATERMARK PATTERN
-- ============================================================
CREATE OR ALTER PROCEDURE dbo.usp_ETL_IncrementalLoad
    @SourceDB       NVARCHAR(128),
    @SourceTable    NVARCHAR(256),
    @TargetTable    NVARCHAR(256),
    @WatermarkCol   NVARCHAR(128),
    @LogID          BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LastWatermark  NVARCHAR(128);
    DECLARE @NewWatermark   NVARCHAR(128);
    DECLARE @RowsInserted   BIGINT;
    DECLARE @RowsUpdated    BIGINT;
    DECLARE @SQL            NVARCHAR(MAX);

    -- Get last successful watermark
    SELECT TOP 1 @LastWatermark = WatermarkNew
    FROM dbo.ETL_ExecutionLog
    WHERE JobName = @SourceTable + '_LOAD'
      AND [Status] = 'SUCCESS'
    ORDER BY EndTime DESC;

    SET @LastWatermark = ISNULL(@LastWatermark, '1900-01-01');

    PRINT 'Loading changes since: ' + @LastWatermark;

    -- UPSERT (MERGE) pattern
    SET @SQL = '
    MERGE INTO [' + @TargetTable + '] AS tgt
    USING (
        SELECT * FROM [' + @SourceDB + ']..' + @SourceTable + '
        WHERE [' + @WatermarkCol + '] > ''' + @LastWatermark + '''
    ) AS src ON tgt.[ID] = src.[ID]
    WHEN MATCHED THEN UPDATE SET tgt.[' + @WatermarkCol + '] = src.[' + @WatermarkCol + ']
    WHEN NOT MATCHED BY TARGET THEN INSERT SELECT src.*;';

    BEGIN TRY
        EXEC sp_executesql @SQL;
        SET @RowsInserted = @@ROWCOUNT;
        SET @NewWatermark = CAST(GETDATE() AS NVARCHAR(128));

        EXEC dbo.usp_ETL_EndRun 
            @LogID        = @LogID,
            @RowsInserted = @RowsInserted,
            @WatermarkNew = @NewWatermark;
    END TRY
    BEGIN CATCH
        EXEC dbo.usp_ETL_LogError 
            @LogID = @LogID, 
            @ErrorMessage = ERROR_MESSAGE();
        THROW;
    END CATCH;
END;
GO

-- ============================================================
-- 4. SSIS CATALOG MONITORING QUERIES
-- ============================================================
USE SSISDB;
GO

-- 4a. Recent SSIS Package Executions
SELECT TOP 20
    e.execution_id                                AS [ExecutionID],
    e.folder_name                                 AS [Folder],
    e.project_name                                AS [Project],
    e.package_name                                AS [Package],
    e.start_time                                  AS [StartTime],
    e.end_time                                    AS [EndTime],
    DATEDIFF(SECOND, e.start_time, e.end_time)    AS [Duration_s],
    CASE e.status
        WHEN 1 THEN '▶ Running'
        WHEN 2 THEN '✓ Success'
        WHEN 3 THEN '❌ Cancelled'
        WHEN 4 THEN '⚠ Failed'
        WHEN 5 THEN '⏸ Pending'
        ELSE CAST(e.status AS VARCHAR)
    END                                           AS [Status],
    e.executed_as_name                            AS [ExecutedBy]
FROM catalog.executions AS e
ORDER BY e.start_time DESC;
GO

-- 4b. Failed SSIS Package Messages
SELECT TOP 50
    om.operation_id                               AS [ExecutionID],
    om.message_time                               AS [Time],
    om.package_name                               AS [Package],
    om.task_name                                  AS [Task],
    om.message                                    AS [ErrorMessage]
FROM catalog.operation_messages AS om
WHERE om.message_type = 120                       -- Error messages
ORDER BY om.message_time DESC;
GO
