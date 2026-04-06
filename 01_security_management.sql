-- ============================================================
-- Script: 01_security_management.sql
-- Author: Norman Mathe | Junior DBA Portfolio
-- Purpose: SQL Server security hardening, RBAC, user access
--          management, and vulnerability monitoring
-- Environment: SQL Server 2016+ 
-- ============================================================

USE master;
GO

-- -------------------------------------------------------
-- 1. Create Role-Based Access Control Structure
-- -------------------------------------------------------

-- Database-level roles (run in target database)
USE AdventureWorks2019;
GO

-- Read-only role
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'db_readonly_user')
    CREATE ROLE [db_readonly_user];
GRANT SELECT ON SCHEMA::dbo TO [db_readonly_user];
DENY INSERT, UPDATE, DELETE ON SCHEMA::dbo TO [db_readonly_user];
GO

-- Data entry role
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'db_dataentry_user')
    CREATE ROLE [db_dataentry_user];
GRANT SELECT, INSERT, UPDATE ON SCHEMA::dbo TO [db_dataentry_user];
DENY DELETE ON SCHEMA::dbo TO [db_dataentry_user];
GO

-- Reporting role (can execute report stored procs)
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'db_reporting_user')
    CREATE ROLE [db_reporting_user];
GRANT SELECT ON SCHEMA::dbo TO [db_reporting_user];
GRANT EXECUTE ON SCHEMA::dbo TO [db_reporting_user];
GO

-- -------------------------------------------------------
-- 2. Create Login & User with Least Privilege
-- -------------------------------------------------------
USE master;
GO

CREATE OR ALTER PROCEDURE dbo.usp_CreateDBUser
    @LoginName      NVARCHAR(128),
    @Password       NVARCHAR(256),
    @DatabaseName   NVARCHAR(128),
    @RoleName       NVARCHAR(128) = 'db_readonly_user',
    @DefaultSchema  NVARCHAR(128) = 'dbo'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);

    -- Create server login (SQL Auth)
    IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @LoginName)
    BEGIN
        SET @SQL = 'CREATE LOGIN [' + @LoginName + '] 
                    WITH PASSWORD = ''' + @Password + ''',
                    DEFAULT_DATABASE = [' + @DatabaseName + '],
                    CHECK_EXPIRATION = ON,
                    CHECK_POLICY = ON;';
        EXEC sp_executesql @SQL;
        PRINT 'Login created: ' + @LoginName;
    END
    ELSE
        PRINT 'Login already exists: ' + @LoginName;

    -- Create database user
    SET @SQL = 'USE [' + @DatabaseName + '];
    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ''' + @LoginName + ''')
    BEGIN
        CREATE USER [' + @LoginName + '] FOR LOGIN [' + @LoginName + ']
        WITH DEFAULT_SCHEMA = [' + @DefaultSchema + '];
        ALTER ROLE [' + @RoleName + '] ADD MEMBER [' + @LoginName + '];
        PRINT ''User created and added to role: ' + @RoleName + ''';
    END';

    EXEC sp_executesql @SQL;
END;
GO

-- -------------------------------------------------------
-- 3. Row-Level Security (RLS) Implementation
-- -------------------------------------------------------
USE AdventureWorks2019;
GO

-- Create security predicate for department-based RLS
-- (assumes a SalesOrderHeader with SalesPersonID)
CREATE OR ALTER FUNCTION dbo.fn_SecurityPredicate_Sales
    (@SalesPersonID INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS [Access]
WHERE 
    IS_MEMBER('db_owner') = 1                    -- DBAs see all
    OR IS_MEMBER('db_reporting_user') = 1        -- Reporting sees all
    OR @SalesPersonID = (                        -- Others see own data only
        SELECT BusinessEntityID 
        FROM HumanResources.Employee 
        WHERE BusinessEntityID = USER_ID()
    );
GO

-- Apply filter predicate to Sales table
-- (Commented to avoid modifying AdventureWorks schema)
/*
CREATE SECURITY POLICY SalesDataPolicy
    ADD FILTER PREDICATE dbo.fn_SecurityPredicate_Sales(SalesPersonID)
    ON Sales.SalesOrderHeader
    WITH (STATE = ON);
*/

-- -------------------------------------------------------
-- 4. Security Audit Queries
-- -------------------------------------------------------
USE master;
GO

-- 4a. Logins with sysadmin role
SELECT 
    sp.name                                       AS [LoginName],
    sp.type_desc                                  AS [LoginType],
    sp.create_date                                AS [Created],
    sp.modify_date                                AS [LastModified],
    LOGINPROPERTY(sp.name, 'IsLocked')            AS [IsLocked],
    LOGINPROPERTY(sp.name, 'PasswordLastSetTime') AS [PwdLastSet],
    LOGINPROPERTY(sp.name, 'DaysUntilExpiration') AS [DaysUntilExpiry]
FROM sys.server_principals AS sp
INNER JOIN sys.server_role_members AS srm 
    ON sp.principal_id = srm.member_principal_id
INNER JOIN sys.server_principals AS role 
    ON srm.role_principal_id = role.principal_id
WHERE role.name = 'sysadmin'
  AND sp.type NOT IN ('R', 'G')                   -- Exclude roles/groups
ORDER BY sp.name;
GO

-- 4b. Logins with no password expiry or policy
SELECT 
    name                                          AS [Login],
    type_desc                                     AS [LoginType],
    is_policy_checked                             AS [PolicyChecked],
    is_expiration_checked                         AS [ExpiryChecked],
    create_date,
    modify_date
FROM sys.sql_logins
WHERE is_policy_checked = 0 OR is_expiration_checked = 0
ORDER BY name;
GO

-- 4c. Databases without encryption (TDE check)
SELECT
    d.name                                        AS [Database],
    d.state_desc                                  AS [State],
    de.encryption_state_desc                      AS [EncryptionState],
    CASE WHEN de.database_id IS NULL THEN '⚠ TDE NOT ENABLED' 
         ELSE '✓ TDE ENABLED' END                AS [TDEStatus]
FROM sys.databases AS d
LEFT JOIN sys.dm_database_encryption_keys AS de
    ON d.database_id = de.database_id
WHERE d.database_id > 4                           -- Exclude system DBs
ORDER BY d.name;
GO

-- 4d. Permission audit - who can do what
USE AdventureWorks2019;
GO

SELECT 
    dp.name                                       AS [Principal],
    dp.type_desc                                  AS [PrincipalType],
    o.name                                        AS [Object],
    o.type_desc                                   AS [ObjectType],
    p.permission_name                             AS [Permission],
    p.state_desc                                  AS [GrantDenyState]
FROM sys.database_permissions AS p
INNER JOIN sys.database_principals AS dp
    ON p.grantee_principal_id = dp.principal_id
LEFT JOIN sys.objects AS o
    ON p.major_id = o.object_id
WHERE dp.type NOT IN ('R')                        -- Exclude roles
ORDER BY dp.name, o.name;
GO

-- -------------------------------------------------------
-- 5. Security Hardening Checklist (as queries)
-- -------------------------------------------------------
USE master;
GO

-- Check if SA account is disabled and renamed
SELECT 
    name                                          AS [Login],
    is_disabled                                   AS [IsDisabled],
    CASE WHEN name = 'sa' AND is_disabled = 0 
         THEN '⚠ SA ACCOUNT ENABLED - RISK!' 
         ELSE '✓ OK' END                         AS [SAStatus]
FROM sys.sql_logins
WHERE name = 'sa';

-- Check if xp_cmdshell is disabled
SELECT 
    name,
    value_in_use,
    CASE WHEN value_in_use = 0 THEN '✓ DISABLED (Secure)' 
         ELSE '⚠ ENABLED - REVIEW REQUIRED' END AS [Status]
FROM sys.configurations
WHERE name IN ('xp_cmdshell', 'Ole Automation Procedures', 'Ad Hoc Distributed Queries');
GO
