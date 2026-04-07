USE master;
GO

-- 1. Create Role-Based Access Control Structure

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

-- Reporting role
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'db_reporting_user')
    CREATE ROLE [db_reporting_user];
GRANT SELECT ON SCHEMA::dbo TO [db_reporting_user];
GRANT EXECUTE ON SCHEMA::dbo TO [db_reporting_user];
GO

-- 2. Create Login & User with Least Privilege
USE master;
GO

CREATE OR ALTER PROCEDURE usp_CreateDBUser
    @LoginName      NVARCHAR(128),
    @Password       NVARCHAR(256),
    @DatabaseName   NVARCHAR(128),
    @RoleName       NVARCHAR(128) = 'db_readonly_user',
    @DefaultSchema  NVARCHAR(128) = 'dbo'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);

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

-- 3. Row-Level Security Function
USE AdventureWorks2019;
GO

CREATE OR ALTER FUNCTION dbo.fn_SecurityPredicate_Sales
    (@SalesPersonID INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 as Access
WHERE 
    IS_MEMBER('db_owner') = 1
    OR IS_MEMBER('db_reporting_user') = 1
    OR @SalesPersonID = (SELECT BusinessEntityID FROM HumanResources.Employee WHERE BusinessEntityID = USER_ID());
GO

-- 4. Security Audit Queries
USE master;
GO

-- Logins with sysadmin role
SELECT 
    sp.name as LoginName,
    sp.type_desc as LoginType,
    sp.create_date as Created,
    sp.modify_date as LastModified,
    LOGINPROPERTY(sp.name, 'IsLocked') as IsLocked,
    LOGINPROPERTY(sp.name, 'PasswordLastSetTime') as PwdLastSet,
    LOGINPROPERTY(sp.name, 'DaysUntilExpiration') as DaysUntilExpiry
FROM sys.server_principals sp
INNER JOIN sys.server_role_members srm ON sp.principal_id = srm.member_principal_id
INNER JOIN sys.server_principals role ON srm.role_principal_id = role.principal_id
WHERE role.name = 'sysadmin'
  AND sp.type NOT IN ('R', 'G')
ORDER BY sp.name;
GO

-- Logins with no password expiry or policy
SELECT 
    name as Login,
    type_desc as LoginType,
    is_policy_checked as PolicyChecked,
    is_expiration_checked as ExpiryChecked,
    create_date,
    modify_date
FROM sys.sql_logins
WHERE is_policy_checked = 0 OR is_expiration_checked = 0
ORDER BY name;
GO

-- Databases without encryption (TDE check)
SELECT
    d.name as DatabaseName,
    d.state_desc as State,
    de.encryption_state_desc as EncryptionState,
    CASE WHEN de.database_id IS NULL THEN 'TDE NOT ENABLED' ELSE 'TDE ENABLED' END as TDEStatus
FROM sys.databases d
LEFT JOIN sys.dm_database_encryption_keys de ON d.database_id = de.database_id
WHERE d.database_id > 4
ORDER BY d.name;
GO

-- Permission audit
USE AdventureWorks2019;
GO

SELECT 
    dp.name as Principal,
    dp.type_desc as PrincipalType,
    o.name as Object,
    o.type_desc as ObjectType,
    p.permission_name as Permission,
    p.state_desc as GrantDenyState
FROM sys.database_permissions p
INNER JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
LEFT JOIN sys.objects o ON p.major_id = o.object_id
WHERE dp.type NOT IN ('R')
ORDER BY dp.name, o.name;
GO

-- 5. Security Hardening Checklist
USE master;
GO

-- Check if SA account is disabled
SELECT 
    name as Login,
    is_disabled as IsDisabled,
    CASE WHEN name = 'sa' AND is_disabled = 0 THEN 'SA account enabled - risk!' ELSE 'OK' END as SAStatus
FROM sys.sql_logins
WHERE name = 'sa';

-- Check if xp_cmdshell is disabled
SELECT 
    name,
    value_in_use,
    CASE WHEN value_in_use = 0 THEN 'Disabled - secure' ELSE 'Enabled - review required' END as Status
FROM sys.configurations
WHERE name IN ('xp_cmdshell', 'Ole Automation Procedures', 'Ad Hoc Distributed Queries');
GO
```
