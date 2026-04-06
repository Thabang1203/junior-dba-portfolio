Here is your README in a simple, clean format without emojis or complex formatting:

```markdown
# SQL Server Junior DBA Portfolio Project
## Norman Mathe | BCom Information Systems | University of Fort Hare

A comprehensive, production-ready SQL Server DBA toolkit demonstrating all 9 core competencies required for a Junior Database Administrator role. Built around Microsoft SQL Server 2016+ using T-SQL, DMVs, SQL Agent, SSIS, and Always On.

## Technologies Used

- SQL Server 2016+ - Core RDBMS platform
- T-SQL - All scripting and automation
- SQL Server DMVs - Performance and health monitoring
- SQL Server Agent - Job scheduling
- SSIS (SSISDB) - ETL pipeline management
- Always On AG - High availability and DR
- Extended Events - Deadlock capture
- DBCC Commands - Integrity checks

---

## Competency Coverage

### 1. Performance Tuning
File: 01_performance_tuning/

- Identifies missing indexes ranked by impact measure (seeks x cost x impact)
- Auto-generates CREATE INDEX statements ready for review
- Flags unused indexes consuming write overhead with removal candidates
- Analyses fragmentation per index with REORGANIZE vs REBUILD recommendations
- Surfaces top CPU-expensive queries with execution plans via sys.dm_exec_query_stats

Key Skills Demonstrated: DMV analysis, index strategy, query plan inspection

---

### 2. Backup and Recovery
File: 02_backup_recovery/01_backup_strategy.sql

- usp_FullBackup - Full backup with CHECKSUM, compression, and RESTORE VERIFYONLY
- usp_DifferentialBackup - Scheduled differential backup SP
- usp_LogBackup - Transaction log backup with recovery model guard
- Documented Point-In-Time Recovery (PITR) procedure with step-by-step comments
- 3-2-1 strategy SQL Agent job definitions (Full daily / Diff 6h / Log 15min)
- Backup health check showing overdue databases with hours since last backup

Key Skills Demonstrated: PITR, backup verification, Agent scheduling, 3-2-1 strategy

---

### 3. Security Management
File: 03_security_management/01_security_management.sql

- Three-tier RBAC: db_readonly_user, db_dataentry_user, db_reporting_user
- usp_CreateDBUser - Parameterised SP to provision users with least privilege
- Row-Level Security (RLS) predicate function with SECURITY POLICY template
- Sysadmin audit query - flags all logins in sysadmin role
- Password policy audit - logins without expiry or complexity checking
- TDE status check across all user databases
- Security hardening: SA account disabled check, xp_cmdshell / Ole Automation status

Key Skills Demonstrated: RBAC, RLS, TDE awareness, security auditing, POPIA alignment

---

### 4. Capacity Planning
File: 04_capacity_planning/01_capacity_planning.sql

- Database file size breakdown (data vs log, used vs free)
- Table-level size report with row counts via usp_TableSizeReport
- Growth history mining from msdb.backupset
- Disk free space monitoring with status indicators (Red/Yellow/Green)
- Auto-growth event tracking with percent vs MB growth flagging
- usp_CapacityForecast - projects storage needs N months forward using growth rate

Key Skills Demonstrated: Trend analysis, forecasting, storage planning, proactive monitoring

---

### 5. Database Health Monitoring
File: 05_health_monitoring/01_health_monitoring.sql

- CPU utilisation from Ring Buffer (last 15 samples)
- Memory - buffer pool, dirty pages, memory utilisation percentage
- Wait statistics - top 15 waits with automated diagnosis text
- Blocking chain - hierarchical tree view of blocked sessions
- I/O latency per file with 100ms threshold alerting
- Error log parser - last 24h events categorised by severity

Key Skills Demonstrated: DMV expertise, proactive monitoring, incident detection

---

### 6. Upgrades and Patch Management
File: 07_troubleshooting/01_troubleshooting_toolkit.sql

- SERVERPROPERTY version/CU/SP inventory
- Pre-patch safety checklist: active jobs, user connections, AG sync state
- Patch-readiness gate - blocks on unhealthy AG or running jobs

---

### 7. Troubleshooting and Support
File: 07_troubleshooting/01_troubleshooting_toolkit.sql

- Active sessions dashboard with elapsed time, waits, logical reads
- Deadlock XML extraction from system_health extended event
- usp_IntegrityCheck - DBCC CHECKDB wrapper with repair mode options
- usp_KillSession - safe session terminator with system SPID guard and elapsed threshold

Key Skills Demonstrated: Incident response, deadlock analysis, corruption detection

---

### 8. High Availability and Disaster Recovery
File: 08_high_availability/01_always_on_setup_monitor.sql

- Full Always On AG configuration script (Primary + Secondary replica join)
- AG health dashboard: role, sync mode, connection state, sync health
- RPO/RTO estimation from log send queue and redo queue sizes
- usp_AGManualFailover - guided failover procedure with planned vs forced options
- Pre-conditions: recovery model validation, endpoint creation reminders

Key Skills Demonstrated: HA architecture, RPO/RTO understanding, failover procedures

---

### 9. SSIS / ETL Management
File: 09_ssis_etl/01_ssis_etl_framework.sql

- ETL Control Database - Jobs metadata, Execution Log, Error Log tables
- usp_ETL_StartRun / usp_ETL_EndRun / usp_ETL_LogError - run lifecycle SPs
- Watermark-based incremental load with MERGE (upsert) pattern
- SSIS Catalog execution history with status decoding
- Failed package message extraction from catalog.operation_messages

Key Skills Demonstrated: ETL design patterns, SSIS monitoring, incremental load, audit logging

---

## Getting Started

### Prerequisites
- SQL Server 2016+ Developer Edition (free) or Express
- SQL Server Management Studio (SSMS) 18+
- AdventureWorks2019 sample database

### Setup

1. Restore AdventureWorks2019:
RESTORE DATABASE [AdventureWorks2019]
FROM DISK = 'C:\Temp\AdventureWorks2019.bak'
WITH MOVE 'AdventureWorks2019' TO 'C:\Data\AdventureWorks2019.mdf',
     MOVE 'AdventureWorks2019_log' TO 'C:\Logs\AdventureWorks2019_log.ldf';

2. Run scripts in order per module. Each script is self-contained and safe to run on Developer Edition.

### Recommended Execution Order
1. 03_security_management - set up users and roles first
2. 01_performance_tuning - analyse and optimise
3. 02_backup_recovery - establish backup schedule
4. 05_health_monitoring - deploy monitoring queries
5. 04_capacity_planning - run after backup history builds
6. 09_ssis_etl - create ETL_Control database
7. 08_high_availability - requires 2-node WSFC lab
8. 07_troubleshooting - on-demand toolkit

---

## Sample Results

### Missing Index Report Output

Table: SalesOrderHeader
ImprovementMeasure: 847,291
UserSeeks: 12,483
AvgImpact: 94.2%
CreateIndexStatement: CREATE INDEX IX_SalesO...

### Backup Health Check Output

Database: AdventureWorks2019
LastFullBackup: 2024-01-15 02:00:00
HoursSinceFullBackup: 6
BackupStatus: OK

Database: Northwind
LastFullBackup: NULL
HoursSinceFullBackup: NULL
BackupStatus: NEVER BACKED UP

---

## Author

Norman Mathe
BCom Information Systems - University of Fort Hare (2024)
Durban, KwaZulu-Natal, South Africa
Targeting: Junior DBA | SQL Developer | Data Engineering roles

Academic Highlights:
- SQL: 100%
- Databases: 91% (Distinction)
- Big Data Analytics Certificate - ORT SA (SI4088)

---

## License

MIT License - free to use, fork, and adapt for learning purposes.
```

This version is clean, easy to read, and professional without any emojis, special characters, or complex formatting.
