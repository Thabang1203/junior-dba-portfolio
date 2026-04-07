# SQL Server Junior DBA Portfolio Project
## Norman Mathe | BCom Information Systems | University of Fort Hare

This is my SQL Server DBA toolkit that shows what I can do as a Junior Database Administrator. I built all these scripts using T-SQL, DMVs, SQL Agent, SSIS, and Always On for SQL Server 2016 and above.

## Technologies I Used

- SQL Server 2016 and above
- T-SQL for all my scripting
- SQL Server DMVs for monitoring
- SQL Server Agent for scheduling jobs
- SSIS and SSISDB for ETL
- Always On Availability Groups for high availability
- Extended Events for deadlock capture
- DBCC commands for database integrity

---

## What I Can Do

### 1. Performance Tuning
File: 01_performance_tuning/

I wrote scripts that find missing indexes and rank them by how much they would help. I also generate the actual CREATE INDEX statements you can run. I find unused indexes that are wasting space and slowing down writes. I check index fragmentation and tell you whether to reorganize or rebuild. I also find the top 10 queries using the most CPU and show you their execution plans.

Key Skills: DMVs, index analysis, query plans

---

### 2. Backup and Recovery
File: 02_backup_recovery/01_backup_strategy.sql

I wrote a full backup procedure that does compression, checksums, and verification. I have a differential backup procedure and a log backup procedure that checks if the database is in FULL recovery mode first. I documented a point in time recovery example showing step by step how to restore. I included SQL Agent job definitions for a 3-2-1 backup strategy. I also have a backup health check query that shows which databases are overdue for backup.

Key Skills: Point in time recovery, backup verification, SQL Agent, 3-2-1 strategy

---

### 3. Security Management
File: 03_security_management/01_security_management.sql

I set up three database roles: read only, data entry, and reporting. I wrote a stored procedure that creates logins and users with least privilege. I built a row level security function that filters data based on who is logged in. I have audit queries that show who is in the sysadmin role and which logins dont have password policies enforced. I check which databases have TDE enabled and which dont. I also check if SA is disabled and if dangerous features like xp_cmdshell are turned off.

Key Skills: RBAC, RLS, TDE, security auditing

---

### 4. Capacity Planning
File: 04_capacity_planning/01_capacity_planning.sql

I have queries that show database sizes broken down by data files and log files, and how much free space is inside each. I wrote a table size report that shows row counts and used space. I pull growth history from the msdb backup tables. I check disk space and show which drives are low. I track auto growth settings and flag percent growth which can cause many VLFs. I also wrote a capacity forecast procedure that projects database growth for the next 6 months.

Key Skills: Trend analysis, forecasting, storage planning

---

### 5. Database Health Monitoring
File: 05_health_monitoring/01_health_monitoring.sql

I monitor CPU usage from the ring buffer. I check memory usage including buffer pool and dirty pages. I show the top 15 wait statistics with diagnosis text explaining what each wait means. I display blocking chains in a tree view so you can see who is blocking who. I check I/O latency per file and flag anything over 100 milliseconds. I also parse the SQL Server error log for the last 24 hours and show errors and warnings.

Key Skills: DMVs, proactive monitoring, incident detection

---

### 6. Upgrades and Patch Management
File: 07_troubleshooting/01_troubleshooting_toolkit.sql

I have a query that shows the SQL Server version, service pack, and cumulative update level. I built a pre patch checklist that shows active jobs, user connections, and AG sync state. This helps you know if the server is safe to patch.

---

### 7. Troubleshooting and Support
File: 07_troubleshooting/01_troubleshooting_toolkit.sql

I have an active sessions dashboard that shows how long queries have been running and what they are waiting on. I extract deadlock XML from the system health extended event. I wrote a DBCC CHECKDB wrapper that can run integrity checks with repair options. I also have a safe kill session procedure that wont let you kill system processes and has a time threshold.

Key Skills: Incident response, deadlock analysis, corruption detection

---

### 8. High Availability and Disaster Recovery
File: 08_high_availability/01_always_on_setup_monitor.sql

I wrote the full Always On configuration script for primary and secondary replicas. I have an AG health dashboard showing role, sync mode, and health status. I estimate RPO from log send queue size and RTO from redo queue size. I built a manual failover procedure that gives you the exact command to run for planned or forced failover.

Key Skills: Always On, RPO and RTO, failover procedures

---

### 9. SSIS and ETL Management
File: 09_ssis_etl/01_ssis_etl_framework.sql

I created an ETL control database with tables for job metadata, execution logs, and error logs. I wrote stored procedures to start a run, end a run successfully, and log failures. I implemented a watermark pattern for incremental loads using MERGE. I also have queries that show recent SSIS package executions and extract error messages from failed packages.

Key Skills: ETL design, SSIS monitoring, incremental load, audit logging

---

## How to Set This Up

What you need:
- SQL Server 2016 or above (Developer Edition is free)
- SQL Server Management Studio 18 or above
- AdventureWorks2019 sample database

Steps:

1. Restore AdventureWorks2019:
RESTORE DATABASE [AdventureWorks2019]
FROM DISK = 'C:\Temp\AdventureWorks2019.bak'
WITH MOVE 'AdventureWorks2019' TO 'C:\Data\AdventureWorks2019.mdf',
     MOVE 'AdventureWorks2019_log' TO 'C:\Logs\AdventureWorks2019_log.ldf';

2. Run the scripts in each module. Each script works on its own.

Best order to run:
1. Security management first to create users and roles
2. Performance tuning to analyse and fix indexes
3. Backup recovery to set up backups
4. Health monitoring to deploy monitoring
5. Capacity planning after backups have run
6. SSIS ETL to create the ETL control database
7. High availability if you have a two node cluster
8. Troubleshooting toolkit for when things go wrong

---

## Sample Output From My Scripts

Missing Index Report:
Table: SalesOrderHeader
UserSeeks: 12,483
AvgImpact: 94.2%
CreateIndexStatement: CREATE INDEX IX_SalesOrderHeader...

Backup Health Check:
Database: AdventureWorks2019
LastFullBackup: 2024-01-15 02:00:00
HoursSinceFullBackup: 6
BackupStatus: OK

Database: Northwind
LastFullBackup: NULL
BackupStatus: NEVER BACKED UP

---

## About Me

Norman Mathe
BCom Information Systems - University of Fort Hare (2024)
Durban, KwaZulu-Natal, South Africa

I am looking for Junior DBA or SQL Developer roles.

My marks:
- SQL: 100%
- Databases: 91% (Distinction)
- Big Data Analytics Certificate from ORT SA (SI4088)

---

