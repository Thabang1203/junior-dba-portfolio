# junior-dba-portfolio
# Junior DBA Portfolio - SQL Server Administration

## Project Overview

This portfolio demonstrates professional-level SQL Server Database Administration skills across all 9 core competency areas required for a Junior DBA role. Every script is production-ready, thoroughly documented, and follows Microsoft best practices.

## Target Role
Junior Database Administrator - SQL Server Environment

## What's Included

### 1. Performance Tuning and Optimization

Scripts:
- 01_identify_missing_indexes.sql - Uses DMVs to find missing indexes and generates CREATE INDEX statements
- 02_index_maintenance_job.sql - Intelligent index maintenance (REBUILD vs REORGANIZE based on fragmentation)

Skills Demonstrated: DMV queries, index analysis, fragmentation management

### 2. Backup and Recovery

Scripts:
- 01_backup_strategy.sql - Full/Differential/Transaction Log backup stored procedures plus Point-in-Time Recovery

Skills Demonstrated: Backup strategies, PITR, disaster recovery procedures

### 3. Security Management

Scripts:
- 01_security_management.sql - RBAC, Row-Level Security, TDE audit, security hardening checklist

Skills Demonstrated: Principle of least privilege, encryption, compliance auditing

### 4. Capacity Planning

Scripts:
- 01_capacity_planning.sql - Growth projection stored procedure with N-month forecasting

Skills Demonstrated: Trend analysis, resource forecasting, proactive planning

### 5. Health Monitoring

Scripts:
- 01_health_monitoring.sql - CPU ring buffer analysis, wait stats with diagnosis, blocking chain detection

Skills Demonstrated: Performance baselining, bottleneck identification, real-time monitoring

### 6. Troubleshooting Toolkit

Scripts:
- 01_troubleshooting_toolkit.sql - Deadlock XML extraction, DBCC CHECKDB wrapper, safe KILL stored procedure

Skills Demonstrated: Deadlock analysis, corruption checking, issue resolution

### 7. High Availability

Scripts:
- 01_always_on_setup_monitor.sql - Always On AG setup, RPO/RTO estimation, failover stored procedure

Skills Demonstrated: HA/DR configurations, failover testing, SLA compliance

### 8. SSIS/ETL Framework

Scripts:
- 01_ssis_etl_framework.sql - ETL Control DB, watermark incremental load, SSISDB monitoring

Skills Demonstrated: ETL patterns, data integration, SSIS administration

## Getting Started

### Prerequisites

- SQL Server 2016 or higher
- SQL Server Management Studio (SSMS) or Azure Data Studio
- Appropriate permissions:
  - VIEW SERVER STATE for monitoring scripts
  - ALTER permissions for index maintenance
  - BACKUP DATABASE for backup scripts
  - CONTROL SERVER for security scripts (or delegated permissions)

### Quick Start

Run missing index analysis:
USE YourDatabaseName;
EXEC dbo.usp_IdentifyMissingIndexes;

Run capacity forecast:
EXEC dbo.usp_CapacityForecast @MonthsToForecast = 6;

## Skills Matrix

Competency 1 - Performance: 2 scripts - Production query slow? Find missing indexes

Competency 2 - Backup/Recovery: 1 script - Accidental data deletion? PITR recovery

Competency 3 - Security: 1 script - Audit requirement? RBAC implementation

Competency 4 - Capacity: 1 script - Running out of space? Growth forecast

Competency 5 - Monitoring: 1 script - Server slow? Wait stats analysis

Competency 6 - Troubleshooting: 1 script - Deadlock happening? Extract and analyze

Competency 7 - High Availability: 1 script - Failover needed? AG automation

Competency 8 - ETL: 1 script - Daily data load? Incremental pattern

## Usage Examples

### Production Environment

Weekly index maintenance:
EXEC dbo.usp_IndexMaintenance @FragmentationThreshold = 30;

Daily backup:
EXEC dbo.usp_BackupDatabase @DatabaseName = 'ProductionDB', @Type = 'FULL';

Hourly health check:
EXEC dbo.usp_HealthMonitor;

## Why This Portfolio Stands Out

Production-Ready Code - Not just tutorials, but scripts you could run in a real environment

Comprehensive Coverage - All 9 DBA responsibility areas from the job spec

Documented Best Practices - Each script follows Microsoft recommendations

Safety First - Includes checks, error handling, and safe defaults

Real Problem Solving - Each script addresses actual DBA pain points

## Connect With Me

[Your LinkedIn]
[Your Email]
[Your Personal Website - if you have one]

## Portfolio Last Updated

April 2026

## Disclaimer

Always test scripts in a development environment first. Review and understand each script before running in production.
