# One Identity Manager — History Database Cleanup

## Overview

Scripts to purge archived data older than 2 years from One Identity Manager **History Databases** (HDBs / TimeTrace databases). These are the separate archive databases used by OIM's Data Archiving feature — they contain process monitoring, change tracking, and job history data imported from the live application database.

> **Important:** The HDB schema is completely different from the application database. Tables like `DialogHistory` or `PersonWantsOrg` do **not** exist in the HDB. This toolkit targets the correct HDB tables as documented in the [OIM Data Archiving Administration Guide](https://support.oneidentity.com/technical-documents/identity-manager/9.2/data-archiving-administration-guide).

## Files

| File | Description |
|------|-------------|
| `cleanup_history.sql` | Main SQL cleanup script — run in SSMS against a single HDB |
| `Invoke-OIMHistoryCleanup.ps1` | PowerShell wrapper — multi-HDB support, logging, batching, dry-run |
| `audit_hdb_age.sql` | **Read-only** audit — date ranges, purge counts, year-by-year breakdown, oldest records |
| `discover_hdb_schema.sql` | Schema discovery — inspect tables, columns, FKs, row counts, sample data |
| `create_test_data.sql` | Inserts `TEST_CLEANUP_` prefixed test rows into an HDB for safe testing |
| `remove_test_data.sql` | Removes all `TEST_CLEANUP_` test rows to restore the HDB |

## HDB Table Reference

The OIM History Database contains 20 tables in three categories:

### Raw Data (bulk-imported from application DB)

| HDB Table | Source (App DB) | Description |
|-----------|----------------|-------------|
| `RawJobHistory` | `JobHistory` | Raw process history records |
| `RawProcess` | `DialogProcess` | Raw process triggers/actions |
| `RawProcessChain` | `DialogProcessChain` | Raw process chain tracking |
| `RawProcessGroup` | `DialogProcess` (GenProcIDGroup) | Raw process trigger grouping |
| `RawProcessStep` | `DialogProcessStep` | Raw process step tracking |
| `RawProcessSubstitute` | `DialogProcessSubstitute` | Raw process substitution records |
| `RawWatchOperation` | `DialogWatchOperation` | Raw data change operations |
| `RawWatchProperty` | `DialogWatchProperty` | Raw data change property values |

### Aggregated Data (statistical summaries)

| HDB Table | Derived From | Description |
|-----------|-------------|-------------|
| `HistoryChain` | `RawJobHistory` | Process history chains |
| `HistoryJob` | `RawJobHistory` | Process history steps |
| `ProcessChain` | `RawProcessChain` | Aggregated process chains |
| `ProcessGroup` | `RawProcessGroup` | Aggregated process groups |
| `ProcessInfo` | `RawProcess` | Aggregated process triggers |
| `ProcessStep` | `RawProcessStep` | Aggregated process steps |
| `ProcessSubstitute` | `RawProcessSubstitute` | Aggregated substitution records |
| `WatchOperation` | `RawWatchOperation` | Aggregated data change operations |
| `WatchProperty` | `RawWatchProperty` | Aggregated data change properties |

### Metadata (DO NOT DELETE)

| HDB Table | Description |
|-----------|-------------|
| `SourceColumn` | Source column definitions |
| `SourceDatabase` | Source database references |
| `SourceTable` | Source table definitions |

## Usage

### SQL Script (SSMS)

1. Open `cleanup_history.sql` in SQL Server Management Studio
2. Change `USE [OneIMHDB]` to your History Database name
3. Connect to the SQL Server hosting the HDB
4. Run — the script prints a pre-flight summary, deletes old data in FK-safe order, and prints remaining counts
5. Repeat for each HDB, or use the PowerShell wrapper for multiple databases

### PowerShell Script (supports multiple HDBs)

Requires the `SqlServer` PowerShell module (`Install-Module SqlServer`).

```powershell
# Single HDB — dry run (report only)
.\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIMHDB" -WhatIf

# Single HDB — actual cleanup (2-year retention, default)
.\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIMHDB"

# Multiple HDBs — list them explicitly
.\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" `
    -Database "OneIMHDB","OneIMHDB2","OneIMHDB3","OneIMHDB4","OneIMHDB5","OneIMHDB6","OneIMHDB7" `
    -WhatIf

# Multiple HDBs — build the list dynamically (e.g. OneIMHDB, OneIMHDB2..OneIMHDB7)
$hdbs = @("OneIMHDB") + (2..7 | ForEach-Object { "OneIMHDB$_" })
.\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database $hdbs -WhatIf

# Auto-discover all HDBs from SQL Server
$hdbs = (Invoke-Sqlcmd -ServerInstance "myserver" -Query "SELECT name FROM sys.databases WHERE name LIKE 'OneIMHDB%' ORDER BY name").name
.\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database $hdbs -WhatIf

# Custom retention period (3 years)
.\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIMHDB","OneIMHDB2" -RetentionYears 3

# Custom batch size
.\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIMHDB" -BatchSize 5000
```

### Schema Discovery

Run `discover_hdb_schema.sql` to inspect an HDB before cleanup:

```
1. Open discover_hdb_schema.sql in SSMS
2. Change USE [OneIMHDB] to your HDB
3. Run — outputs tables, row counts, sizes, FKs, date columns, and sample data
```

### Auditing — Check What Will Be Purged

Run `audit_hdb_age.sql` **before** cleanup to verify which data is older than 2 years:

```
1. Open audit_hdb_age.sql in SSMS
2. Change USE [OneIMHDB] to your HDB
3. Run — completely read-only, no data is modified
```

The audit shows three sections:

1. **Per-table summary** — oldest record, newest record, total rows, rows to purge, rows to keep, purge percentage
2. **Year-by-year breakdown** — row counts per year with PURGE / PARTIAL / KEEP labels so you can see exactly which years will be affected
3. **Oldest 5 records** — spot-check the actual oldest rows in each table to confirm they're genuinely old data

## Testing with Real HDBs

The test scripts let you safely validate the cleanup against a **real History Database** without risking existing data. All test rows use a `TEST_CLEANUP_` prefix.

```
1. Edit create_test_data.sql → change USE [OneIMHDB] to your HDB
2. Run create_test_data.sql   → inserts prefixed test rows (spread over 4 years)
3. Run cleanup_history.sql    → purges rows older than 2 years
4. Verify that old TEST_CLEANUP_ rows were deleted (see queries below)
5. Run remove_test_data.sql   → removes ALL remaining TEST_CLEANUP_ rows
6. Your HDB is back to its original state
```

## How It Works

1. **Dynamic date column discovery** — each table is scanned for datetime columns, preferring `XDateInserted` > `XDateUpdated` > `StartDate` > `EndDate`
2. **FK-safe delete order** — Raw child tables are deleted before their Aggregated parent tables
3. **Batched deletes** — rows are deleted in configurable batches (default 10,000) with `CHECKPOINT` after each table to manage transaction log growth
4. **Metadata protection** — `SourceColumn`, `SourceDatabase`, and `SourceTable` are never touched

## Compatibility

- SQL Server 2012+
- One Identity Manager 8.x / 9.x History Databases
- PowerShell 5.1+ with SqlServer module

-- QBMDBQueueHistory
SELECT COUNT(*) AS Remaining FROM QBMDBQueueHistory WHERE SlotName LIKE 'TEST_CLEANUP_%' AND XDateInserted < DATEADD(YEAR, -2, GETDATE());

-- QBMProcessHistory
SELECT COUNT(*) AS Remaining FROM QBMProcessHistory WHERE ProcessName LIKE 'TEST_CLEANUP_%' AND XDateInserted < DATEADD(YEAR, -2, GETDATE());

-- QBMDBQueueSlotHistory
SELECT COUNT(*) AS Remaining FROM QBMDBQueueSlotHistory WHERE SlotName LIKE 'TEST_CLEANUP_%' AND XDateInserted < DATEADD(YEAR, -2, GETDATE());
```

All queries should return **0** after a successful cleanup. Any non-zero result means the cleanup missed rows in that table.

> **Note:** The cleanup script purges by date, not by prefix — so it will also delete any real data older than 2 years. If you only want to test against the prefixed data, run the cleanup on an HDB that has no real data you need to keep, or adjust the retention period.

## ⚠️ Important

- **Always backup your database** before running these scripts
- **Run the dry run first** to review what will be deleted
- **Check compliance requirements** — some regulations require longer retention
- **Schedule during maintenance windows** — batch deletes can cause lock contention
- **Rebuild indexes** after large deletions for optimal performance
- Requires the `SqlServer` PowerShell module (`Install-Module SqlServer`)
