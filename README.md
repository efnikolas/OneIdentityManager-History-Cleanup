# One Identity Manager — History Database Cleanup

## Overview

Scripts to purge history/journal data older than 2 years from One Identity Manager **History Databases** (HDBs). These are the separate archive databases, not the live application database — all data is historical.

## Files

| File | Description |
|------|-------------|
| `cleanup_history.sql` | Pure SQL script — run directly in SSMS against a single HDB |
| `Invoke-OIMHistoryCleanup.ps1` | PowerShell wrapper with multi-HDB support, logging, batching, and dry-run |
| `create_test_data.sql` | Inserts ~335K prefixed (`TEST_CLEANUP_`) test rows into an existing HDB |
| `remove_test_data.sql` | Removes all `TEST_CLEANUP_` test rows — restores HDB to its original state |

## Tables Cleaned

| Table | Data |
|-------|------|
| `DialogHistory` | UI/process execution history |
| `DialogJournal` | Change journal / audit trail |
| `DialogJournalDetail` | Detailed change records (age-based + orphan cleanup) |
| `JobHistory` | Job server execution history |
| `PersonWantsOrg` | Archived request history (all records historical in HDB) |
| `QBMDBQueueHistory` | DBQueue processor history |
| `QBMProcessHistory` | Process orchestration logs |
| `QBMDBQueueSlotHistory` | DBQueue slot history |
| `*History*` (dynamic) | Any additional history tables found automatically |

## Usage

### SQL Script (SSMS)

1. Open `cleanup_history.sql` in SQL Server Management Studio
2. Connect to your One Identity Manager **History Database**
3. **Run as-is for a dry run** — the dynamic section only counts rows
4. Uncomment the `DELETE` blocks in Section 8 to enable full cleanup
5. Repeat for each HDB if you have multiple

### PowerShell Script (supports multiple HDBs)

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

## Testing with Real HDBs

The test scripts let you safely validate the cleanup against a **real History Database** without risking existing data. All test rows are tagged with a `TEST_CLEANUP_` prefix.

```
1. Edit create_test_data.sql → change USE [OneIMHDB] to your HDB
2. Run create_test_data.sql   → inserts ~335K prefixed rows (spread over 4 years)
3. Run cleanup_history.sql    → purges rows older than 2 years (real + test)
4. Verify that old TEST_CLEANUP_ rows were deleted
5. Run remove_test_data.sql   → removes ALL remaining TEST_CLEANUP_ rows
6. Your HDB is back to its original state
```

### Quick Verification Queries (per table)

Run these after `cleanup_history.sql` to confirm old test rows were purged. Only rows newer than 2 years should remain.

```sql
-- DialogHistory
SELECT COUNT(*) AS Remaining FROM DialogHistory WHERE XUserInserted LIKE 'TEST_CLEANUP_%' AND XDateInserted < DATEADD(YEAR, -2, GETDATE());

-- DialogJournal
SELECT COUNT(*) AS Remaining FROM DialogJournal WHERE XUserInserted LIKE 'TEST_CLEANUP_%' AND XDateInserted < DATEADD(YEAR, -2, GETDATE());

-- DialogJournalDetail
SELECT COUNT(*) AS Remaining FROM DialogJournalDetail WHERE ColumnName LIKE 'TEST_CLEANUP_%' AND XDateInserted < DATEADD(YEAR, -2, GETDATE());

-- JobHistory
SELECT COUNT(*) AS Remaining FROM JobHistory WHERE XUserInserted LIKE 'TEST_CLEANUP_%' AND XDateInserted < DATEADD(YEAR, -2, GETDATE());

-- PersonWantsOrg (uses XDateUpdated)
SELECT COUNT(*) AS Remaining FROM PersonWantsOrg WHERE XUserInserted LIKE 'TEST_CLEANUP_%' AND XDateUpdated < DATEADD(YEAR, -2, GETDATE());

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
