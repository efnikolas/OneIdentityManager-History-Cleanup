# One Identity Manager — History Database (HDB) Schema Reference

> **Source:** [Identity Manager 9.2 Data Archiving Administration Guide](https://support.oneidentity.com/technical-documents/identity-manager/9.2/data-archiving-administration-guide/4)
> — Section: *Mapping data in the One Identity Manager History Database*
>
> **Important:** Column-level schema details are NOT published in the One Identity
> online HTML documentation. They are only available in the PDF guides or by
> querying the actual database. Run `export_hdb_schema.sql` against your HDB
> to get the exact columns for your version.

---

## Table Mapping (from official documentation)

The Data Archiving Administration Guide (9.2) documents how data flows from the
One Identity Manager database to the History Database:

| Data Category | Source (OIM DB) | Raw (HDB) | Aggregated (HDB) |
|---|---|---|---|
| Grouping of process triggers | `DialogProcess.GenProcIDGroup` | `RawProcessGroup` | **ProcessGroup** |
| Process triggers (actions) | `DialogProcess` | `RawProcess` | **ProcessInfo** (where `UID_ProcessInfo = DialogProcess.GenProcID`) |
| Process tracking — process | `DialogProcessChain` | `RawProcessChain` | **ProcessChain** |
| Process tracking — process step | `DialogProcessStep` | `RawProcessStep` | **ProcessStep** |
| Data change records — operations | `DialogWatchOperation` | `RawWatchOperation` | **WatchOperation** |
| Data change records — columns | `DialogWatchProperty` | `RawWatchProperty` | **WatchProperty** |
| Process history — processes | `JobHistory` (UID_Tree) | `RawJobHistory` | **HistoryChain** (where `UID_HistoryChain = JobHistory.UID_Tree`) |
| Process history — process steps | `JobHistory` | `RawJobHistory` | **HistoryJob** |
| Process substitution (collisions) | `DialogProcessSubstitute` | `RawProcessSubstitute` | **ProcessSubstitute** |

### Additional metadata tables (not in mapping)

| Table | Purpose |
|---|---|
| `SourceColumn` | Column metadata |
| `SourceDatabase` | Source database references |
| `SourceTable` | Table metadata |

---

## Table Categories

### Raw Tables (staging — 8 tables)
These hold raw imported data before aggregation:

1. `RawWatchProperty`
2. `RawWatchOperation`
3. `RawProcessStep`
4. `RawProcessSubstitute`
5. `RawProcessChain`
6. `RawProcess`
7. `RawProcessGroup`
8. `RawJobHistory`

**Key date column:** `XDateInserted` (for most Raw tables), `ExportDate` (RawProcessGroup), `StartAt` (RawJobHistory), `OperationDate` (RawWatchOperation)

### Aggregated Tables (9 tables)
These are the processed/aggregated tables used by TimeTrace:

1. `WatchProperty` — FK to WatchOperation
2. `WatchOperation` — date column: `OperationDate`
3. `ProcessStep` — date column: `ThisDate`
4. `ProcessSubstitute` — FK to ProcessInfo
5. `ProcessChain` — date column: `ThisDate`
6. `HistoryJob` — date column: `StartAt`
7. `HistoryChain` — date columns: `FirstDate`, `LastDate`
8. `ProcessInfo` — date columns: `FirstDate`, `LastDate`
9. `ProcessGroup` — date columns: `FirstDate`, `LastDate`, `ExportDate`, `ImportDate`

### Metadata Tables (3 tables — never deleted)

1. `SourceColumn`
2. `SourceDatabase`
3. `SourceTable`

---

## Foreign Key Relationships (deletion order)

Delete children before parents to avoid FK violations:

```
WatchProperty       → WatchOperation
ProcessSubstitute   → ProcessInfo
ProcessStep         → (standalone or FK-dependent)
ProcessChain        → (standalone or FK-dependent)
WatchOperation      → (standalone)
HistoryJob          → (standalone)
HistoryChain        → (standalone)
ProcessInfo         → ProcessGroup
ProcessGroup        → (root table)
```

---

## Date Columns for Cleanup (verified columns only)

The following date columns have been verified as existing in the HDB schema.
**Note:** `ReadyAt` does NOT exist on `HistoryJob` — only `StartAt`.

| Table | Date Column(s) | Nullable | Cleanup Strategy |
|---|---|---|---|
| **WatchOperation** | `OperationDate` | ? | `WHERE OperationDate < @Cutoff OR OperationDate IS NULL` |
| **WatchProperty** | *(none directly)* | — | FK JOIN to `WatchOperation.OperationDate` |
| **ProcessStep** | `ThisDate` | ? | `WHERE ThisDate < @Cutoff OR ThisDate IS NULL` |
| **ProcessChain** | `ThisDate` | ? | `WHERE ThisDate < @Cutoff OR ThisDate IS NULL` |
| **ProcessSubstitute** | *(none directly)* | — | FK JOIN to `ProcessInfo.FirstDate/LastDate` |
| **HistoryJob** | `StartAt` | ? | `WHERE StartAt < @Cutoff OR StartAt IS NULL` |
| **HistoryChain** | `FirstDate`, `LastDate` | YES, ? | `WHERE COALESCE(FirstDate, LastDate) < @Cutoff OR (FirstDate IS NULL AND LastDate IS NULL)` |
| **ProcessInfo** | `FirstDate`, `LastDate` | YES, ? | `WHERE COALESCE(FirstDate, LastDate) < @Cutoff OR (FirstDate IS NULL AND LastDate IS NULL)` |
| **ProcessGroup** | `FirstDate`, `LastDate`, `ExportDate` | YES, YES, ? | `WHERE COALESCE(FirstDate, LastDate, ExportDate) < @Cutoff OR (all three NULL)` |

> **`?` = Run `export_hdb_schema.sql` to verify nullability for your version.**

---

## How to Get the Exact Schema for Your Database

Run the following against your HDB in SSMS:

```sql
-- In SSMS: Results to Text (Ctrl+T), then execute export_hdb_schema.sql
-- Save the output as your definitive schema reference.
```

Or use the existing discovery script:

```sql
-- Quick discovery (console output)
:r discover_hdb_schema.sql
```

---

## References

- [Identity Manager 9.2 Data Archiving Administration Guide](https://support.oneidentity.com/technical-documents/identity-manager/9.2/data-archiving-administration-guide)
  - [Mapping data in the HDB](https://support.oneidentity.com/technical-documents/identity-manager/9.2/data-archiving-administration-guide/4)
  - [Archiving procedure setup](https://support.oneidentity.com/technical-documents/identity-manager/9.2/data-archiving-administration-guide/3)
- [Identity Manager 9.2 Configuration Guide](https://support.oneidentity.com/technical-documents/identity-manager/9.2/configuration-guide)
- Column-level schema: Only available in the downloadable PDF or by querying the database directly
