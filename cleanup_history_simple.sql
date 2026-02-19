-- ============================================================
-- One Identity Manager — HDB Cleanup (Simple Version)
-- ============================================================
-- Deletes data older than @CutoffDate from all HDB tables.
-- Schema is hardcoded per OIM 9.x Data Archiving docs.
--
-- Tables:  20 total in HDB schema
--   - 8 Raw* tables: staging/inbox, auto-cleaned after processing (SKIPPED)
--   - 3 metadata tables: SourceColumn/Database/Table (SKIPPED)
--   - 9 aggregated tables: the actual data we clean up
-- Order:   Children first, parents last (FK-safe)
--
-- ⚠️  BACKUP YOUR DATABASE BEFORE RUNNING THIS SCRIPT
-- ============================================================

USE [OneIMHDB3]   -- ← change to your HDB name
GO
SET NOCOUNT ON
GO

-- ── CONFIG ──────────────────────────────────────────────────
DECLARE @CutoffDate  DATETIME = DATEADD(YEAR, -2, GETDATE())
DECLARE @BatchSize   INT      = 50000
DECLARE @WhatIf      BIT      = 0        -- 1 = print counts only
-- ────────────────────────────────────────────────────────────

PRINT '================================================'
PRINT 'HDB Cleanup — ' + DB_NAME()
PRINT 'Cutoff:     ' + CONVERT(VARCHAR, @CutoffDate, 120)
PRINT 'Batch size: ' + CAST(@BatchSize AS VARCHAR)
PRINT 'WhatIf:     ' + CAST(@WhatIf AS VARCHAR)
PRINT '================================================'
PRINT ''

-- ── PRE-FLIGHT (instant estimated counts) ───────────────────
PRINT '# PRE-FLIGHT'
PRINT '------------------------------------------------'

DECLARE @est BIGINT
DECLARE @tbl NVARCHAR(128)
DECLARE @preflight TABLE (TableName NVARCHAR(128))
INSERT @preflight VALUES
  ('WatchProperty'),('WatchOperation'),('ProcessStep'),
  ('ProcessSubstitute'),('ProcessChain'),('HistoryJob'),
  ('HistoryChain'),('ProcessInfo'),('ProcessGroup')

DECLARE pf CURSOR LOCAL FAST_FORWARD FOR SELECT TableName FROM @preflight
OPEN pf
FETCH NEXT FROM pf INTO @tbl
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(@tbl, 'U') IS NOT NULL
    BEGIN
        SELECT @est = SUM(row_count)
        FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(@tbl) AND index_id IN (0,1)
        PRINT '  ' + @tbl + ': ~' + CAST(ISNULL(@est,0) AS VARCHAR) + ' rows'
    END
    FETCH NEXT FROM pf INTO @tbl
END
CLOSE pf; DEALLOCATE pf

PRINT ''

IF @WhatIf = 1
BEGIN
    PRINT 'WhatIf mode — nothing deleted.'
    RETURN
END

-- ── Variables ────────────────────────────────────────────────
DECLARE @sql NVARCHAR(MAX)
DECLARE @d   INT
DECLARE @tot BIGINT
DECLARE @st  DATETIME
DECLARE @sec INT
DECLARE @rate BIGINT

-- ── Create temporary indexes for cleanup performance ────────
-- These dramatically speed up FK-join deletes and date scans.
-- Dropped FIRST to avoid bloat from a previous cancelled run,
-- then recreated fresh. Also dropped again at end of script.
PRINT '# CREATING CLEANUP INDEXES'
PRINT '------------------------------------------------'

-- Drop stale indexes from any previous cancelled run
DECLARE @ixDrop NVARCHAR(500)
DECLARE ix_clean CURSOR LOCAL FAST_FORWARD FOR
    SELECT 'DROP INDEX [' + i.name + '] ON [' + OBJECT_NAME(i.object_id) + ']'
    FROM sys.indexes i
    WHERE i.name LIKE 'IX_Cleanup_%' AND i.is_hypothetical = 0
OPEN ix_clean
FETCH NEXT FROM ix_clean INTO @ixDrop
WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '  Dropping stale: ' + @ixDrop
    EXEC sp_executesql @ixDrop
    FETCH NEXT FROM ix_clean INTO @ixDrop
END
CLOSE ix_clean; DEALLOCATE ix_clean

-- FK-join indexes (for WatchProperty & ProcessSubstitute)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_WatchProp_FK')
    CREATE NONCLUSTERED INDEX IX_Cleanup_WatchProp_FK ON WatchProperty (UID_DialogWatchOperation)
PRINT '  Created IX_Cleanup_WatchProp_FK on WatchProperty'

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_WatchOp_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_WatchOp_Date ON WatchOperation (OperationDate) INCLUDE (UID_DialogWatchOperation)
PRINT '  Created IX_Cleanup_WatchOp_Date on WatchOperation'

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcSub_FK')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcSub_FK ON ProcessSubstitute (UID_ProcessInfoNew)
PRINT '  Created IX_Cleanup_ProcSub_FK on ProcessSubstitute'

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcInfo_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcInfo_Date ON ProcessInfo (FirstDate) INCLUDE (UID_ProcessInfo)
PRINT '  Created IX_Cleanup_ProcInfo_Date on ProcessInfo'

-- Date column indexes (for direct date deletes)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_WatchOp_OpDate')
    CREATE NONCLUSTERED INDEX IX_Cleanup_WatchOp_OpDate ON WatchOperation (OperationDate)
PRINT '  Created IX_Cleanup_WatchOp_OpDate on WatchOperation'

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcStep_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcStep_Date ON ProcessStep (ThisDate)
PRINT '  Created IX_Cleanup_ProcStep_Date on ProcessStep'

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcChain_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcChain_Date ON ProcessChain (ThisDate)
PRINT '  Created IX_Cleanup_ProcChain_Date on ProcessChain'

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_HistJob_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_HistJob_Date ON HistoryJob (StartAt)
PRINT '  Created IX_Cleanup_HistJob_Date on HistoryJob'

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_HistChain_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_HistChain_Date ON HistoryChain (FirstDate)
PRINT '  Created IX_Cleanup_HistChain_Date on HistoryChain'

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcInfo_FirstDate')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcInfo_FirstDate ON ProcessInfo (FirstDate)
PRINT '  Created IX_Cleanup_ProcInfo_FirstDate on ProcessInfo'

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcGroup_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcGroup_Date ON ProcessGroup (FirstDate)
PRINT '  Created IX_Cleanup_ProcGroup_Date on ProcessGroup'

PRINT ''

-- ============================================================
-- DELETE ORDER: children first, parents last
-- ============================================================
PRINT '# CLEANUP'
PRINT '================================================'

-- ────────────────────────────────────────────────────────────
-- 1. WatchProperty  (FK join → WatchOperation.OperationDate)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('WatchProperty', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning WatchProperty (via WatchOperation)...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) child
        FROM WatchProperty child
        INNER JOIN WatchOperation parent
            ON child.UID_DialogWatchOperation = parent.UID_DialogWatchOperation
        WHERE parent.OperationDate < @CutoffDate OR parent.OperationDate IS NULL
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            RAISERROR('  %I64d deleted | %ds | ~%I64d rows/sec', 0, 1, @tot, @sec, @rate) WITH NOWAIT
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @tot) WITH NOWAIT
END

-- ────────────────────────────────────────────────────────────
-- 2. WatchOperation  (OperationDate)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('WatchOperation', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning WatchOperation...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM WatchOperation WHERE OperationDate < @CutoffDate OR OperationDate IS NULL
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            RAISERROR('  %I64d deleted | %ds | ~%I64d rows/sec', 0, 1, @tot, @sec, @rate) WITH NOWAIT
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @tot) WITH NOWAIT
END

-- ────────────────────────────────────────────────────────────
-- 3. ProcessStep  (ThisDate)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessStep', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessStep...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessStep WHERE ThisDate < @CutoffDate OR ThisDate IS NULL
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            RAISERROR('  %I64d deleted | %ds | ~%I64d rows/sec', 0, 1, @tot, @sec, @rate) WITH NOWAIT
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @tot) WITH NOWAIT
END

-- ────────────────────────────────────────────────────────────
-- 4. ProcessSubstitute  (FK join → ProcessInfo, COALESCE(FirstDate, LastDate))
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessSubstitute', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessSubstitute (via ProcessInfo)...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) child
        FROM ProcessSubstitute child
        INNER JOIN ProcessInfo parent ON child.UID_ProcessInfoNew = parent.UID_ProcessInfo
        WHERE COALESCE(parent.FirstDate, parent.LastDate) < @CutoffDate
              OR (parent.FirstDate IS NULL AND parent.LastDate IS NULL)
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            RAISERROR('  %I64d deleted | %ds | ~%I64d rows/sec', 0, 1, @tot, @sec, @rate) WITH NOWAIT
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @tot) WITH NOWAIT
END

-- ────────────────────────────────────────────────────────────
-- 5. ProcessChain  (ThisDate)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessChain', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessChain...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessChain WHERE ThisDate < @CutoffDate OR ThisDate IS NULL
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            RAISERROR('  %I64d deleted | %ds | ~%I64d rows/sec', 0, 1, @tot, @sec, @rate) WITH NOWAIT
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @tot) WITH NOWAIT
END

-- ────────────────────────────────────────────────────────────
-- 6. HistoryJob  (COALESCE(StartAt, ReadyAt))
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('HistoryJob', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning HistoryJob...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM HistoryJob
        WHERE COALESCE(StartAt, ReadyAt) < @CutoffDate
              OR (StartAt IS NULL AND ReadyAt IS NULL)
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            RAISERROR('  %I64d deleted | %ds | ~%I64d rows/sec', 0, 1, @tot, @sec, @rate) WITH NOWAIT
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @tot) WITH NOWAIT
END

-- ────────────────────────────────────────────────────────────
-- 7. HistoryChain  (COALESCE(FirstDate, LastDate))
--    FirstDate is often NULL; LastDate is reliably populated
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('HistoryChain', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning HistoryChain...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM HistoryChain
        WHERE COALESCE(FirstDate, LastDate) < @CutoffDate
              OR (FirstDate IS NULL AND LastDate IS NULL)
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            RAISERROR('  %I64d deleted | %ds | ~%I64d rows/sec', 0, 1, @tot, @sec, @rate) WITH NOWAIT
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @tot) WITH NOWAIT
END

-- ────────────────────────────────────────────────────────────
-- 8. ProcessInfo  (COALESCE(FirstDate, LastDate))
--    FirstDate is often NULL; LastDate is reliably populated
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessInfo', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessInfo...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessInfo
        WHERE COALESCE(FirstDate, LastDate) < @CutoffDate
              OR (FirstDate IS NULL AND LastDate IS NULL)
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            RAISERROR('  %I64d deleted | %ds | ~%I64d rows/sec', 0, 1, @tot, @sec, @rate) WITH NOWAIT
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @tot) WITH NOWAIT
END

-- ────────────────────────────────────────────────────────────
-- 9. ProcessGroup  (COALESCE(FirstDate, LastDate))
--    FirstDate is often NULL; LastDate is reliably populated
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessGroup', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessGroup...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessGroup
        WHERE COALESCE(FirstDate, LastDate) < @CutoffDate
              OR (FirstDate IS NULL AND LastDate IS NULL)
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            RAISERROR('  %I64d deleted | %ds | ~%I64d rows/sec', 0, 1, @tot, @sec, @rate) WITH NOWAIT
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @tot) WITH NOWAIT
END

-- ── Drop cleanup indexes ────────────────────────────────────
PRINT ''
PRINT '# DROPPING CLEANUP INDEXES'
PRINT '------------------------------------------------'

DECLARE @ixClean NVARCHAR(500)
DECLARE ix_drop CURSOR LOCAL FAST_FORWARD FOR
    SELECT 'DROP INDEX [' + i.name + '] ON [' + OBJECT_NAME(i.object_id) + ']'
    FROM sys.indexes i
    WHERE i.name LIKE 'IX_Cleanup_%' AND i.is_hypothetical = 0
OPEN ix_drop
FETCH NEXT FROM ix_drop INTO @ixClean
WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '  ' + @ixClean
    EXEC sp_executesql @ixClean
    FETCH NEXT FROM ix_drop INTO @ixClean
END
CLOSE ix_drop; DEALLOCATE ix_drop

-- ============================================================
PRINT ''
PRINT '================================================'
PRINT 'Cleanup complete.'
PRINT '================================================'
GO
