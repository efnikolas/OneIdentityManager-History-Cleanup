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

-- ── HELPER: batched delete by date column ───────────────────
-- Deletes rows WHERE [DateCol] < @CutoffDate in batches.
DECLARE @sql NVARCHAR(MAX)
DECLARE @d   INT
DECLARE @tot BIGINT
DECLARE @st  DATETIME
DECLARE @sec INT
DECLARE @rate BIGINT

-- ── HELPER: batched delete via FK join ──────────────────────
-- For child tables with no date column: delete where parent
-- row is older than cutoff.

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
        WHERE parent.OperationDate < @CutoffDate
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            PRINT '  ' + CAST(@tot AS VARCHAR) + ' deleted | ' + CAST(@sec AS VARCHAR) + 's | ~' + CAST(@rate AS VARCHAR) + ' rows/sec'
        END
    END
    PRINT '  Done: ' + CAST(@tot AS VARCHAR) + ' rows removed.'
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
        DELETE TOP (@BatchSize) FROM WatchOperation WHERE OperationDate < @CutoffDate
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            PRINT '  ' + CAST(@tot AS VARCHAR) + ' deleted | ' + CAST(@sec AS VARCHAR) + 's | ~' + CAST(@rate AS VARCHAR) + ' rows/sec'
        END
    END
    PRINT '  Done: ' + CAST(@tot AS VARCHAR) + ' rows removed.'
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
        DELETE TOP (@BatchSize) FROM ProcessStep WHERE ThisDate < @CutoffDate
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            PRINT '  ' + CAST(@tot AS VARCHAR) + ' deleted | ' + CAST(@sec AS VARCHAR) + 's | ~' + CAST(@rate AS VARCHAR) + ' rows/sec'
        END
    END
    PRINT '  Done: ' + CAST(@tot AS VARCHAR) + ' rows removed.'
END

-- ────────────────────────────────────────────────────────────
-- 4. ProcessSubstitute  (FK join → ProcessInfo.FirstDate)
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
        WHERE parent.FirstDate < @CutoffDate
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            PRINT '  ' + CAST(@tot AS VARCHAR) + ' deleted | ' + CAST(@sec AS VARCHAR) + 's | ~' + CAST(@rate AS VARCHAR) + ' rows/sec'
        END
    END
    PRINT '  Done: ' + CAST(@tot AS VARCHAR) + ' rows removed.'
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
        DELETE TOP (@BatchSize) FROM ProcessChain WHERE ThisDate < @CutoffDate
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            PRINT '  ' + CAST(@tot AS VARCHAR) + ' deleted | ' + CAST(@sec AS VARCHAR) + 's | ~' + CAST(@rate AS VARCHAR) + ' rows/sec'
        END
    END
    PRINT '  Done: ' + CAST(@tot AS VARCHAR) + ' rows removed.'
END

-- ────────────────────────────────────────────────────────────
-- 6. HistoryJob  (StartAt)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('HistoryJob', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning HistoryJob...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM HistoryJob WHERE StartAt < @CutoffDate
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            PRINT '  ' + CAST(@tot AS VARCHAR) + ' deleted | ' + CAST(@sec AS VARCHAR) + 's | ~' + CAST(@rate AS VARCHAR) + ' rows/sec'
        END
    END
    PRINT '  Done: ' + CAST(@tot AS VARCHAR) + ' rows removed.'
END

-- ────────────────────────────────────────────────────────────
-- 7. HistoryChain  (FirstDate)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('HistoryChain', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning HistoryChain...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM HistoryChain WHERE FirstDate < @CutoffDate
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            PRINT '  ' + CAST(@tot AS VARCHAR) + ' deleted | ' + CAST(@sec AS VARCHAR) + 's | ~' + CAST(@rate AS VARCHAR) + ' rows/sec'
        END
    END
    PRINT '  Done: ' + CAST(@tot AS VARCHAR) + ' rows removed.'
END

-- ────────────────────────────────────────────────────────────
-- 8. ProcessInfo  (FirstDate)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessInfo', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessInfo...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessInfo WHERE FirstDate < @CutoffDate
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            PRINT '  ' + CAST(@tot AS VARCHAR) + ' deleted | ' + CAST(@sec AS VARCHAR) + 's | ~' + CAST(@rate AS VARCHAR) + ' rows/sec'
        END
    END
    PRINT '  Done: ' + CAST(@tot AS VARCHAR) + ' rows removed.'
END

-- ────────────────────────────────────────────────────────────
-- 9. ProcessGroup  (FirstDate)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessGroup', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessGroup...'
    SET @d = 1; SET @tot = 0; SET @st = GETDATE()
    WHILE @d > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessGroup WHERE FirstDate < @CutoffDate
        SET @d = @@ROWCOUNT; SET @tot += @d
        IF @d > 0 BEGIN CHECKPOINT; SET @sec = DATEDIFF(SECOND, @st, GETDATE()); SET @rate = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END
            PRINT '  ' + CAST(@tot AS VARCHAR) + ' deleted | ' + CAST(@sec AS VARCHAR) + 's | ~' + CAST(@rate AS VARCHAR) + ' rows/sec'
        END
    END
    PRINT '  Done: ' + CAST(@tot AS VARCHAR) + ' rows removed.'
END

-- ============================================================
PRINT ''
PRINT '================================================'
PRINT 'Cleanup complete.'
PRINT '================================================'
GO
