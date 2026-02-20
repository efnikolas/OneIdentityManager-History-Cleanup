-- ============================================================================
-- HDB Cleanup — Delete aggregated rows older than 2 years
-- ============================================================================
--
-- Target:  One Identity Manager TimeTrace / History Database (HDB)
-- Scope:   9 aggregated tables only (Raw staging tables are excluded)
-- Rule:    Delete rows where ALL available date columns are older than 2 years.
--          Rows with NULL dates are KEPT unless at least one non-NULL date
--          column is older than 2 years.
--          Rows with ANY date within the last 2 years are NEVER deleted.
--
-- Strategy:
--   - Temporarily switches to SIMPLE recovery so CHECKPOINT actually
--     truncates the log (prevents log file from eating all disk space)
--   - Batched deletes (configurable batch size) to avoid log explosion
--   - CHECKPOINT after each batch to keep log file manageable
--   - FK-safe deletion order: children first, parents last
--   - Fully resumable: rerun safely — already-deleted rows won't match
--   - Pre/post flight row counts + log file size monitoring
--   - Restores original recovery model when done
--
-- IMPORTANT: BACKUP YOUR DATABASE BEFORE RUNNING THIS SCRIPT
--            After completion, take a FULL BACKUP to restart log chain.
-- ============================================================================

----------------------------------------------------------------------
-- CONFIG — Change the database name and batch size here
----------------------------------------------------------------------
USE [OneIMHDB3]          -- << Set your HDB database name
GO
SET NOCOUNT ON
GO

-- ================================================================
-- STEP 0: Switch to SIMPLE recovery to prevent log bloat
-- ================================================================
-- Save original recovery model, then switch to SIMPLE.
-- In SIMPLE mode, CHECKPOINT actually truncates the log.
-- We restore the original model at the end of the script.
DECLARE @OrigRecovery NVARCHAR(60)
SELECT @OrigRecovery = recovery_model_desc
FROM sys.databases WHERE name = DB_NAME()

PRINT '# RECOVERY MODEL'
PRINT '  Current: ' + @OrigRecovery

IF @OrigRecovery <> 'SIMPLE'
BEGIN
    PRINT '  Switching to SIMPLE for cleanup (will restore to ' + @OrigRecovery + ' when done)...'
    -- Use dynamic SQL because ALTER DATABASE doesn't accept variables
    DECLARE @sql NVARCHAR(200) = 'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + ' SET RECOVERY SIMPLE'
    EXEC sp_executesql @sql
    PRINT '  Switched to SIMPLE.'
END
ELSE
    PRINT '  Already SIMPLE — no change needed.'

-- Show log file size before we start
PRINT ''
PRINT '# LOG FILE SIZE (before cleanup)'
SELECT
    name            AS LogicalName,
    size / 128      AS SizeMB,
    FILEPROPERTY(name, 'SpaceUsed') / 128 AS UsedMB
FROM sys.database_files WHERE type_desc = 'LOG'

DECLARE @Cutoff   DATETIME = DATEADD(YEAR, -2, GETDATE())   -- 2 years ago
DECLARE @BatchSize INT     = 500000                          -- rows per batch

-- Internal tracking variables
DECLARE @Deleted   INT
DECLARE @Total     BIGINT
DECLARE @Start     DATETIME
DECLARE @Sec       INT
DECLARE @ScriptStart DATETIME = GETDATE()

PRINT ''
PRINT '============================================================'
PRINT ' HDB CLEANUP — Delete rows older than 2 years'
PRINT '============================================================'
PRINT ''
PRINT ' Cutoff date : ' + CONVERT(VARCHAR(30), @Cutoff, 120)
PRINT ' Batch size  : ' + CAST(@BatchSize AS VARCHAR(20))
PRINT ' Recovery    : SIMPLE (log truncated on each CHECKPOINT)'
PRINT ' Started at  : ' + CONVERT(VARCHAR(30), GETDATE(), 120)
PRINT ''

-- ================================================================
-- PRE-FLIGHT: Show estimated row counts per table
-- ================================================================
PRINT '# PRE-FLIGHT — Estimated row counts'
PRINT '------------------------------------------------------------'

DECLARE @tbl NVARCHAR(128), @est BIGINT
DECLARE @tables TABLE (name NVARCHAR(128), seq INT)
INSERT @tables VALUES
    ('WatchProperty',    1), ('WatchOperation',   2),
    ('ProcessStep',      3), ('ProcessSubstitute',4),
    ('ProcessChain',     5), ('HistoryJob',       6),
    ('HistoryChain',     7), ('ProcessInfo',      8),
    ('ProcessGroup',     9)

DECLARE cur_pre CURSOR LOCAL FAST_FORWARD FOR
    SELECT name FROM @tables ORDER BY seq
OPEN cur_pre
FETCH NEXT FROM cur_pre INTO @tbl
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @est = SUM(row_count)
    FROM sys.dm_db_partition_stats
    WHERE object_id = OBJECT_ID(@tbl) AND index_id IN (0,1)

    RAISERROR('  %-25s ~%I64d rows', 0, 1, @tbl, @est) WITH NOWAIT
    FETCH NEXT FROM cur_pre INTO @tbl
END
CLOSE cur_pre; DEALLOCATE cur_pre
PRINT ''

-- ================================================================
-- CLEANUP — Delete in FK-safe order (children before parents)
-- ================================================================
PRINT '# CLEANUP'
PRINT '============================================================'

-- ────────────────────────────────────────────────────────────────
-- 1. WatchProperty  (FK child of WatchOperation)
--    No own date column — join to parent WatchOperation.OperationDate
--    Only delete if parent's OperationDate is non-NULL and < cutoff
-- ────────────────────────────────────────────────────────────────
IF OBJECT_ID('WatchProperty', 'U') IS NOT NULL
BEGIN
    PRINT ''
    PRINT '[1/9] WatchProperty (via WatchOperation.OperationDate)...'
    SET @Total = 0; SET @Start = GETDATE(); SET @Deleted = 1

    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) wp
        FROM WatchProperty wp
        INNER JOIN WatchOperation wo
            ON wp.UID_DialogWatchOperation = wo.UID_DialogWatchOperation
        WHERE wo.OperationDate IS NOT NULL
          AND wo.OperationDate < @Cutoff

        SET @Deleted = @@ROWCOUNT
        SET @Total  += @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            RAISERROR('    ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
        END
    END

    SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
    RAISERROR('  Done: %I64d rows deleted (%d sec)', 0, 1, @Total, @Sec) WITH NOWAIT
END
ELSE PRINT '[1/9] WatchProperty — table not found, skipping.'

-- ────────────────────────────────────────────────────────────────
-- 2. WatchOperation
--    Date column: OperationDate
--    Only delete if OperationDate is non-NULL and < cutoff
-- ────────────────────────────────────────────────────────────────
IF OBJECT_ID('WatchOperation', 'U') IS NOT NULL
BEGIN
    PRINT ''
    PRINT '[2/9] WatchOperation...'
    SET @Total = 0; SET @Start = GETDATE(); SET @Deleted = 1

    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM WatchOperation
        WHERE OperationDate IS NOT NULL
          AND OperationDate < @Cutoff

        SET @Deleted = @@ROWCOUNT
        SET @Total  += @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            RAISERROR('    ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
        END
    END

    SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
    RAISERROR('  Done: %I64d rows deleted (%d sec)', 0, 1, @Total, @Sec) WITH NOWAIT
END
ELSE PRINT '[2/9] WatchOperation — table not found, skipping.'

-- ────────────────────────────────────────────────────────────────
-- 3. ProcessStep
--    Date column: ThisDate
--    Only delete if ThisDate is non-NULL and < cutoff
-- ────────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessStep', 'U') IS NOT NULL
BEGIN
    PRINT ''
    PRINT '[3/9] ProcessStep...'
    SET @Total = 0; SET @Start = GETDATE(); SET @Deleted = 1

    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessStep
        WHERE ThisDate IS NOT NULL
          AND ThisDate < @Cutoff

        SET @Deleted = @@ROWCOUNT
        SET @Total  += @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            RAISERROR('    ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
        END
    END

    SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
    RAISERROR('  Done: %I64d rows deleted (%d sec)', 0, 1, @Total, @Sec) WITH NOWAIT
END
ELSE PRINT '[3/9] ProcessStep — table not found, skipping.'

-- ────────────────────────────────────────────────────────────────
-- 4. ProcessSubstitute  (FK child of ProcessInfo)
--    No own date column — join to parent ProcessInfo
--    Only delete if parent has at least one non-NULL date < cutoff
--    AND no date that is >= cutoff (protect recent data)
-- ────────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessSubstitute', 'U') IS NOT NULL
BEGIN
    PRINT ''
    PRINT '[4/9] ProcessSubstitute (via ProcessInfo dates)...'
    SET @Total = 0; SET @Start = GETDATE(); SET @Deleted = 1

    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) ps
        FROM ProcessSubstitute ps
        INNER JOIN ProcessInfo pi
            ON ps.UID_ProcessInfoNew = pi.UID_ProcessInfo
        WHERE (pi.FirstDate IS NOT NULL OR pi.LastDate IS NOT NULL)       -- at least one date exists
          AND (pi.FirstDate IS NULL OR pi.FirstDate < @Cutoff)            -- FirstDate not recent
          AND (pi.LastDate  IS NULL OR pi.LastDate  < @Cutoff)            -- LastDate not recent

        SET @Deleted = @@ROWCOUNT
        SET @Total  += @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            RAISERROR('    ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
        END
    END

    SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
    RAISERROR('  Done: %I64d rows deleted (%d sec)', 0, 1, @Total, @Sec) WITH NOWAIT
END
ELSE PRINT '[4/9] ProcessSubstitute — table not found, skipping.'

-- ────────────────────────────────────────────────────────────────
-- 5. ProcessChain
--    Date column: ThisDate
--    Only delete if ThisDate is non-NULL and < cutoff
-- ────────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessChain', 'U') IS NOT NULL
BEGIN
    PRINT ''
    PRINT '[5/9] ProcessChain...'
    SET @Total = 0; SET @Start = GETDATE(); SET @Deleted = 1

    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessChain
        WHERE ThisDate IS NOT NULL
          AND ThisDate < @Cutoff

        SET @Deleted = @@ROWCOUNT
        SET @Total  += @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            RAISERROR('    ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
        END
    END

    SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
    RAISERROR('  Done: %I64d rows deleted (%d sec)', 0, 1, @Total, @Sec) WITH NOWAIT
END
ELSE PRINT '[5/9] ProcessChain — table not found, skipping.'

-- ────────────────────────────────────────────────────────────────
-- 6. HistoryJob
--    Date column: StartAt
--    Only delete if StartAt is non-NULL and < cutoff
-- ────────────────────────────────────────────────────────────────
IF OBJECT_ID('HistoryJob', 'U') IS NOT NULL
BEGIN
    PRINT ''
    PRINT '[6/9] HistoryJob...'
    SET @Total = 0; SET @Start = GETDATE(); SET @Deleted = 1

    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM HistoryJob
        WHERE StartAt IS NOT NULL
          AND StartAt < @Cutoff

        SET @Deleted = @@ROWCOUNT
        SET @Total  += @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            RAISERROR('    ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
        END
    END

    SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
    RAISERROR('  Done: %I64d rows deleted (%d sec)', 0, 1, @Total, @Sec) WITH NOWAIT
END
ELSE PRINT '[6/9] HistoryJob — table not found, skipping.'

-- ────────────────────────────────────────────────────────────────
-- 7. HistoryChain
--    Date columns: FirstDate, LastDate (both nullable)
--    Delete only if at least one date exists AND all existing
--    dates are older than cutoff. Keep all-NULL rows.
-- ────────────────────────────────────────────────────────────────
IF OBJECT_ID('HistoryChain', 'U') IS NOT NULL
BEGIN
    PRINT ''
    PRINT '[7/9] HistoryChain...'
    SET @Total = 0; SET @Start = GETDATE(); SET @Deleted = 1

    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM HistoryChain
        WHERE (FirstDate IS NOT NULL OR LastDate IS NOT NULL)
          AND (FirstDate IS NULL OR FirstDate < @Cutoff)
          AND (LastDate  IS NULL OR LastDate  < @Cutoff)

        SET @Deleted = @@ROWCOUNT
        SET @Total  += @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            RAISERROR('    ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
        END
    END

    SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
    RAISERROR('  Done: %I64d rows deleted (%d sec)', 0, 1, @Total, @Sec) WITH NOWAIT
END
ELSE PRINT '[7/9] HistoryChain — table not found, skipping.'

-- ────────────────────────────────────────────────────────────────
-- 8. ProcessInfo  (parent of ProcessSubstitute — delete after child)
--    Date columns: FirstDate, LastDate (both nullable)
--    Same logic as HistoryChain
-- ────────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessInfo', 'U') IS NOT NULL
BEGIN
    PRINT ''
    PRINT '[8/9] ProcessInfo...'
    SET @Total = 0; SET @Start = GETDATE(); SET @Deleted = 1

    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessInfo
        WHERE (FirstDate IS NOT NULL OR LastDate IS NOT NULL)
          AND (FirstDate IS NULL OR FirstDate < @Cutoff)
          AND (LastDate  IS NULL OR LastDate  < @Cutoff)

        SET @Deleted = @@ROWCOUNT
        SET @Total  += @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            RAISERROR('    ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
        END
    END

    SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
    RAISERROR('  Done: %I64d rows deleted (%d sec)', 0, 1, @Total, @Sec) WITH NOWAIT
END
ELSE PRINT '[8/9] ProcessInfo — table not found, skipping.'

-- ────────────────────────────────────────────────────────────────
-- 9. ProcessGroup  (root parent — delete last)
--    Date columns: FirstDate, LastDate, ExportDate
--    Delete only if at least one date exists AND all existing
--    dates are older than cutoff. Keep all-NULL rows.
-- ────────────────────────────────────────────────────────────────
IF OBJECT_ID('ProcessGroup', 'U') IS NOT NULL
BEGIN
    PRINT ''
    PRINT '[9/9] ProcessGroup...'
    SET @Total = 0; SET @Start = GETDATE(); SET @Deleted = 1

    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessGroup
        WHERE (FirstDate IS NOT NULL OR LastDate IS NOT NULL OR ExportDate IS NOT NULL)
          AND (FirstDate   IS NULL OR FirstDate   < @Cutoff)
          AND (LastDate    IS NULL OR LastDate    < @Cutoff)
          AND (ExportDate  IS NULL OR ExportDate  < @Cutoff)

        SET @Deleted = @@ROWCOUNT
        SET @Total  += @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            RAISERROR('    ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
        END
    END

    SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
    RAISERROR('  Done: %I64d rows deleted (%d sec)', 0, 1, @Total, @Sec) WITH NOWAIT
END
ELSE PRINT '[9/9] ProcessGroup — table not found, skipping.'

-- ================================================================
-- POST-FLIGHT: Show remaining row counts
-- ================================================================
PRINT ''
PRINT '# POST-FLIGHT — Estimated rows remaining'
PRINT '------------------------------------------------------------'

DECLARE @tbl2 NVARCHAR(128), @est2 BIGINT
DECLARE cur_post CURSOR LOCAL FAST_FORWARD FOR
    SELECT name FROM @tables ORDER BY seq
OPEN cur_post
FETCH NEXT FROM cur_post INTO @tbl2
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @est2 = SUM(row_count)
    FROM sys.dm_db_partition_stats
    WHERE object_id = OBJECT_ID(@tbl2) AND index_id IN (0,1)

    RAISERROR('  %-25s ~%I64d rows', 0, 1, @tbl2, @est2) WITH NOWAIT
    FETCH NEXT FROM cur_post INTO @tbl2
END
CLOSE cur_post; DEALLOCATE cur_post

-- ================================================================
-- POST-CLEANUP: Log file size check
-- ================================================================
PRINT ''
PRINT '# LOG FILE SIZE (after cleanup)'
SELECT
    name            AS LogicalName,
    size / 128      AS SizeMB,
    FILEPROPERTY(name, 'SpaceUsed') / 128 AS UsedMB
FROM sys.database_files WHERE type_desc = 'LOG'

-- ================================================================
-- RESTORE ORIGINAL RECOVERY MODEL
-- ================================================================
IF @OrigRecovery <> 'SIMPLE'
BEGIN
    PRINT ''
    PRINT 'Restoring recovery model to ' + @OrigRecovery + '...'
    DECLARE @restoreSql NVARCHAR(200) = 'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + ' SET RECOVERY ' + @OrigRecovery
    EXEC sp_executesql @restoreSql
    PRINT 'Recovery model restored.'
END

DECLARE @TotalSec INT = DATEDIFF(SECOND, @ScriptStart, GETDATE())
PRINT ''
PRINT '============================================================'
RAISERROR(' Total runtime: %d seconds (~%d minutes)', 0, 1, @TotalSec, @TotalSec / 60) WITH NOWAIT
PRINT ' Finished at  : ' + CONVERT(VARCHAR(30), GETDATE(), 120)
PRINT '============================================================'
PRINT ''
PRINT ' IMPORTANT: Take a FULL BACKUP now to restart the log chain!'
PRINT ''
PRINT 'Done.'
GO
