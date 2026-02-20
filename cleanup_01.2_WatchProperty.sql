-- ============================================================================
-- HDB Cleanup — 1/9: WatchProperty (v2 — Optimized)
-- ============================================================================
-- FK child of WatchOperation. MUST run BEFORE cleanup_02_WatchOperation.sql
--
-- OPTIMIZATION: Collects old WatchOperation keys into a temp table ONCE,
-- then batch-deletes WatchProperty using a simple indexed lookup instead
-- of re-evaluating the JOIN on every batch.
--
-- Resumable: rerun safely if interrupted (temp table is rebuilt each run).
-- ============================================================================

USE [OneIMHDB3]          -- << Set your HDB database name
GO
SET NOCOUNT ON
GO

DECLARE @Cutoff    DATETIME = DATEADD(YEAR, -2, GETDATE())
DECLARE @BatchSize INT      = 500000
DECLARE @Deleted   INT      = 1
DECLARE @Total     BIGINT   = 0
DECLARE @Start     DATETIME = GETDATE()

PRINT '============================================================'
PRINT ' WatchProperty (v2 — temp-table optimized)'
PRINT ' Cutoff: ' + CONVERT(VARCHAR(30), @Cutoff, 120)
PRINT '============================================================'

-- Pre-flight count
DECLARE @before BIGINT
SELECT @before = SUM(row_count) FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('WatchProperty') AND index_id IN (0,1)
RAISERROR('  Rows before: ~%I64d', 0, 1, @before) WITH NOWAIT

-- ================================================================
-- STEP 1: Collect old WatchOperation keys into a temp table (one-time)
-- ================================================================
PRINT ''
PRINT '  Building temp table of old WatchOperation keys...'

IF OBJECT_ID('tempdb..#OldWatchOps') IS NOT NULL
    DROP TABLE #OldWatchOps

SELECT UID_DialogWatchOperation
INTO #OldWatchOps
FROM WatchOperation
WHERE OperationDate IS NOT NULL
  AND OperationDate < @Cutoff

DECLARE @keyCount BIGINT = @@ROWCOUNT
RAISERROR('  Found %I64d old WatchOperation keys', 0, 1, @keyCount) WITH NOWAIT

-- Index the temp table for fast lookups
CREATE CLUSTERED INDEX CX_OldWatchOps ON #OldWatchOps (UID_DialogWatchOperation)
PRINT '  Temp table indexed.'
PRINT ''

-- ================================================================
-- STEP 2: Batch-delete WatchProperty using temp table lookup
-- ================================================================
PRINT '  Deleting WatchProperty rows...'

WHILE @Deleted > 0
BEGIN
    DELETE TOP (@BatchSize) wp
    FROM WatchProperty wp
    WHERE EXISTS (
        SELECT 1 FROM #OldWatchOps o
        WHERE o.UID_DialogWatchOperation = wp.UID_DialogWatchOperation
    )

    SET @Deleted = @@ROWCOUNT
    SET @Total  += @Deleted
    IF @Deleted > 0
    BEGIN
        CHECKPOINT
        RAISERROR('    ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
    END
END

-- ================================================================
-- Cleanup
-- ================================================================
DROP TABLE #OldWatchOps

DECLARE @Sec INT = DATEDIFF(SECOND, @Start, GETDATE())
DECLARE @after BIGINT
SELECT @after = SUM(row_count) FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('WatchProperty') AND index_id IN (0,1)

PRINT ''
RAISERROR('  Total deleted : %I64d', 0, 1, @Total) WITH NOWAIT
RAISERROR('  Rows remaining: ~%I64d', 0, 1, @after) WITH NOWAIT
RAISERROR('  Runtime       : %d sec (~%d min)', 0, 1, @Sec, @Sec / 60) WITH NOWAIT
PRINT '============================================================'
PRINT 'WatchProperty — Done.'
GO
