-- ============================================================================
-- HDB Cleanup — 1/9: WatchProperty (v3 — FK disabled for speed)
-- ============================================================================
-- FK child of WatchOperation. MUST run BEFORE cleanup_02_WatchOperation.sql
--
-- OPTIMIZATION: Temporarily disables FK constraints on WatchProperty so
-- SQL Server doesn't validate referential integrity on every single delete.
-- Re-enables them WITH CHECK at the end.
--
-- Resumable: rerun safely if interrupted. FKs re-enabled at the end.
-- If script is cancelled mid-run, re-enable FKs manually:
--   ALTER TABLE WatchProperty WITH CHECK CHECK CONSTRAINT HDB_1248
--   ALTER TABLE WatchProperty WITH CHECK CHECK CONSTRAINT HDB_1251
-- ============================================================================

USE [OneIMHDB3]          -- << Set your HDB database name
GO
SET NOCOUNT ON
GO

DECLARE @Cutoff    DATETIME = DATEADD(YEAR, -2, GETDATE())
DECLARE @BatchSize INT      = 49999
DECLARE @Deleted   INT      = 1
DECLARE @Total     BIGINT   = 0
DECLARE @Start     DATETIME = GETDATE()

PRINT '============================================================'
PRINT ' WatchProperty (v3 — FK disabled for speed)'
PRINT ' Cutoff: ' + CONVERT(VARCHAR(30), @Cutoff, 120)
PRINT '============================================================'

-- Pre-flight count
DECLARE @before BIGINT
SELECT @before = SUM(row_count) FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('WatchProperty') AND index_id IN (0,1)
RAISERROR('  Rows before: ~%I64d', 0, 1, @before) WITH NOWAIT

-- ================================================================
-- STEP 1: Disable FK constraints (big speed win)
-- ================================================================
PRINT ''
PRINT '  Disabling FK constraints on WatchProperty...'
ALTER TABLE WatchProperty NOCHECK CONSTRAINT HDB_1248
ALTER TABLE WatchProperty NOCHECK CONSTRAINT HDB_1251
PRINT '  FKs disabled.'
PRINT ''

-- ================================================================
-- STEP 2: Batch-delete using JOIN (no FK overhead now)
-- ================================================================
PRINT '  Deleting WatchProperty rows...'

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

-- ================================================================
-- STEP 3: Re-enable FK constraints WITH CHECK
-- ================================================================
PRINT ''
PRINT '  Re-enabling FK constraints on WatchProperty...'
ALTER TABLE WatchProperty WITH CHECK CHECK CONSTRAINT HDB_1248
ALTER TABLE WatchProperty WITH CHECK CHECK CONSTRAINT HDB_1251
PRINT '  FKs re-enabled and validated.'

-- ================================================================
-- Report
-- ================================================================
DECLARE @Sec INT = DATEDIFF(SECOND, @Start, GETDATE())
DECLARE @after BIGINT
SELECT @after = SUM(row_count) FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('WatchProperty') AND index_id IN (0,1)

PRINT ''
RAISERROR('  Total deleted : %I64d', 0, 1, @Total) WITH NOWAIT
RAISERROR('  Rows remaining: ~%I64d', 0, 1, @after) WITH NOWAIT
PRINT '  Runtime       : ' + CAST(@Sec AS VARCHAR(20)) + ' sec (~' + CAST(@Sec / 60 AS VARCHAR(20)) + ' min)'
PRINT '============================================================'
PRINT 'WatchProperty — Done.'
GO
