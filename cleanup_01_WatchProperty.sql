-- ============================================================================
-- HDB Cleanup — 1/9: WatchProperty
-- ============================================================================
-- FK child of WatchOperation. MUST run BEFORE cleanup_02_WatchOperation.sql
-- Deletes where parent WatchOperation.OperationDate is older than 2 years.
-- Rows with NULL OperationDate on the parent are kept.
-- Resumable: rerun safely if interrupted.
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
PRINT ' WatchProperty (via WatchOperation.OperationDate)'
PRINT ' Cutoff: ' + CONVERT(VARCHAR(30), @Cutoff, 120)
PRINT '============================================================'

-- Pre-flight count
DECLARE @before BIGINT
SELECT @before = SUM(row_count) FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('WatchProperty') AND index_id IN (0,1)
RAISERROR('  Rows before: ~%I64d', 0, 1, @before) WITH NOWAIT
PRINT ''

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
        RAISERROR('  ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
    END
END

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
