-- ============================================================================
-- HDB Cleanup — 6/9: HistoryJob
-- ============================================================================
-- Independent table — no FK dependencies. Can run in parallel with others.
-- Deletes where StartAt is older than 2 years.
-- Rows with NULL StartAt are kept.
-- Resumable: rerun safely if interrupted.
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
PRINT ' HistoryJob'
PRINT ' Cutoff: ' + CONVERT(VARCHAR(30), @Cutoff, 120)
PRINT '============================================================'

DECLARE @before BIGINT
SELECT @before = SUM(row_count) FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('HistoryJob') AND index_id IN (0,1)
RAISERROR('  Rows before: ~%I64d', 0, 1, @before) WITH NOWAIT
PRINT ''

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
        RAISERROR('  ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
    END
END

DECLARE @Sec INT = DATEDIFF(SECOND, @Start, GETDATE())
DECLARE @after BIGINT
SELECT @after = SUM(row_count) FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('HistoryJob') AND index_id IN (0,1)

PRINT ''
RAISERROR('  Total deleted : %I64d', 0, 1, @Total) WITH NOWAIT
RAISERROR('  Rows remaining: ~%I64d', 0, 1, @after) WITH NOWAIT
PRINT '  Runtime       : ' + CAST(@Sec AS VARCHAR(20)) + ' sec (~' + CAST(@Sec / 60 AS VARCHAR(20)) + ' min)'
PRINT '============================================================'
PRINT 'HistoryJob — Done.'
GO
