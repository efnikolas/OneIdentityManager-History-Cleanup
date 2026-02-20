-- ============================================================================
-- HDB Cleanup — 8/9: ProcessInfo
-- ============================================================================
-- Parent of ProcessSubstitute. MUST run AFTER cleanup_04_ProcessSubstitute.sql
-- Deletes where ALL existing dates (FirstDate, LastDate) are older than 2 years.
-- Rows with all NULL dates are kept.
-- Rows where ANY date is within 2 years are kept.
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
PRINT ' ProcessInfo'
PRINT ' Cutoff: ' + CONVERT(VARCHAR(30), @Cutoff, 120)
PRINT '============================================================'

DECLARE @before BIGINT
SELECT @before = SUM(row_count) FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('ProcessInfo') AND index_id IN (0,1)
RAISERROR('  Rows before: ~%I64d', 0, 1, @before) WITH NOWAIT
PRINT ''

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
        RAISERROR('  ...%I64d deleted so far', 0, 1, @Total) WITH NOWAIT
    END
END

DECLARE @Sec INT = DATEDIFF(SECOND, @Start, GETDATE())
DECLARE @after BIGINT
SELECT @after = SUM(row_count) FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('ProcessInfo') AND index_id IN (0,1)

PRINT ''
RAISERROR('  Total deleted : %I64d', 0, 1, @Total) WITH NOWAIT
RAISERROR('  Rows remaining: ~%I64d', 0, 1, @after) WITH NOWAIT
RAISERROR('  Runtime       : %d sec (~%d min)', 0, 1, @Sec, @Sec / 60) WITH NOWAIT
PRINT '============================================================'
PRINT 'ProcessInfo — Done.'
GO
