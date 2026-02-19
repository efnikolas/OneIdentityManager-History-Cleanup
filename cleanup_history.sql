-- ============================================================
-- One Identity Manager -- History Database (HDB) Cleanup
-- ============================================================
-- Purges archived data older than @CutoffDate from the OIM
-- History Database (TimeTrace).
--
-- Tables (from OIM 9.3 Data Archiving Administration Guide):
--   Raw:        RawJobHistory, RawProcess, RawProcessChain,
--               RawProcessGroup, RawProcessStep,
--               RawProcessSubstitute, RawWatchOperation,
--               RawWatchProperty
--   Aggregated: HistoryChain, HistoryJob, ProcessChain,
--               ProcessGroup, ProcessInfo, ProcessStep,
--               ProcessSubstitute, WatchOperation,
--               WatchProperty
--   Metadata:   SourceColumn, SourceDatabase, SourceTable
--               (never deleted)
--
-- BACKUP YOUR DATABASE BEFORE RUNNING THIS SCRIPT
-- ============================================================

USE [OneIMHDB3]   -- << CHANGE THIS to your HDB name
GO

SET NOCOUNT ON
GO

-- ============================================================
-- CONFIGURATION
-- ============================================================
DECLARE @CutoffDate  DATETIME = DATEADD(YEAR, -2, GETDATE())
DECLARE @BatchSize   INT      = 50000
DECLARE @WhatIf      BIT      = 0       -- 1 = preview only, no deletes
DECLARE @BatchDelay  VARCHAR(12) = '00:00:00'  -- pause between batches

PRINT '================================================'
PRINT 'OIM History Database Cleanup'
PRINT 'Database:    ' + DB_NAME()
PRINT 'Cutoff date: ' + CONVERT(VARCHAR(20), @CutoffDate, 120)
PRINT 'Batch size:  ' + CAST(@BatchSize AS VARCHAR)
PRINT 'WhatIf:      ' + CAST(@WhatIf AS VARCHAR)
PRINT '================================================'
PRINT ''

-- ============================================================
-- HELPER VARIABLES
-- ============================================================
DECLARE @Deleted INT
DECLARE @Total   BIGINT
DECLARE @Start   DATETIME
DECLARE @Sec     INT
DECLARE @Rate    BIGINT

-- ============================================================
-- PRE-FLIGHT: Show estimated row counts per table
-- (uses partition stats -- instant, no table scans)
-- ============================================================
PRINT '# PRE-FLIGHT (estimated row counts)'
PRINT '------------------------------------------------'

DECLARE @PreTbl NVARCHAR(128)
DECLARE @PreCnt BIGINT
DECLARE pre_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT name FROM sys.tables
    WHERE name IN (
        'RawWatchProperty','RawWatchOperation','RawProcessStep',
        'RawProcessSubstitute','RawProcessChain','RawProcess',
        'RawProcessGroup','RawJobHistory',
        'WatchProperty','WatchOperation','ProcessStep',
        'ProcessSubstitute','ProcessChain','HistoryJob',
        'HistoryChain','ProcessInfo','ProcessGroup'
    )
    ORDER BY name

OPEN pre_cur
FETCH NEXT FROM pre_cur INTO @PreTbl
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @PreCnt = SUM(p.row_count)
    FROM sys.dm_db_partition_stats p
    WHERE p.object_id = OBJECT_ID(@PreTbl) AND p.index_id IN (0,1)

    PRINT '  ' + @PreTbl + ': ~' + CAST(ISNULL(@PreCnt,0) AS VARCHAR) + ' rows'
    FETCH NEXT FROM pre_cur INTO @PreTbl
END
CLOSE pre_cur
DEALLOCATE pre_cur
PRINT ''

-- ============================================================
-- WHATIF -- stop here if preview mode
-- ============================================================
IF @WhatIf = 1
BEGIN
    PRINT 'WhatIf mode -- no data deleted.'
    RETURN
END

PRINT '# CLEANUP'
PRINT '================================================'

-- ============================================================
-- DELETE in FK-safe order (children before parents).
-- Four tables have no date column and are deleted via
-- JOIN to their parent table.
--
-- Order:
--   1. RawWatchProperty     (FK join -> RawWatchOperation)
--   2. RawWatchOperation    (OperationDate)
--   3. RawProcessStep       (XDateInserted)
--   4. RawProcessSubstitute (FK join -> RawProcess)
--   5. RawProcessChain      (XDateInserted)
--   6. RawProcess           (XDateInserted)
--   7. RawProcessGroup      (ExportDate)
--   8. RawJobHistory        (StartAt)
--   9. WatchProperty        (FK join -> WatchOperation)
--  10. WatchOperation       (OperationDate)
--  11. ProcessStep          (ThisDate)
--  12. ProcessSubstitute    (FK join -> ProcessInfo)
--  13. ProcessChain         (ThisDate)
--  14. HistoryJob           (StartAt)
--  15. HistoryChain         (COALESCE(FirstDate, LastDate))
--  16. ProcessInfo          (COALESCE(FirstDate, LastDate))
--  17. ProcessGroup         (COALESCE(FirstDate, LastDate, ExportDate))
-- ============================================================

-- 1. RawWatchProperty (FK join -> RawWatchOperation.OperationDate)
IF OBJECT_ID('RawWatchProperty','U') IS NOT NULL
AND OBJECT_ID('RawWatchOperation','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning RawWatchProperty (via RawWatchOperation.OperationDate)...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) child
        FROM RawWatchProperty child
        INNER JOIN RawWatchOperation parent
            ON child.UID_DialogWatchOperation = parent.UID_DialogWatchOperation
        WHERE parent.OperationDate < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 2. RawWatchOperation (OperationDate)
IF OBJECT_ID('RawWatchOperation','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning RawWatchOperation...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM RawWatchOperation WHERE OperationDate < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 3. RawProcessStep (XDateInserted)
IF OBJECT_ID('RawProcessStep','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning RawProcessStep...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM RawProcessStep WHERE XDateInserted < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 4. RawProcessSubstitute (FK join -> RawProcess.XDateInserted)
IF OBJECT_ID('RawProcessSubstitute','U') IS NOT NULL
AND OBJECT_ID('RawProcess','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning RawProcessSubstitute (via RawProcess.XDateInserted)...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) child
        FROM RawProcessSubstitute child
        INNER JOIN RawProcess parent ON child.GenProcIDNew = parent.GenProcID
        WHERE parent.XDateInserted < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 5. RawProcessChain (XDateInserted)
IF OBJECT_ID('RawProcessChain','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning RawProcessChain...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM RawProcessChain WHERE XDateInserted < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 6. RawProcess (XDateInserted)
IF OBJECT_ID('RawProcess','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning RawProcess...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM RawProcess WHERE XDateInserted < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 7. RawProcessGroup (ExportDate)
IF OBJECT_ID('RawProcessGroup','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning RawProcessGroup...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM RawProcessGroup WHERE ExportDate < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 8. RawJobHistory (StartAt)
IF OBJECT_ID('RawJobHistory','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning RawJobHistory...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM RawJobHistory WHERE StartAt < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 9. WatchProperty (FK join -> WatchOperation.OperationDate)
IF OBJECT_ID('WatchProperty','U') IS NOT NULL
AND OBJECT_ID('WatchOperation','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning WatchProperty (via WatchOperation.OperationDate)...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) child
        FROM WatchProperty child
        INNER JOIN WatchOperation parent
            ON child.UID_DialogWatchOperation = parent.UID_DialogWatchOperation
        WHERE parent.OperationDate < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 10. WatchOperation (OperationDate)
IF OBJECT_ID('WatchOperation','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning WatchOperation...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM WatchOperation WHERE OperationDate < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 11. ProcessStep (ThisDate)
IF OBJECT_ID('ProcessStep','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessStep...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessStep WHERE ThisDate < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 12. ProcessSubstitute (FK join -> ProcessInfo, COALESCE(FirstDate, LastDate))
IF OBJECT_ID('ProcessSubstitute','U') IS NOT NULL
AND OBJECT_ID('ProcessInfo','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessSubstitute (via ProcessInfo.FirstDate)...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) child
        FROM ProcessSubstitute child
        INNER JOIN ProcessInfo parent ON child.UID_ProcessInfoNew = parent.UID_ProcessInfo
        WHERE COALESCE(parent.FirstDate, parent.LastDate) < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 13. ProcessChain (ThisDate)
IF OBJECT_ID('ProcessChain','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessChain...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessChain WHERE ThisDate < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 14. HistoryJob (StartAt)
IF OBJECT_ID('HistoryJob','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning HistoryJob...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM HistoryJob WHERE StartAt < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 15. HistoryChain (COALESCE(FirstDate, LastDate))
IF OBJECT_ID('HistoryChain','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning HistoryChain...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM HistoryChain WHERE COALESCE(FirstDate, LastDate) < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 16. ProcessInfo (COALESCE(FirstDate, LastDate))
IF OBJECT_ID('ProcessInfo','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessInfo...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessInfo WHERE COALESCE(FirstDate, LastDate) < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END
PRINT ''

-- 17. ProcessGroup (COALESCE(FirstDate, LastDate, ExportDate))
IF OBJECT_ID('ProcessGroup','U') IS NOT NULL
BEGIN
    PRINT 'Cleaning ProcessGroup...'
    SET @Total = 0  SET @Start = GETDATE()  SET @Deleted = 1
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM ProcessGroup
        WHERE COALESCE(FirstDate, LastDate, ExportDate) < @CutoffDate
        SET @Deleted = @@ROWCOUNT
        SET @Total = @Total + @Deleted
        IF @Deleted > 0
        BEGIN
            CHECKPOINT
            SET @Sec = DATEDIFF(SECOND, @Start, GETDATE())
            SET @Rate = CASE WHEN @Sec > 0 THEN @Total / @Sec ELSE 0 END
            RAISERROR('  batch %d | total %I64d | %ds | ~%I64d rows/sec', 0, 1, @Deleted, @Total, @Sec, @Rate) WITH NOWAIT
            IF @BatchDelay <> '00:00:00' WAITFOR DELAY @BatchDelay
        END
    END
    RAISERROR('  Done: %I64d rows removed.', 0, 1, @Total) WITH NOWAIT
END

-- ============================================================
-- POST-CLEANUP SUMMARY
-- ============================================================
PRINT ''
PRINT '================================================'
PRINT '# POST-CLEANUP (estimated row counts)'
PRINT '------------------------------------------------'

DECLARE @PostTbl NVARCHAR(128)
DECLARE @PostCnt BIGINT
DECLARE post_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT name FROM sys.tables
    WHERE name IN (
        'RawWatchProperty','RawWatchOperation','RawProcessStep',
        'RawProcessSubstitute','RawProcessChain','RawProcess',
        'RawProcessGroup','RawJobHistory',
        'WatchProperty','WatchOperation','ProcessStep',
        'ProcessSubstitute','ProcessChain','HistoryJob',
        'HistoryChain','ProcessInfo','ProcessGroup'
    )
    ORDER BY name

OPEN post_cur
FETCH NEXT FROM post_cur INTO @PostTbl
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @PostCnt = SUM(p.row_count)
    FROM sys.dm_db_partition_stats p
    WHERE p.object_id = OBJECT_ID(@PostTbl) AND p.index_id IN (0,1)

    PRINT '  ' + @PostTbl + ': ~' + CAST(ISNULL(@PostCnt,0) AS VARCHAR) + ' rows'
    FETCH NEXT FROM post_cur INTO @PostTbl
END
CLOSE post_cur
DEALLOCATE post_cur

PRINT ''
PRINT 'Recommended post-cleanup steps:'
PRINT '  1. EXEC sp_updatestats'
PRINT '  2. ALTER INDEX ALL ON <table> REBUILD (maintenance window)'
PRINT '  3. Back up transaction log if using FULL recovery model'
PRINT '================================================'
PRINT 'Cleanup complete.'
PRINT '================================================'
GO
