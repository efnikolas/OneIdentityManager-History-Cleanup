-- ============================================================
-- One Identity Manager -- HDB Cleanup
-- ============================================================
-- Deletes all data older than @CutoffDate from every HDB table.
-- Children deleted before parents to respect foreign keys.
-- Batched in loops of @BatchSize to avoid giant transactions.
-- Cleans up any leftover temp tables/indexes from prior runs.
--
-- BACKUP YOUR DATABASE BEFORE RUNNING THIS SCRIPT
-- ============================================================

USE [OneIMHDB3]   -- change to your HDB name
GO
SET NOCOUNT ON
GO

DECLARE @CutoffDate DATETIME = DATEADD(YEAR, -2, GETDATE())
DECLARE @BatchSize  INT      = 100000
DECLARE @dbName    NVARCHAR(128) = DB_NAME()
DECLARE @cutoffStr VARCHAR(30)   = CONVERT(VARCHAR, @CutoffDate, 120)
DECLARE @sec       INT

RAISERROR('================================================', 0, 1) WITH NOWAIT
RAISERROR('HDB Cleanup - %s', 0, 1, @dbName) WITH NOWAIT
RAISERROR('Deleting everything before: %s', 0, 1, @cutoffStr) WITH NOWAIT
RAISERROR('Batch size: %d', 0, 1, @BatchSize) WITH NOWAIT
RAISERROR('================================================', 0, 1) WITH NOWAIT

-- ============================================================
-- CLEANUP: Remove leftovers from previous failed runs
-- ============================================================
RAISERROR('Cleaning up leftovers from previous runs...', 0, 1) WITH NOWAIT

-- Drop any stale #Keep_* temp tables
IF OBJECT_ID('tempdb..#Keep_WatchProperty')    IS NOT NULL DROP TABLE #Keep_WatchProperty
IF OBJECT_ID('tempdb..#Keep_WatchOperation')   IS NOT NULL DROP TABLE #Keep_WatchOperation
IF OBJECT_ID('tempdb..#Keep_ProcessStep')      IS NOT NULL DROP TABLE #Keep_ProcessStep
IF OBJECT_ID('tempdb..#Keep_ProcessSubstitute')IS NOT NULL DROP TABLE #Keep_ProcessSubstitute
IF OBJECT_ID('tempdb..#Keep_ProcessChain')     IS NOT NULL DROP TABLE #Keep_ProcessChain
IF OBJECT_ID('tempdb..#Keep_HistoryJob')       IS NOT NULL DROP TABLE #Keep_HistoryJob
IF OBJECT_ID('tempdb..#Keep_HistoryChain')     IS NOT NULL DROP TABLE #Keep_HistoryChain
IF OBJECT_ID('tempdb..#Keep_ProcessInfo')      IS NOT NULL DROP TABLE #Keep_ProcessInfo
IF OBJECT_ID('tempdb..#Keep_ProcessGroup')     IS NOT NULL DROP TABLE #Keep_ProcessGroup
IF OBJECT_ID('tempdb..#FKDefinitions')         IS NOT NULL DROP TABLE #FKDefinitions

-- Drop any IX_Cleanup_* indexes left behind
DECLARE @dropIdx NVARCHAR(500)
DECLARE idx_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT 'DROP INDEX [' + i.name + '] ON [' + OBJECT_NAME(i.object_id) + ']'
    FROM sys.indexes i
    WHERE i.name LIKE 'IX_Cleanup_%'
OPEN idx_cur
FETCH NEXT FROM idx_cur INTO @dropIdx
WHILE @@FETCH_STATUS = 0
BEGIN
    RAISERROR('  %s', 0, 1, @dropIdx) WITH NOWAIT
    EXEC sp_executesql @dropIdx
    FETCH NEXT FROM idx_cur INTO @dropIdx
END
CLOSE idx_cur; DEALLOCATE idx_cur

RAISERROR('Cleanup done.', 0, 1) WITH NOWAIT
RAISERROR(' ', 0, 1) WITH NOWAIT

-- ============================================================
-- DELETE in batches per table
-- ============================================================
DECLARE @rc INT = 1
DECLARE @total BIGINT
DECLARE @st DATETIME

-- 1. WatchProperty (child of WatchOperation via FK join)
RAISERROR('WatchProperty...', 0, 1) WITH NOWAIT
SET @total = 0; SET @st = GETDATE(); SET @rc = 1
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) wp
    FROM WatchProperty wp
    INNER JOIN WatchOperation wo ON wp.UID_DialogWatchOperation = wo.UID_DialogWatchOperation
    WHERE wo.OperationDate < @CutoffDate
    SET @rc = @@ROWCOUNT
    SET @total = @total + @rc
    IF @rc > 0
        RAISERROR('  %I64d so far...', 0, 1, @total) WITH NOWAIT
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  %I64d rows deleted (%ds)', 0, 1, @total, @sec) WITH NOWAIT

-- 2. WatchOperation
RAISERROR('WatchOperation...', 0, 1) WITH NOWAIT
SET @total = 0; SET @st = GETDATE(); SET @rc = 1
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM WatchOperation WHERE OperationDate < @CutoffDate
    SET @rc = @@ROWCOUNT
    SET @total = @total + @rc
    IF @rc > 0
        RAISERROR('  %I64d so far...', 0, 1, @total) WITH NOWAIT
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  %I64d rows deleted (%ds)', 0, 1, @total, @sec) WITH NOWAIT

-- 3. ProcessStep
RAISERROR('ProcessStep...', 0, 1) WITH NOWAIT
SET @total = 0; SET @st = GETDATE(); SET @rc = 1
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM ProcessStep WHERE ThisDate < @CutoffDate
    SET @rc = @@ROWCOUNT
    SET @total = @total + @rc
    IF @rc > 0
        RAISERROR('  %I64d so far...', 0, 1, @total) WITH NOWAIT
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  %I64d rows deleted (%ds)', 0, 1, @total, @sec) WITH NOWAIT

-- 4. ProcessSubstitute (child of ProcessInfo via FK join)
RAISERROR('ProcessSubstitute...', 0, 1) WITH NOWAIT
SET @total = 0; SET @st = GETDATE(); SET @rc = 1
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) ps
    FROM ProcessSubstitute ps
    INNER JOIN ProcessInfo pi ON ps.UID_ProcessInfoNew = pi.UID_ProcessInfo
    WHERE COALESCE(pi.FirstDate, pi.LastDate) < @CutoffDate
    SET @rc = @@ROWCOUNT
    SET @total = @total + @rc
    IF @rc > 0
        RAISERROR('  %I64d so far...', 0, 1, @total) WITH NOWAIT
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  %I64d rows deleted (%ds)', 0, 1, @total, @sec) WITH NOWAIT

-- 5. ProcessChain
RAISERROR('ProcessChain...', 0, 1) WITH NOWAIT
SET @total = 0; SET @st = GETDATE(); SET @rc = 1
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM ProcessChain WHERE ThisDate < @CutoffDate
    SET @rc = @@ROWCOUNT
    SET @total = @total + @rc
    IF @rc > 0
        RAISERROR('  %I64d so far...', 0, 1, @total) WITH NOWAIT
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  %I64d rows deleted (%ds)', 0, 1, @total, @sec) WITH NOWAIT

-- 6. HistoryJob
RAISERROR('HistoryJob...', 0, 1) WITH NOWAIT
SET @total = 0; SET @st = GETDATE(); SET @rc = 1
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM HistoryJob WHERE StartAt < @CutoffDate
    SET @rc = @@ROWCOUNT
    SET @total = @total + @rc
    IF @rc > 0
        RAISERROR('  %I64d so far...', 0, 1, @total) WITH NOWAIT
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  %I64d rows deleted (%ds)', 0, 1, @total, @sec) WITH NOWAIT

-- 7. HistoryChain
RAISERROR('HistoryChain...', 0, 1) WITH NOWAIT
SET @total = 0; SET @st = GETDATE(); SET @rc = 1
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM HistoryChain WHERE COALESCE(FirstDate, LastDate) < @CutoffDate
    SET @rc = @@ROWCOUNT
    SET @total = @total + @rc
    IF @rc > 0
        RAISERROR('  %I64d so far...', 0, 1, @total) WITH NOWAIT
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  %I64d rows deleted (%ds)', 0, 1, @total, @sec) WITH NOWAIT

-- 8. ProcessInfo
RAISERROR('ProcessInfo...', 0, 1) WITH NOWAIT
SET @total = 0; SET @st = GETDATE(); SET @rc = 1
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM ProcessInfo WHERE COALESCE(FirstDate, LastDate) < @CutoffDate
    SET @rc = @@ROWCOUNT
    SET @total = @total + @rc
    IF @rc > 0
        RAISERROR('  %I64d so far...', 0, 1, @total) WITH NOWAIT
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  %I64d rows deleted (%ds)', 0, 1, @total, @sec) WITH NOWAIT

-- 9. ProcessGroup
RAISERROR('ProcessGroup...', 0, 1) WITH NOWAIT
SET @total = 0; SET @st = GETDATE(); SET @rc = 1
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM ProcessGroup WHERE COALESCE(FirstDate, LastDate, ExportDate) < @CutoffDate
    SET @rc = @@ROWCOUNT
    SET @total = @total + @rc
    IF @rc > 0
        RAISERROR('  %I64d so far...', 0, 1, @total) WITH NOWAIT
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  %I64d rows deleted (%ds)', 0, 1, @total, @sec) WITH NOWAIT

RAISERROR('================================================', 0, 1) WITH NOWAIT
RAISERROR('Done.', 0, 1) WITH NOWAIT
GO
