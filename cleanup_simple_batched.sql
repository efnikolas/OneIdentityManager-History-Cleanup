-- ============================================================================
-- OneIM HDB Cleanup — High Performance Batched Deletes (SIMPLE Recovery)
-- ============================================================================
-- 1. Cleans up orphaned temp tables and indexes from previous runs.
-- 2. Creates targeted temporary indexes to prevent slow Full Table Scans.
-- 3. Estimates row counts pre-flight.
-- 4. Deletes in loops of @BatchSize with explicit CHECKPOINTs.
-- 5. Drops the temporary indexes so they don't consume permanent space.
-- 6. Estimates row counts post-flight and calculates total runtime.
--
-- BACKUP YOUR DATABASE BEFORE RUNNING THIS SCRIPT
-- ============================================================================

USE [OneIMHDB3]; -- CHANGE THIS TO YOUR ACTUAL HDB NAME
GO
SET NOCOUNT ON;
GO

DECLARE @CutoffDate DATETIME = DATEADD(YEAR, -2, GETDATE());
DECLARE @BatchSize  INT      = 100000; -- Lowered slightly for faster index updates during deletes
DECLARE @rc INT, @total BIGINT, @st DATETIME, @sec INT;
DECLARE @scriptStart DATETIME = GETDATE();

PRINT '================================================';
PRINT 'HDB High-Performance Cleanup (SIMPLE Recovery)';
PRINT 'Database: ' + DB_NAME();
PRINT 'Cutoff:   ' + CONVERT(VARCHAR, @CutoffDate, 120);
PRINT '================================================';
PRINT '';

-- ────────────────────────────────────────────────────────────
-- 1. DEFENSIVE CLEANUP: Remove leftovers from previous runs
-- ────────────────────────────────────────────────────────────
RAISERROR('Cleaning up leftovers from previous runs...', 0, 1) WITH NOWAIT;

IF OBJECT_ID('tempdb..#Keep_WatchProperty')    IS NOT NULL DROP TABLE #Keep_WatchProperty;
IF OBJECT_ID('tempdb..#Keep_WatchOperation')   IS NOT NULL DROP TABLE #Keep_WatchOperation;
IF OBJECT_ID('tempdb..#Keep_ProcessStep')      IS NOT NULL DROP TABLE #Keep_ProcessStep;
IF OBJECT_ID('tempdb..#Keep_ProcessSubstitute')IS NOT NULL DROP TABLE #Keep_ProcessSubstitute;
IF OBJECT_ID('tempdb..#Keep_ProcessChain')     IS NOT NULL DROP TABLE #Keep_ProcessChain;
IF OBJECT_ID('tempdb..#Keep_HistoryJob')       IS NOT NULL DROP TABLE #Keep_HistoryJob;
IF OBJECT_ID('tempdb..#Keep_HistoryChain')     IS NOT NULL DROP TABLE #Keep_HistoryChain;
IF OBJECT_ID('tempdb..#Keep_ProcessInfo')      IS NOT NULL DROP TABLE #Keep_ProcessInfo;
IF OBJECT_ID('tempdb..#Keep_ProcessGroup')     IS NOT NULL DROP TABLE #Keep_ProcessGroup;
IF OBJECT_ID('tempdb..#FKDefinitions')         IS NOT NULL DROP TABLE #FKDefinitions;

-- Drop any existing cleanup indexes before we recreate them
DECLARE @dropIdx NVARCHAR(500);
DECLARE idx_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT 'DROP INDEX [' + i.name + '] ON [' + OBJECT_NAME(i.object_id) + ']'
    FROM sys.indexes i
    WHERE i.name LIKE 'IX_Cleanup_%';
OPEN idx_cur;
FETCH NEXT FROM idx_cur INTO @dropIdx;
WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC sp_executesql @dropIdx;
    FETCH NEXT FROM idx_cur INTO @dropIdx;
END
CLOSE idx_cur; DEALLOCATE idx_cur;

-- ────────────────────────────────────────────────────────────
-- 2. PERFORMANCE TUNING: Create Temporary Cleanup Indexes
-- ────────────────────────────────────────────────────────────
RAISERROR('Creating temporary performance indexes (this may take a moment)...', 0, 1) WITH NOWAIT;

-- FK Indexes
CREATE NONCLUSTERED INDEX [IX_Cleanup_WatchProperty_FK] ON [WatchProperty] ([UID_DialogWatchOperation]);
CREATE NONCLUSTERED INDEX [IX_Cleanup_ProcessSubstitute_FK] ON [ProcessSubstitute] ([UID_ProcessInfoNew]);
-- Date Indexes
CREATE NONCLUSTERED INDEX [IX_Cleanup_WatchOperation_Date] ON [WatchOperation] ([OperationDate]);
CREATE NONCLUSTERED INDEX [IX_Cleanup_ProcessStep_Date] ON [ProcessStep] ([ThisDate]);
CREATE NONCLUSTERED INDEX [IX_Cleanup_ProcessChain_Date] ON [ProcessChain] ([ThisDate]);
CREATE NONCLUSTERED INDEX [IX_Cleanup_HistoryJob_Date] ON [HistoryJob] ([StartAt]);
CREATE NONCLUSTERED INDEX [IX_Cleanup_HistoryChain_Date] ON [HistoryChain] ([FirstDate], [LastDate]);
CREATE NONCLUSTERED INDEX [IX_Cleanup_ProcessInfo_Date] ON [ProcessInfo] ([FirstDate], [LastDate]);
CREATE NONCLUSTERED INDEX [IX_Cleanup_ProcessGroup_Date] ON [ProcessGroup] ([FirstDate], [LastDate], [ExportDate]);

RAISERROR('Indexes created successfully.', 0, 1) WITH NOWAIT;

-- ────────────────────────────────────────────────────────────
-- 3. PRE-FLIGHT: Estimated Row Counts
-- ────────────────────────────────────────────────────────────
PRINT '';
PRINT '# PRE-FLIGHT (estimated rows per table)';
PRINT '------------------------------------------------';
DECLARE @tbl NVARCHAR(128), @est BIGINT;
DECLARE @tables TABLE (name NVARCHAR(128));
INSERT @tables VALUES
  ('WatchProperty'),('WatchOperation'),('ProcessStep'),
  ('ProcessSubstitute'),('ProcessChain'),('HistoryJob'),
  ('HistoryChain'),('ProcessInfo'),('ProcessGroup');

DECLARE pf CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM @tables;
OPEN pf;
FETCH NEXT FROM pf INTO @tbl;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @est = SUM(row_count) FROM sys.dm_db_partition_stats
    WHERE object_id = OBJECT_ID(@tbl) AND index_id IN (0,1);
    RAISERROR('  %s: ~%I64d rows', 0, 1, @tbl, @est) WITH NOWAIT;
    FETCH NEXT FROM pf INTO @tbl;
END
CLOSE pf; DEALLOCATE pf;

PRINT '';
PRINT '# CLEANUP EXECUTION';
PRINT '================================================';

-- ────────────────────────────────────────────────────────────
-- 4. BATCHED DELETES (Child tables first, respecting FKs)
-- ────────────────────────────────────────────────────────────

-- 1. WatchProperty (FK child of WatchOperation)
PRINT 'WatchProperty...';
SET @total = 0; SET @st = GETDATE(); SET @rc = 1;
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) wp
    FROM WatchProperty wp
    INNER JOIN WatchOperation wo ON wp.UID_DialogWatchOperation = wo.UID_DialogWatchOperation
    WHERE wo.OperationDate < @CutoffDate;
    SET @rc = @@ROWCOUNT; SET @total += @rc;
    IF @rc > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @total) WITH NOWAIT; END
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE());
RAISERROR('  Total: %I64d (%ds)', 0, 1, @total, @sec) WITH NOWAIT;

-- 2. WatchOperation
PRINT 'WatchOperation...';
SET @total = 0; SET @st = GETDATE(); SET @rc = 1;
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM WatchOperation WHERE OperationDate < @CutoffDate;
    SET @rc = @@ROWCOUNT; SET @total += @rc;
    IF @rc > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @total) WITH NOWAIT; END
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE());
RAISERROR('  Total: %I64d (%ds)', 0, 1, @total, @sec) WITH NOWAIT;

-- 3. ProcessStep
PRINT 'ProcessStep...';
SET @total = 0; SET @st = GETDATE(); SET @rc = 1;
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM ProcessStep WHERE ThisDate < @CutoffDate;
    SET @rc = @@ROWCOUNT; SET @total += @rc;
    IF @rc > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @total) WITH NOWAIT; END
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE());
RAISERROR('  Total: %I64d (%ds)', 0, 1, @total, @sec) WITH NOWAIT;

-- 4. ProcessSubstitute (FK child of ProcessInfo)
PRINT 'ProcessSubstitute...';
SET @total = 0; SET @st = GETDATE(); SET @rc = 1;
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) ps
    FROM ProcessSubstitute ps
    INNER JOIN ProcessInfo pi ON ps.UID_ProcessInfoNew = pi.UID_ProcessInfo
    WHERE COALESCE(pi.FirstDate, pi.LastDate) < @CutoffDate;
    SET @rc = @@ROWCOUNT; SET @total += @rc;
    IF @rc > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @total) WITH NOWAIT; END
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE());
RAISERROR('  Total: %I64d (%ds)', 0, 1, @total, @sec) WITH NOWAIT;

-- 5. ProcessChain
PRINT 'ProcessChain...';
SET @total = 0; SET @st = GETDATE(); SET @rc = 1;
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM ProcessChain WHERE ThisDate < @CutoffDate;
    SET @rc = @@ROWCOUNT; SET @total += @rc;
    IF @rc > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @total) WITH NOWAIT; END
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE());
RAISERROR('  Total: %I64d (%ds)', 0, 1, @total, @sec) WITH NOWAIT;

-- 6. HistoryJob
PRINT 'HistoryJob...';
SET @total = 0; SET @st = GETDATE(); SET @rc = 1;
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM HistoryJob WHERE StartAt < @CutoffDate;
    SET @rc = @@ROWCOUNT; SET @total += @rc;
    IF @rc > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @total) WITH NOWAIT; END
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE());
RAISERROR('  Total: %I64d (%ds)', 0, 1, @total, @sec) WITH NOWAIT;

-- 7. HistoryChain
PRINT 'HistoryChain...';
SET @total = 0; SET @st = GETDATE(); SET @rc = 1;
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM HistoryChain WHERE COALESCE(FirstDate, LastDate) < @CutoffDate;
    SET @rc = @@ROWCOUNT; SET @total += @rc;
    IF @rc > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @total) WITH NOWAIT; END
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE());
RAISERROR('  Total: %I64d (%ds)', 0, 1, @total, @sec) WITH NOWAIT;

-- 8. ProcessInfo
PRINT 'ProcessInfo...';
SET @total = 0; SET @st = GETDATE(); SET @rc = 1;
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM ProcessInfo WHERE COALESCE(FirstDate, LastDate) < @CutoffDate;
    SET @rc = @@ROWCOUNT; SET @total += @rc;
    IF @rc > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @total) WITH NOWAIT; END
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE());
RAISERROR('  Total: %I64d (%ds)', 0, 1, @total, @sec) WITH NOWAIT;

-- 9. ProcessGroup
PRINT 'ProcessGroup...';
SET @total = 0; SET @st = GETDATE(); SET @rc = 1;
WHILE @rc > 0
BEGIN
    DELETE TOP (@BatchSize) FROM ProcessGroup WHERE COALESCE(FirstDate, LastDate, ExportDate) < @CutoffDate;
    SET @rc = @@ROWCOUNT; SET @total += @rc;
    IF @rc > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @total) WITH NOWAIT; END
END
SET @sec = DATEDIFF(SECOND, @st, GETDATE());
RAISERROR('  Total: %I64d (%ds)', 0, 1, @total, @sec) WITH NOWAIT;

-- ────────────────────────────────────────────────────────────
-- 5. CLEANUP: Drop Temporary Cleanup Indexes
-- ────────────────────────────────────────────────────────────
PRINT '';
RAISERROR('Dropping temporary cleanup indexes...', 0, 1) WITH NOWAIT;

DECLARE ix_drop CURSOR LOCAL FAST_FORWARD FOR
    SELECT 'DROP INDEX [' + i.name + '] ON [' + OBJECT_NAME(i.object_id) + ']'
    FROM sys.indexes i
    WHERE i.name LIKE 'IX_Cleanup_%';
OPEN ix_drop;
FETCH NEXT FROM ix_drop INTO @dropIdx;
WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC sp_executesql @dropIdx;
    FETCH NEXT FROM ix_drop INTO @dropIdx;
END
CLOSE ix_drop; DEALLOCATE ix_drop;

RAISERROR('Temporary indexes dropped.', 0, 1) WITH NOWAIT;

-- ────────────────────────────────────────────────────────────
-- 6. POST-FLIGHT: Remaining Row Counts & Runtime
-- ────────────────────────────────────────────────────────────
PRINT '';
PRINT '# POST-FLIGHT (estimated rows remaining)';
PRINT '------------------------------------------------';
DECLARE pf2 CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM @tables;
OPEN pf2;
FETCH NEXT FROM pf2 INTO @tbl;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @est = SUM(row_count) FROM sys.dm_db_partition_stats
    WHERE object_id = OBJECT_ID(@tbl) AND index_id IN (0,1);
    RAISERROR('  %s: ~%I64d rows', 0, 1, @tbl, @est) WITH NOWAIT;
    FETCH NEXT FROM pf2 INTO @tbl;
END
CLOSE pf2; DEALLOCATE pf2;

-- Fixed calculation block
DECLARE @totalSec INT = DATEDIFF(SECOND, @scriptStart, GETDATE());
DECLARE @totalMin INT = @totalSec / 60;

PRINT '';
RAISERROR('Total runtime: %d seconds (~%d minutes)', 0, 1, @totalSec, @totalMin) WITH NOWAIT;
PRINT '================================================';
PRINT 'Done.';
GO
