-- ============================================================
-- HDB Cleanup — Simple Batched Deletes
-- ============================================================
-- Resumable: each batch auto-commits. If the script stops,
-- just rerun it — already-deleted rows won't match again.
--
-- BACKUP YOUR DATABASE BEFORE THE FIRST RUN
-- ============================================================

USE [OneIMHDB3]
GO
SET NOCOUNT ON
GO

DECLARE @Cut DATETIME = DATEADD(YEAR, -2, GETDATE())
DECLARE @B   INT      = 500000
DECLARE @d   INT, @t  BIGINT, @s DATETIME, @sec INT

DECLARE @scriptStart DATETIME = GETDATE()

PRINT 'Cutoff: ' + CONVERT(VARCHAR, @Cut, 120)
PRINT ''

-- ── Pre-flight: estimated row counts ──────────────────────
PRINT '# PRE-FLIGHT (estimated rows per table)'
PRINT '------------------------------------------------'
DECLARE @tbl NVARCHAR(128), @est BIGINT
DECLARE @tables TABLE (name NVARCHAR(128))
INSERT @tables VALUES
  ('WatchProperty'),('WatchOperation'),('ProcessStep'),
  ('ProcessSubstitute'),('ProcessChain'),('HistoryJob'),
  ('HistoryChain'),('ProcessInfo'),('ProcessGroup')

DECLARE pf CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM @tables
OPEN pf
FETCH NEXT FROM pf INTO @tbl
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @est = SUM(row_count) FROM sys.dm_db_partition_stats
    WHERE object_id = OBJECT_ID(@tbl) AND index_id IN (0,1)
    RAISERROR('  %s: ~%I64d rows', 0, 1, @tbl, @est) WITH NOWAIT
    FETCH NEXT FROM pf INTO @tbl
END
CLOSE pf; DEALLOCATE pf
PRINT ''
PRINT '# CLEANUP'
PRINT '================================================'

-- 1. WatchProperty (FK child — must go first)
PRINT 'WatchProperty...'
SET @t = 0; SET @s = GETDATE(); SET @d = 1
WHILE @d > 0
BEGIN
    DELETE TOP (@B) wp
    FROM WatchProperty wp
    INNER JOIN WatchOperation wo ON wp.UID_DialogWatchOperation = wo.UID_DialogWatchOperation
    WHERE wo.OperationDate < @Cut
    SET @d = @@ROWCOUNT; SET @t += @d
    IF @d > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @t) WITH NOWAIT END
END
SET @sec = DATEDIFF(SECOND, @s, GETDATE())
RAISERROR('  Total: %I64d (%ds)', 0, 1, @t, @sec) WITH NOWAIT

-- 2. WatchOperation
PRINT 'WatchOperation...'
SET @t = 0; SET @s = GETDATE(); SET @d = 1
WHILE @d > 0
BEGIN
    DELETE TOP (@B) FROM WatchOperation WHERE OperationDate < @Cut
    SET @d = @@ROWCOUNT; SET @t += @d
    IF @d > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @t) WITH NOWAIT END
END
SET @sec = DATEDIFF(SECOND, @s, GETDATE())
RAISERROR('  Total: %I64d (%ds)', 0, 1, @t, @sec) WITH NOWAIT

-- 3. ProcessStep
PRINT 'ProcessStep...'
SET @t = 0; SET @s = GETDATE(); SET @d = 1
WHILE @d > 0
BEGIN
    DELETE TOP (@B) FROM ProcessStep WHERE ThisDate < @Cut
    SET @d = @@ROWCOUNT; SET @t += @d
    IF @d > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @t) WITH NOWAIT END
END
SET @sec = DATEDIFF(SECOND, @s, GETDATE())
RAISERROR('  Total: %I64d (%ds)', 0, 1, @t, @sec) WITH NOWAIT

-- 4. ProcessSubstitute (FK child)
PRINT 'ProcessSubstitute...'
SET @t = 0; SET @s = GETDATE(); SET @d = 1
WHILE @d > 0
BEGIN
    DELETE TOP (@B) ps
    FROM ProcessSubstitute ps
    INNER JOIN ProcessInfo pi ON ps.UID_ProcessInfoNew = pi.UID_ProcessInfo
    WHERE COALESCE(pi.FirstDate, pi.LastDate) < @Cut
    SET @d = @@ROWCOUNT; SET @t += @d
    IF @d > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @t) WITH NOWAIT END
END
SET @sec = DATEDIFF(SECOND, @s, GETDATE())
RAISERROR('  Total: %I64d (%ds)', 0, 1, @t, @sec) WITH NOWAIT

-- 5. ProcessChain
PRINT 'ProcessChain...'
SET @t = 0; SET @s = GETDATE(); SET @d = 1
WHILE @d > 0
BEGIN
    DELETE TOP (@B) FROM ProcessChain WHERE ThisDate < @Cut
    SET @d = @@ROWCOUNT; SET @t += @d
    IF @d > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @t) WITH NOWAIT END
END
SET @sec = DATEDIFF(SECOND, @s, GETDATE())
RAISERROR('  Total: %I64d (%ds)', 0, 1, @t, @sec) WITH NOWAIT

-- 6. HistoryJob
PRINT 'HistoryJob...'
SET @t = 0; SET @s = GETDATE(); SET @d = 1
WHILE @d > 0
BEGIN
    DELETE TOP (@B) FROM HistoryJob WHERE StartAt < @Cut
    SET @d = @@ROWCOUNT; SET @t += @d
    IF @d > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @t) WITH NOWAIT END
END
SET @sec = DATEDIFF(SECOND, @s, GETDATE())
RAISERROR('  Total: %I64d (%ds)', 0, 1, @t, @sec) WITH NOWAIT

-- 7. HistoryChain
PRINT 'HistoryChain...'
SET @t = 0; SET @s = GETDATE(); SET @d = 1
WHILE @d > 0
BEGIN
    DELETE TOP (@B) FROM HistoryChain WHERE COALESCE(FirstDate, LastDate) < @Cut
    SET @d = @@ROWCOUNT; SET @t += @d
    IF @d > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @t) WITH NOWAIT END
END
SET @sec = DATEDIFF(SECOND, @s, GETDATE())
RAISERROR('  Total: %I64d (%ds)', 0, 1, @t, @sec) WITH NOWAIT

-- 8. ProcessInfo
PRINT 'ProcessInfo...'
SET @t = 0; SET @s = GETDATE(); SET @d = 1
WHILE @d > 0
BEGIN
    DELETE TOP (@B) FROM ProcessInfo WHERE COALESCE(FirstDate, LastDate) < @Cut
    SET @d = @@ROWCOUNT; SET @t += @d
    IF @d > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @t) WITH NOWAIT END
END
SET @sec = DATEDIFF(SECOND, @s, GETDATE())
RAISERROR('  Total: %I64d (%ds)', 0, 1, @t, @sec) WITH NOWAIT

-- 9. ProcessGroup
PRINT 'ProcessGroup...'
SET @t = 0; SET @s = GETDATE(); SET @d = 1
WHILE @d > 0
BEGIN
    DELETE TOP (@B) FROM ProcessGroup WHERE COALESCE(FirstDate, LastDate, ExportDate) < @Cut
    SET @d = @@ROWCOUNT; SET @t += @d
    IF @d > 0 BEGIN CHECKPOINT; RAISERROR('  %I64d done', 0, 1, @t) WITH NOWAIT END
END
SET @sec = DATEDIFF(SECOND, @s, GETDATE())
RAISERROR('  Total: %I64d (%ds)', 0, 1, @t, @sec) WITH NOWAIT

-- ── Post-flight: remaining row counts ─────────────────────
PRINT ''
PRINT '# POST-FLIGHT (estimated rows remaining)'
PRINT '------------------------------------------------'
DECLARE @tbl2 NVARCHAR(128), @est2 BIGINT
DECLARE @tables2 TABLE (name NVARCHAR(128))
INSERT @tables2 VALUES
  ('WatchProperty'),('WatchOperation'),('ProcessStep'),
  ('ProcessSubstitute'),('ProcessChain'),('HistoryJob'),
  ('HistoryChain'),('ProcessInfo'),('ProcessGroup')

DECLARE pf2 CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM @tables2
OPEN pf2
FETCH NEXT FROM pf2 INTO @tbl2
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @est2 = SUM(row_count) FROM sys.dm_db_partition_stats
    WHERE object_id = OBJECT_ID(@tbl2) AND index_id IN (0,1)
    RAISERROR('  %s: ~%I64d rows', 0, 1, @tbl2, @est2) WITH NOWAIT
    FETCH NEXT FROM pf2 INTO @tbl2
END
CLOSE pf2; DEALLOCATE pf2

DECLARE @totalSec INT = DATEDIFF(SECOND, @scriptStart, GETDATE())
PRINT ''
RAISERROR('Total runtime: %d seconds (~%d minutes)', 0, 1, @totalSec, @totalSec / 60) WITH NOWAIT
PRINT '================================================'
PRINT 'Done.'
GO
