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

PRINT 'Cutoff: ' + CONVERT(VARCHAR, @Cut, 120)
PRINT ''

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

PRINT ''
PRINT 'Done.'
GO
