-- ============================================================
-- One Identity Manager — HDB Cleanup (TRUNCATE Version)
-- ============================================================
-- Keeps rows newer than @CutoffDate, truncates the rest.
-- Much faster than batched DELETE when removing 90%+ of data.
--
-- Approach per table:
--   1. Copy rows to KEEP into a temp table (SELECT INTO)
--   2. TRUNCATE the original (instant, minimally logged)
--   3. Re-insert the kept rows
--
-- Because TRUNCATE fails if FK constraints reference the table,
-- this script drops all HDB foreign keys first, then recreates
-- them at the end.
--
-- Recovery model is temporarily set to SIMPLE so that SELECT
-- INTO and bulk INSERT are minimally logged. Restored at end.
--
-- ⚠️  BACKUP YOUR DATABASE BEFORE RUNNING THIS SCRIPT
-- ⚠️  This requires exclusive access (no concurrent writes)
-- ============================================================

USE [OneIMHDB3]   -- ← change to your HDB name
GO
SET NOCOUNT ON
GO

-- ── CONFIG ──────────────────────────────────────────────────
DECLARE @CutoffDate  DATETIME = DATEADD(YEAR, -2, GETDATE())
DECLARE @WhatIf      BIT      = 0        -- 1 = print counts only
-- ────────────────────────────────────────────────────────────

PRINT '================================================'
PRINT 'HDB Cleanup (TRUNCATE) — ' + DB_NAME()
PRINT 'Cutoff:  ' + CONVERT(VARCHAR, @CutoffDate, 120)
PRINT 'WhatIf:  ' + CAST(@WhatIf AS VARCHAR)
PRINT '================================================'
PRINT ''

-- ── PRE-FLIGHT: show what will be kept vs removed ───────────
PRINT '# PRE-FLIGHT'
PRINT '------------------------------------------------'

DECLARE @total   BIGINT
DECLARE @keep    BIGINT
DECLARE @remove  BIGINT

-- WatchOperation
SELECT @total = COUNT_BIG(*) FROM WatchOperation
SELECT @keep  = COUNT_BIG(*) FROM WatchOperation WHERE OperationDate >= @CutoffDate
SET @remove = @total - @keep
PRINT 'WatchOperation:   ' + CAST(@total AS VARCHAR) + ' total | ' + CAST(@keep AS VARCHAR) + ' keep | ' + CAST(@remove AS VARCHAR) + ' remove'

-- WatchProperty (via FK to WatchOperation)
SELECT @total = COUNT_BIG(*) FROM WatchProperty
SELECT @keep  = COUNT_BIG(*) FROM WatchProperty child
    INNER JOIN WatchOperation parent ON child.UID_DialogWatchOperation = parent.UID_DialogWatchOperation
    WHERE parent.OperationDate >= @CutoffDate
SET @remove = @total - @keep
PRINT 'WatchProperty:    ' + CAST(@total AS VARCHAR) + ' total | ' + CAST(@keep AS VARCHAR) + ' keep | ' + CAST(@remove AS VARCHAR) + ' remove'

-- ProcessInfo
SELECT @total = COUNT_BIG(*) FROM ProcessInfo
SELECT @keep  = COUNT_BIG(*) FROM ProcessInfo WHERE FirstDate >= @CutoffDate
SET @remove = @total - @keep
PRINT 'ProcessInfo:      ' + CAST(@total AS VARCHAR) + ' total | ' + CAST(@keep AS VARCHAR) + ' keep | ' + CAST(@remove AS VARCHAR) + ' remove'

-- ProcessSubstitute (via FK to ProcessInfo)
SELECT @total = COUNT_BIG(*) FROM ProcessSubstitute
SELECT @keep  = COUNT_BIG(*) FROM ProcessSubstitute child
    INNER JOIN ProcessInfo parent ON child.UID_ProcessInfoNew = parent.UID_ProcessInfo
    WHERE parent.FirstDate >= @CutoffDate
SET @remove = @total - @keep
PRINT 'ProcessSubstitute:' + CAST(@total AS VARCHAR) + ' total | ' + CAST(@keep AS VARCHAR) + ' keep | ' + CAST(@remove AS VARCHAR) + ' remove'

-- ProcessGroup
SELECT @total = COUNT_BIG(*) FROM ProcessGroup
SELECT @keep  = COUNT_BIG(*) FROM ProcessGroup WHERE FirstDate >= @CutoffDate
SET @remove = @total - @keep
PRINT 'ProcessGroup:     ' + CAST(@total AS VARCHAR) + ' total | ' + CAST(@keep AS VARCHAR) + ' keep | ' + CAST(@remove AS VARCHAR) + ' remove'

-- ProcessStep
SELECT @total = COUNT_BIG(*) FROM ProcessStep
SELECT @keep  = COUNT_BIG(*) FROM ProcessStep WHERE ThisDate >= @CutoffDate
SET @remove = @total - @keep
PRINT 'ProcessStep:      ' + CAST(@total AS VARCHAR) + ' total | ' + CAST(@keep AS VARCHAR) + ' keep | ' + CAST(@remove AS VARCHAR) + ' remove'

-- ProcessChain
SELECT @total = COUNT_BIG(*) FROM ProcessChain
SELECT @keep  = COUNT_BIG(*) FROM ProcessChain WHERE ThisDate >= @CutoffDate
SET @remove = @total - @keep
PRINT 'ProcessChain:     ' + CAST(@total AS VARCHAR) + ' total | ' + CAST(@keep AS VARCHAR) + ' keep | ' + CAST(@remove AS VARCHAR) + ' remove'

-- HistoryJob
SELECT @total = COUNT_BIG(*) FROM HistoryJob
SELECT @keep  = COUNT_BIG(*) FROM HistoryJob WHERE StartAt >= @CutoffDate
SET @remove = @total - @keep
PRINT 'HistoryJob:       ' + CAST(@total AS VARCHAR) + ' total | ' + CAST(@keep AS VARCHAR) + ' keep | ' + CAST(@remove AS VARCHAR) + ' remove'

-- HistoryChain
SELECT @total = COUNT_BIG(*) FROM HistoryChain
SELECT @keep  = COUNT_BIG(*) FROM HistoryChain WHERE FirstDate >= @CutoffDate
SET @remove = @total - @keep
PRINT 'HistoryChain:     ' + CAST(@total AS VARCHAR) + ' total | ' + CAST(@keep AS VARCHAR) + ' keep | ' + CAST(@remove AS VARCHAR) + ' remove'

PRINT ''

IF @WhatIf = 1
BEGIN
    PRINT 'WhatIf mode — nothing changed.'
    RETURN
END

-- ════════════════════════════════════════════════════════════
-- EXECUTION MODE
-- ════════════════════════════════════════════════════════════

-- ── Save and switch recovery model to SIMPLE ────────────────
DECLARE @origRecovery NVARCHAR(60)
SELECT @origRecovery = recovery_model_desc
FROM sys.databases WHERE name = DB_NAME()

PRINT 'Recovery model: ' + @origRecovery
IF @origRecovery <> 'SIMPLE'
BEGIN
    PRINT 'Switching to SIMPLE for minimal logging...'
    DECLARE @sqlRM NVARCHAR(200) = 'ALTER DATABASE [' + DB_NAME() + '] SET RECOVERY SIMPLE'
    EXEC sp_executesql @sqlRM
END

-- ── Capture and drop all HDB foreign keys ───────────────────
PRINT ''
PRINT '# Dropping foreign keys...'

-- Store FK definitions for later recreation
IF OBJECT_ID('tempdb..#FKDefinitions') IS NOT NULL DROP TABLE #FKDefinitions
CREATE TABLE #FKDefinitions (
    FKName       NVARCHAR(256),
    ChildTable   NVARCHAR(256),
    ChildCol     NVARCHAR(256),
    ParentTable  NVARCHAR(256),
    ParentCol    NVARCHAR(256)
)

INSERT #FKDefinitions (FKName, ChildTable, ChildCol, ParentTable, ParentCol)
SELECT
    fk.name,
    OBJECT_NAME(fk.parent_object_id),
    cp.name,
    OBJECT_NAME(fk.referenced_object_id),
    cr.name
FROM sys.foreign_keys fk
INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN sys.columns cp ON fkc.parent_object_id  = cp.object_id AND fkc.parent_column_id     = cp.column_id
INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
WHERE OBJECT_NAME(fk.parent_object_id) IN (
    'WatchProperty','WatchOperation','ProcessStep','ProcessSubstitute',
    'ProcessChain','HistoryJob','HistoryChain','ProcessInfo','ProcessGroup'
)

-- Drop them
DECLARE @fkDrop NVARCHAR(500)
DECLARE fk_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT 'ALTER TABLE [' + ChildTable + '] DROP CONSTRAINT [' + FKName + ']'
    FROM #FKDefinitions
OPEN fk_cur
FETCH NEXT FROM fk_cur INTO @fkDrop
WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '  ' + @fkDrop
    EXEC sp_executesql @fkDrop
    FETCH NEXT FROM fk_cur INTO @fkDrop
END
CLOSE fk_cur; DEALLOCATE fk_cur

PRINT ''
PRINT '# TRUNCATE cleanup'
PRINT '================================================'

DECLARE @st DATETIME
DECLARE @sec INT
DECLARE @kept BIGINT

-- ────────────────────────────────────────────────────────────
-- 1. WatchProperty  (FK join → WatchOperation.OperationDate)
-- ────────────────────────────────────────────────────────────
PRINT 'WatchProperty...'
SET @st = GETDATE()

SELECT child.*
INTO #Keep_WatchProperty
FROM WatchProperty child
INNER JOIN WatchOperation parent
    ON child.UID_DialogWatchOperation = parent.UID_DialogWatchOperation
WHERE parent.OperationDate >= @CutoffDate

TRUNCATE TABLE WatchProperty

SET IDENTITY_INSERT WatchProperty OFF  -- no identity cols expected, but safe
INSERT INTO WatchProperty SELECT * FROM #Keep_WatchProperty
SET @kept = @@ROWCOUNT
DROP TABLE #Keep_WatchProperty

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
PRINT '  Kept ' + CAST(@kept AS VARCHAR) + ' rows | ' + CAST(@sec AS VARCHAR) + 's'

-- ────────────────────────────────────────────────────────────
-- 2. WatchOperation  (OperationDate)
-- ────────────────────────────────────────────────────────────
PRINT 'WatchOperation...'
SET @st = GETDATE()

SELECT * INTO #Keep_WatchOperation
FROM WatchOperation WHERE OperationDate >= @CutoffDate

TRUNCATE TABLE WatchOperation

INSERT INTO WatchOperation SELECT * FROM #Keep_WatchOperation
SET @kept = @@ROWCOUNT
DROP TABLE #Keep_WatchOperation

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
PRINT '  Kept ' + CAST(@kept AS VARCHAR) + ' rows | ' + CAST(@sec AS VARCHAR) + 's'

-- ────────────────────────────────────────────────────────────
-- 3. ProcessStep  (ThisDate)
-- ────────────────────────────────────────────────────────────
PRINT 'ProcessStep...'
SET @st = GETDATE()

SELECT * INTO #Keep_ProcessStep
FROM ProcessStep WHERE ThisDate >= @CutoffDate

TRUNCATE TABLE ProcessStep

INSERT INTO ProcessStep SELECT * FROM #Keep_ProcessStep
SET @kept = @@ROWCOUNT
DROP TABLE #Keep_ProcessStep

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
PRINT '  Kept ' + CAST(@kept AS VARCHAR) + ' rows | ' + CAST(@sec AS VARCHAR) + 's'

-- ────────────────────────────────────────────────────────────
-- 4. ProcessSubstitute  (FK join → ProcessInfo.FirstDate)
-- ────────────────────────────────────────────────────────────
PRINT 'ProcessSubstitute...'
SET @st = GETDATE()

SELECT child.*
INTO #Keep_ProcessSubstitute
FROM ProcessSubstitute child
INNER JOIN ProcessInfo parent ON child.UID_ProcessInfoNew = parent.UID_ProcessInfo
WHERE parent.FirstDate >= @CutoffDate

TRUNCATE TABLE ProcessSubstitute

INSERT INTO ProcessSubstitute SELECT * FROM #Keep_ProcessSubstitute
SET @kept = @@ROWCOUNT
DROP TABLE #Keep_ProcessSubstitute

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
PRINT '  Kept ' + CAST(@kept AS VARCHAR) + ' rows | ' + CAST(@sec AS VARCHAR) + 's'

-- ────────────────────────────────────────────────────────────
-- 5. ProcessChain  (ThisDate)
-- ────────────────────────────────────────────────────────────
PRINT 'ProcessChain...'
SET @st = GETDATE()

SELECT * INTO #Keep_ProcessChain
FROM ProcessChain WHERE ThisDate >= @CutoffDate

TRUNCATE TABLE ProcessChain

INSERT INTO ProcessChain SELECT * FROM #Keep_ProcessChain
SET @kept = @@ROWCOUNT
DROP TABLE #Keep_ProcessChain

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
PRINT '  Kept ' + CAST(@kept AS VARCHAR) + ' rows | ' + CAST(@sec AS VARCHAR) + 's'

-- ────────────────────────────────────────────────────────────
-- 6. HistoryJob  (StartAt)
-- ────────────────────────────────────────────────────────────
PRINT 'HistoryJob...'
SET @st = GETDATE()

SELECT * INTO #Keep_HistoryJob
FROM HistoryJob WHERE StartAt >= @CutoffDate

TRUNCATE TABLE HistoryJob

INSERT INTO HistoryJob SELECT * FROM #Keep_HistoryJob
SET @kept = @@ROWCOUNT
DROP TABLE #Keep_HistoryJob

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
PRINT '  Kept ' + CAST(@kept AS VARCHAR) + ' rows | ' + CAST(@sec AS VARCHAR) + 's'

-- ────────────────────────────────────────────────────────────
-- 7. HistoryChain  (FirstDate)
-- ────────────────────────────────────────────────────────────
PRINT 'HistoryChain...'
SET @st = GETDATE()

SELECT * INTO #Keep_HistoryChain
FROM HistoryChain WHERE FirstDate >= @CutoffDate

TRUNCATE TABLE HistoryChain

INSERT INTO HistoryChain SELECT * FROM #Keep_HistoryChain
SET @kept = @@ROWCOUNT
DROP TABLE #Keep_HistoryChain

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
PRINT '  Kept ' + CAST(@kept AS VARCHAR) + ' rows | ' + CAST(@sec AS VARCHAR) + 's'

-- ────────────────────────────────────────────────────────────
-- 8. ProcessInfo  (FirstDate)
-- ────────────────────────────────────────────────────────────
PRINT 'ProcessInfo...'
SET @st = GETDATE()

SELECT * INTO #Keep_ProcessInfo
FROM ProcessInfo WHERE FirstDate >= @CutoffDate

TRUNCATE TABLE ProcessInfo

INSERT INTO ProcessInfo SELECT * FROM #Keep_ProcessInfo
SET @kept = @@ROWCOUNT
DROP TABLE #Keep_ProcessInfo

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
PRINT '  Kept ' + CAST(@kept AS VARCHAR) + ' rows | ' + CAST(@sec AS VARCHAR) + 's'

-- ────────────────────────────────────────────────────────────
-- 9. ProcessGroup  (FirstDate)
-- ────────────────────────────────────────────────────────────
PRINT 'ProcessGroup...'
SET @st = GETDATE()

SELECT * INTO #Keep_ProcessGroup
FROM ProcessGroup WHERE FirstDate >= @CutoffDate

TRUNCATE TABLE ProcessGroup

INSERT INTO ProcessGroup SELECT * FROM #Keep_ProcessGroup
SET @kept = @@ROWCOUNT
DROP TABLE #Keep_ProcessGroup

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
PRINT '  Kept ' + CAST(@kept AS VARCHAR) + ' rows | ' + CAST(@sec AS VARCHAR) + 's'

-- ── Recreate foreign keys ───────────────────────────────────
PRINT ''
PRINT '# Recreating foreign keys...'

DECLARE @fkCreate NVARCHAR(500)
DECLARE fk_cur2 CURSOR LOCAL FAST_FORWARD FOR
    SELECT 'ALTER TABLE [' + ChildTable + '] ADD CONSTRAINT [' + FKName
         + '] FOREIGN KEY ([' + ChildCol + ']) REFERENCES [' + ParentTable
         + '] ([' + ParentCol + '])'
    FROM #FKDefinitions
OPEN fk_cur2
FETCH NEXT FROM fk_cur2 INTO @fkCreate
WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '  ' + @fkCreate
    EXEC sp_executesql @fkCreate
    FETCH NEXT FROM fk_cur2 INTO @fkCreate
END
CLOSE fk_cur2; DEALLOCATE fk_cur2

DROP TABLE #FKDefinitions

-- ── Restore recovery model ──────────────────────────────────
IF @origRecovery <> 'SIMPLE'
BEGIN
    DECLARE @sqlRestore NVARCHAR(200) = 'ALTER DATABASE [' + DB_NAME() + '] SET RECOVERY ' + @origRecovery
    PRINT ''
    PRINT 'Restoring recovery model to ' + @origRecovery + '...'
    EXEC sp_executesql @sqlRestore
END

-- ════════════════════════════════════════════════════════════
PRINT ''
PRINT '================================================'
PRINT 'Cleanup complete.'
PRINT '================================================'
GO
