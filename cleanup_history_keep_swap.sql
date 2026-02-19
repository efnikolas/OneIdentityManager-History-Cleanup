-- ============================================================
-- One Identity Manager -- HDB Cleanup (Keep-and-Swap Hybrid)
-- ============================================================
-- Strategy:
--   - Keep-and-swap for large purge-ratio tables:
--       1) WatchOperation
--       2) WatchProperty
--   - Batched delete for remaining tables.
--
-- Audit rule enforced:
--   Only rows with KNOWN date < @CutoffDate are purged.
--   NULL date rows are preserved.
--
-- BACKUP YOUR DATABASE BEFORE RUNNING THIS SCRIPT
-- ============================================================

USE [OneIMHDB3]   -- change to your HDB name
GO
SET NOCOUNT ON
GO

DECLARE @CutoffDate DATETIME = DATEADD(YEAR, -2, GETDATE())
DECLARE @BatchSize  INT      = 500000
DECLARE @dbName     NVARCHAR(128) = DB_NAME()
DECLARE @cutoffStr  VARCHAR(30)   = CONVERT(VARCHAR, @CutoffDate, 120)
DECLARE @sec        INT

RAISERROR('================================================', 0, 1) WITH NOWAIT
RAISERROR('HDB Cleanup (Keep-and-Swap) - %s', 0, 1, @dbName) WITH NOWAIT
RAISERROR('Purging known old data before: %s', 0, 1, @cutoffStr) WITH NOWAIT
RAISERROR('Batch size: %d', 0, 1, @BatchSize) WITH NOWAIT
RAISERROR('================================================', 0, 1) WITH NOWAIT

-- ============================================================
-- CLEANUP leftovers
-- ============================================================
RAISERROR('Cleaning up leftovers from previous runs...', 0, 1) WITH NOWAIT

IF OBJECT_ID('dbo.Keep_WatchOperation', 'U') IS NOT NULL DROP TABLE dbo.Keep_WatchOperation
IF OBJECT_ID('dbo.Keep_WatchProperty', 'U') IS NOT NULL DROP TABLE dbo.Keep_WatchProperty

DECLARE @dropIdx NVARCHAR(500)
DECLARE idx_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT 'DROP INDEX [' + i.name + '] ON [' + OBJECT_NAME(i.object_id) + ']'
    FROM sys.indexes i
    WHERE i.name LIKE 'IX_Cleanup_%' AND i.is_hypothetical = 0
OPEN idx_cur
FETCH NEXT FROM idx_cur INTO @dropIdx
WHILE @@FETCH_STATUS = 0
BEGIN
    RAISERROR('  %s', 0, 1, @dropIdx) WITH NOWAIT
    EXEC sp_executesql @dropIdx
    FETCH NEXT FROM idx_cur INTO @dropIdx
END
CLOSE idx_cur
DEALLOCATE idx_cur

RAISERROR('Cleanup done.', 0, 1) WITH NOWAIT
RAISERROR(' ', 0, 1) WITH NOWAIT

-- ============================================================
-- KEEP-AND-SWAP: WatchOperation + WatchProperty
-- ============================================================
DECLARE @st DATETIME
DECLARE @rows BIGINT
DECLARE @rc INT

DECLARE @WatchOperationCols NVARCHAR(MAX)
DECLARE @WatchPropertyCols  NVARCHAR(MAX)
DECLARE @WatchPropertyColsWp NVARCHAR(MAX)
DECLARE @sql NVARCHAR(MAX)

SELECT @WatchOperationCols = STRING_AGG(QUOTENAME(c.name), ',') WITHIN GROUP (ORDER BY c.column_id)
FROM sys.columns c
WHERE c.object_id = OBJECT_ID('dbo.WatchOperation')
  AND c.is_computed = 0

SELECT @WatchPropertyCols = STRING_AGG(QUOTENAME(c.name), ',') WITHIN GROUP (ORDER BY c.column_id)
FROM sys.columns c
WHERE c.object_id = OBJECT_ID('dbo.WatchProperty')
  AND c.is_computed = 0

SELECT @WatchPropertyColsWp = STRING_AGG('wp.' + QUOTENAME(c.name), ',') WITHIN GROUP (ORDER BY c.column_id)
FROM sys.columns c
WHERE c.object_id = OBJECT_ID('dbo.WatchProperty')
    AND c.is_computed = 0

IF @WatchOperationCols IS NULL OR @WatchPropertyCols IS NULL OR @WatchPropertyColsWp IS NULL
BEGIN
    RAISERROR('WatchOperation/WatchProperty not found. Aborting.', 16, 1)
    RETURN
END

-- Capture FK definition (WatchProperty -> WatchOperation)
DECLARE @FKName SYSNAME
DECLARE @FKParentSchema SYSNAME
DECLARE @FKParentTable SYSNAME
DECLARE @FKRefSchema SYSNAME
DECLARE @FKRefTable SYSNAME
DECLARE @FKParentCols NVARCHAR(MAX)
DECLARE @FKRefCols NVARCHAR(MAX)
DECLARE @DeleteAction NVARCHAR(30)
DECLARE @UpdateAction NVARCHAR(30)
DECLARE @DropFKSql NVARCHAR(MAX)
DECLARE @CreateFKSql NVARCHAR(MAX)
DECLARE @FKObjectId INT

SELECT TOP 1
    @FKObjectId = fk.object_id,
    @FKName = fk.name,
    @FKParentSchema = OBJECT_SCHEMA_NAME(fk.parent_object_id),
    @FKParentTable = OBJECT_NAME(fk.parent_object_id),
    @FKRefSchema = OBJECT_SCHEMA_NAME(fk.referenced_object_id),
    @FKRefTable = OBJECT_NAME(fk.referenced_object_id),
    @DeleteAction = fk.delete_referential_action_desc,
    @UpdateAction = fk.update_referential_action_desc
FROM sys.foreign_keys fk
WHERE fk.parent_object_id = OBJECT_ID('dbo.WatchProperty')
  AND fk.referenced_object_id = OBJECT_ID('dbo.WatchOperation')

IF @FKName IS NULL
BEGIN
    RAISERROR('Required FK WatchProperty -> WatchOperation not found. Aborting.', 16, 1)
    RETURN
END

SELECT @FKParentCols = STRING_AGG(QUOTENAME(pc.name), ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id),
       @FKRefCols = STRING_AGG(QUOTENAME(rc.name), ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id)
FROM sys.foreign_key_columns fkc
JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
WHERE fkc.constraint_object_id = @FKObjectId

SET @DropFKSql =
    'ALTER TABLE ' + QUOTENAME(@FKParentSchema) + '.' + QUOTENAME(@FKParentTable) +
    ' DROP CONSTRAINT ' + QUOTENAME(@FKName)

SET @CreateFKSql =
    'ALTER TABLE ' + QUOTENAME(@FKParentSchema) + '.' + QUOTENAME(@FKParentTable) +
    ' WITH CHECK ADD CONSTRAINT ' + QUOTENAME(@FKName) +
    ' FOREIGN KEY (' + @FKParentCols + ')' +
    ' REFERENCES ' + QUOTENAME(@FKRefSchema) + '.' + QUOTENAME(@FKRefTable) +
    ' (' + @FKRefCols + ')'

IF @DeleteAction <> 'NO_ACTION'
    SET @CreateFKSql += ' ON DELETE ' + @DeleteAction
IF @UpdateAction <> 'NO_ACTION'
    SET @CreateFKSql += ' ON UPDATE ' + @UpdateAction

RAISERROR('Staging keep rows for WatchOperation...', 0, 1) WITH NOWAIT
SET @st = GETDATE()

SET @sql =
    'SELECT ' + @WatchOperationCols + '
     INTO dbo.Keep_WatchOperation
     FROM dbo.WatchOperation
     WHERE OperationDate >= @CutoffDate OR OperationDate IS NULL;'
EXEC sp_executesql @sql, N'@CutoffDate DATETIME', @CutoffDate

SELECT @rows = COUNT_BIG(*) FROM dbo.Keep_WatchOperation
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  Keep_WatchOperation rows: %I64d (%ds)', 0, 1, @rows, @sec) WITH NOWAIT

RAISERROR('Staging keep rows for WatchProperty...', 0, 1) WITH NOWAIT
SET @st = GETDATE()

SET @sql =
    'SELECT ' + @WatchPropertyColsWp + '
     INTO dbo.Keep_WatchProperty
     FROM dbo.WatchProperty wp
     INNER JOIN dbo.Keep_WatchOperation wo
       ON wp.UID_DialogWatchOperation = wo.UID_DialogWatchOperation;'
EXEC sp_executesql @sql

SELECT @rows = COUNT_BIG(*) FROM dbo.Keep_WatchProperty
SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  Keep_WatchProperty rows: %I64d (%ds)', 0, 1, @rows, @sec) WITH NOWAIT

RAISERROR('Truncating WatchProperty (child)...', 0, 1) WITH NOWAIT
TRUNCATE TABLE dbo.WatchProperty

RAISERROR('Dropping FK and truncating WatchOperation...', 0, 1) WITH NOWAIT
EXEC sp_executesql @DropFKSql
TRUNCATE TABLE dbo.WatchOperation

RAISERROR('Reloading WatchOperation keep rows (batched)...', 0, 1) WITH NOWAIT
SET @st = GETDATE()
DECLARE @reloadTotal BIGINT = 0
DECLARE @reloadRc INT = 1

IF EXISTS (SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID('dbo.WatchOperation'))
    SET IDENTITY_INSERT dbo.WatchOperation ON

-- Add a temp rownum column to batch the reload
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.Keep_WatchOperation') AND name = '_RowFlag')
    ALTER TABLE dbo.Keep_WatchOperation ADD _RowFlag BIT NOT NULL DEFAULT 0

WHILE @reloadRc > 0
BEGIN
    SET @sql =
        'INSERT INTO dbo.WatchOperation (' + @WatchOperationCols + ')
         SELECT TOP (' + CAST(@BatchSize AS VARCHAR) + ') ' + @WatchOperationCols + '
         FROM dbo.Keep_WatchOperation WHERE _RowFlag = 0;'
    EXEC sp_executesql @sql
    SET @reloadRc = @@ROWCOUNT
    SET @reloadTotal += @reloadRc

    -- Mark inserted rows
    SET @sql =
        'UPDATE TOP (' + CAST(@BatchSize AS VARCHAR) + ') dbo.Keep_WatchOperation SET _RowFlag = 1 WHERE _RowFlag = 0;'
    EXEC sp_executesql @sql

    IF @reloadRc > 0
        RAISERROR('  %I64d reloaded so far...', 0, 1, @reloadTotal) WITH NOWAIT
END

IF EXISTS (SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID('dbo.WatchOperation'))
    SET IDENTITY_INSERT dbo.WatchOperation OFF

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  WatchOperation reloaded: %I64d rows (%ds)', 0, 1, @reloadTotal, @sec) WITH NOWAIT

RAISERROR('Recreating FK...', 0, 1) WITH NOWAIT
EXEC sp_executesql @CreateFKSql
SET @sql =
    'ALTER TABLE ' + QUOTENAME(@FKParentSchema) + '.' + QUOTENAME(@FKParentTable) +
    ' CHECK CONSTRAINT ' + QUOTENAME(@FKName)
EXEC sp_executesql @sql

RAISERROR('Reloading WatchProperty keep rows (batched)...', 0, 1) WITH NOWAIT
SET @st = GETDATE()
SET @reloadTotal = 0
SET @reloadRc = 1

IF EXISTS (SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID('dbo.WatchProperty'))
    SET IDENTITY_INSERT dbo.WatchProperty ON

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.Keep_WatchProperty') AND name = '_RowFlag')
    ALTER TABLE dbo.Keep_WatchProperty ADD _RowFlag BIT NOT NULL DEFAULT 0

WHILE @reloadRc > 0
BEGIN
    SET @sql =
        'INSERT INTO dbo.WatchProperty (' + @WatchPropertyCols + ')
         SELECT TOP (' + CAST(@BatchSize AS VARCHAR) + ') ' + @WatchPropertyCols + '
         FROM dbo.Keep_WatchProperty WHERE _RowFlag = 0;'
    EXEC sp_executesql @sql
    SET @reloadRc = @@ROWCOUNT
    SET @reloadTotal += @reloadRc

    SET @sql =
        'UPDATE TOP (' + CAST(@BatchSize AS VARCHAR) + ') dbo.Keep_WatchProperty SET _RowFlag = 1 WHERE _RowFlag = 0;'
    EXEC sp_executesql @sql

    IF @reloadRc > 0
        RAISERROR('  %I64d reloaded so far...', 0, 1, @reloadTotal) WITH NOWAIT
END

IF EXISTS (SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID('dbo.WatchProperty'))
    SET IDENTITY_INSERT dbo.WatchProperty OFF

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  WatchProperty reloaded: %I64d rows (%ds)', 0, 1, @reloadTotal, @sec) WITH NOWAIT

-- Crash safety: verify reload counts before dropping Keep tables
DECLARE @finalWO BIGINT, @keepWO BIGINT
DECLARE @finalWP BIGINT, @keepWP BIGINT
SELECT @finalWO = COUNT_BIG(*) FROM dbo.WatchOperation
SELECT @keepWO  = COUNT_BIG(*) FROM dbo.Keep_WatchOperation
SELECT @finalWP = COUNT_BIG(*) FROM dbo.WatchProperty
SELECT @keepWP  = COUNT_BIG(*) FROM dbo.Keep_WatchProperty

IF @finalWO < @keepWO OR @finalWP < @keepWP
BEGIN
    RAISERROR('ERROR: Reload count mismatch! Keep tables preserved for manual recovery.', 16, 1) WITH NOWAIT
    RAISERROR('  WatchOperation: reloaded=%I64d, keep=%I64d', 0, 1, @finalWO, @keepWO) WITH NOWAIT
    RAISERROR('  WatchProperty:  reloaded=%I64d, keep=%I64d', 0, 1, @finalWP, @keepWP) WITH NOWAIT
    RETURN
END

RAISERROR('  Reload verified. Dropping Keep tables...', 0, 1) WITH NOWAIT
DROP TABLE dbo.Keep_WatchProperty
DROP TABLE dbo.Keep_WatchOperation

RAISERROR('Keep-and-swap complete for WatchOperation/WatchProperty.', 0, 1) WITH NOWAIT
RAISERROR(' ', 0, 1) WITH NOWAIT

-- ============================================================
-- BATCH DELETE remaining tables
-- ============================================================
DECLARE @total BIGINT

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
