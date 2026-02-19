-- ============================================================
-- RECOVERY: Reload WatchOperation/WatchProperty from Keep tables
-- ============================================================
-- Use this ONLY if the keep-and-swap script failed mid-reload
-- and the Keep_WatchOperation / Keep_WatchProperty tables
-- still exist with your data.
--
-- This script:
--   1. Empties the (partially loaded) live tables
--   2. Reloads from Keep tables in deterministic batches
--   3. Recreates the FK if missing
--   4. Verifies counts
--   5. Drops Keep tables on success
-- ============================================================

USE [OneIMHDB3]   -- change to your HDB name
GO
SET NOCOUNT ON
GO

DECLARE @BatchSize INT = 500000
DECLARE @sec INT

-- ── Verify Keep tables exist ──────────────────────────────
IF OBJECT_ID('dbo.Keep_WatchOperation', 'U') IS NULL
   OR OBJECT_ID('dbo.Keep_WatchProperty', 'U') IS NULL
BEGIN
    RAISERROR('Keep tables not found. Nothing to recover.', 16, 1)
    RETURN
END

DECLARE @keepWO BIGINT = (SELECT COUNT_BIG(*) FROM dbo.Keep_WatchOperation)
DECLARE @keepWP BIGINT = (SELECT COUNT_BIG(*) FROM dbo.Keep_WatchProperty)
RAISERROR('Keep_WatchOperation: %I64d rows', 0, 1, @keepWO) WITH NOWAIT
RAISERROR('Keep_WatchProperty:  %I64d rows', 0, 1, @keepWP) WITH NOWAIT

-- ── Build dynamic column lists ────────────────────────────
DECLARE @WatchOperationCols NVARCHAR(MAX)
DECLARE @WatchPropertyCols  NVARCHAR(MAX)
DECLARE @sql NVARCHAR(MAX)

SELECT @WatchOperationCols = STRING_AGG(QUOTENAME(c.name), ',') WITHIN GROUP (ORDER BY c.column_id)
FROM sys.columns c
WHERE c.object_id = OBJECT_ID('dbo.WatchOperation')
  AND c.is_computed = 0

SELECT @WatchPropertyCols = STRING_AGG(QUOTENAME(c.name), ',') WITHIN GROUP (ORDER BY c.column_id)
FROM sys.columns c
WHERE c.object_id = OBJECT_ID('dbo.WatchProperty')
  AND c.is_computed = 0

-- ── Drop FK if it exists (needed to truncate WatchOperation) ──
DECLARE @FKName SYSNAME
SELECT TOP 1 @FKName = fk.name
FROM sys.foreign_keys fk
WHERE fk.parent_object_id = OBJECT_ID('dbo.WatchProperty')
  AND fk.referenced_object_id = OBJECT_ID('dbo.WatchOperation')

IF @FKName IS NOT NULL
BEGIN
    SET @sql = 'ALTER TABLE dbo.WatchProperty DROP CONSTRAINT ' + QUOTENAME(@FKName)
    RAISERROR('Dropping FK %s...', 0, 1, @FKName) WITH NOWAIT
    EXEC sp_executesql @sql
END

-- ── Empty live tables ─────────────────────────────────────
RAISERROR('Truncating WatchProperty...', 0, 1) WITH NOWAIT
TRUNCATE TABLE dbo.WatchProperty

RAISERROR('Truncating WatchOperation...', 0, 1) WITH NOWAIT
TRUNCATE TABLE dbo.WatchOperation

-- ── Add _BatchID to Keep tables if not present ────────────
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.Keep_WatchOperation') AND name = '_BatchID')
    ALTER TABLE dbo.Keep_WatchOperation ADD _BatchID INT IDENTITY(1,1)

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.Keep_WatchProperty') AND name = '_BatchID')
    ALTER TABLE dbo.Keep_WatchProperty ADD _BatchID INT IDENTITY(1,1)

-- ── Reload WatchOperation ─────────────────────────────────
RAISERROR('Reloading WatchOperation...', 0, 1) WITH NOWAIT
DECLARE @st DATETIME = GETDATE()
DECLARE @reloadTotal BIGINT = 0
DECLARE @reloadRc INT = 0
DECLARE @maxId INT
DECLARE @lo INT = 1

EXEC sp_executesql N'SELECT @m = MAX(_BatchID) FROM dbo.Keep_WatchOperation', N'@m INT OUTPUT', @m = @maxId OUTPUT

IF EXISTS (SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID('dbo.WatchOperation'))
    SET IDENTITY_INSERT dbo.WatchOperation ON

WHILE @lo <= @maxId
BEGIN
    SET @sql =
        'INSERT INTO dbo.WatchOperation (' + @WatchOperationCols + ')
         SELECT ' + @WatchOperationCols + '
         FROM dbo.Keep_WatchOperation
         WHERE _BatchID BETWEEN ' + CAST(@lo AS VARCHAR) + ' AND ' + CAST(@lo + @BatchSize - 1 AS VARCHAR) + ';'
    EXEC sp_executesql @sql
    SET @reloadRc = @@ROWCOUNT
    SET @reloadTotal += @reloadRc
    SET @lo = @lo + @BatchSize
    RAISERROR('  %I64d reloaded so far...', 0, 1, @reloadTotal) WITH NOWAIT
END

IF EXISTS (SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID('dbo.WatchOperation'))
    SET IDENTITY_INSERT dbo.WatchOperation OFF

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  WatchOperation done: %I64d rows (%ds)', 0, 1, @reloadTotal, @sec) WITH NOWAIT

-- ── Recreate FK ───────────────────────────────────────────
RAISERROR('Recreating FK...', 0, 1) WITH NOWAIT
-- Use the standard OIM FK name pattern; adjust if yours differs
IF NOT EXISTS (
    SELECT 1 FROM sys.foreign_keys
    WHERE parent_object_id = OBJECT_ID('dbo.WatchProperty')
      AND referenced_object_id = OBJECT_ID('dbo.WatchOperation')
)
BEGIN
    ALTER TABLE dbo.WatchProperty
    WITH CHECK ADD CONSTRAINT FK_WatchProperty_WatchOperation
    FOREIGN KEY (UID_DialogWatchOperation)
    REFERENCES dbo.WatchOperation (UID_DialogWatchOperation)

    ALTER TABLE dbo.WatchProperty
    CHECK CONSTRAINT FK_WatchProperty_WatchOperation
END
RAISERROR('FK created.', 0, 1) WITH NOWAIT

-- ── Reload WatchProperty ──────────────────────────────────
RAISERROR('Reloading WatchProperty...', 0, 1) WITH NOWAIT
SET @st = GETDATE()
SET @reloadTotal = 0
SET @lo = 1

EXEC sp_executesql N'SELECT @m = MAX(_BatchID) FROM dbo.Keep_WatchProperty', N'@m INT OUTPUT', @m = @maxId OUTPUT

IF EXISTS (SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID('dbo.WatchProperty'))
    SET IDENTITY_INSERT dbo.WatchProperty ON

WHILE @lo <= @maxId
BEGIN
    SET @sql =
        'INSERT INTO dbo.WatchProperty (' + @WatchPropertyCols + ')
         SELECT ' + @WatchPropertyCols + '
         FROM dbo.Keep_WatchProperty
         WHERE _BatchID BETWEEN ' + CAST(@lo AS VARCHAR) + ' AND ' + CAST(@lo + @BatchSize - 1 AS VARCHAR) + ';'
    EXEC sp_executesql @sql
    SET @reloadRc = @@ROWCOUNT
    SET @reloadTotal += @reloadRc
    SET @lo = @lo + @BatchSize
    RAISERROR('  %I64d reloaded so far...', 0, 1, @reloadTotal) WITH NOWAIT
END

IF EXISTS (SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID('dbo.WatchProperty'))
    SET IDENTITY_INSERT dbo.WatchProperty OFF

SET @sec = DATEDIFF(SECOND, @st, GETDATE())
RAISERROR('  WatchProperty done: %I64d rows (%ds)', 0, 1, @reloadTotal, @sec) WITH NOWAIT

-- ── Verify ────────────────────────────────────────────────
DECLARE @finalWO BIGINT = (SELECT COUNT_BIG(*) FROM dbo.WatchOperation)
DECLARE @finalWP BIGINT = (SELECT COUNT_BIG(*) FROM dbo.WatchProperty)

RAISERROR('Verification:', 0, 1) WITH NOWAIT
RAISERROR('  WatchOperation: live=%I64d, keep=%I64d', 0, 1, @finalWO, @keepWO) WITH NOWAIT
RAISERROR('  WatchProperty:  live=%I64d, keep=%I64d', 0, 1, @finalWP, @keepWP) WITH NOWAIT

IF @finalWO < @keepWO OR @finalWP < @keepWP
BEGIN
    RAISERROR('ERROR: Count mismatch! Keep tables preserved.', 16, 1) WITH NOWAIT
    RETURN
END

RAISERROR('SUCCESS. Dropping Keep tables...', 0, 1) WITH NOWAIT
DROP TABLE dbo.Keep_WatchProperty
DROP TABLE dbo.Keep_WatchOperation

RAISERROR('================================================', 0, 1) WITH NOWAIT
RAISERROR('Recovery complete.', 0, 1) WITH NOWAIT
GO
