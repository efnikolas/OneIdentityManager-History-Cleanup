-- ============================================================================
-- HDB Cleanup — STEP 99: Post-cleanup finish
-- ============================================================================
-- Run this AFTER all per-table cleanup scripts are done.
-- It drops the temporary indexes, restores recovery model, shows final counts.
-- ============================================================================

USE [OneIMHDB3]          -- << Set your HDB database name
GO
SET NOCOUNT ON
GO

-- ================================================================
-- 1. Drop cleanup indexes
-- ================================================================
PRINT '# DROPPING CLEANUP INDEXES...'
PRINT '------------------------------------------------------------'

DECLARE @ixDrop NVARCHAR(500)
DECLARE ix_clean CURSOR LOCAL FAST_FORWARD FOR
    SELECT 'DROP INDEX ' + QUOTENAME(i.name) + ' ON ' + QUOTENAME(OBJECT_NAME(i.object_id))
    FROM sys.indexes i
    WHERE i.name LIKE 'IX_Cleanup_%' AND i.is_hypothetical = 0
OPEN ix_clean
FETCH NEXT FROM ix_clean INTO @ixDrop
WHILE @@FETCH_STATUS = 0
BEGIN
    RAISERROR('  %s', 0, 1, @ixDrop) WITH NOWAIT
    EXEC sp_executesql @ixDrop
    FETCH NEXT FROM ix_clean INTO @ixDrop
END
CLOSE ix_clean; DEALLOCATE ix_clean
PRINT '  Done.'

-- ================================================================
-- 2. Restore recovery model to FULL
-- ================================================================
PRINT ''
PRINT '# RECOVERY MODEL'
DECLARE @Recovery NVARCHAR(60)
SELECT @Recovery = recovery_model_desc FROM sys.databases WHERE name = DB_NAME()
PRINT '  Current: ' + @Recovery

IF @Recovery = 'SIMPLE'
BEGIN
    PRINT '  Switching back to FULL...'
    DECLARE @sql NVARCHAR(200) = 'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + ' SET RECOVERY FULL'
    EXEC sp_executesql @sql
    PRINT '  Done.'
END
ELSE
    PRINT '  Already ' + @Recovery + ' — no change needed.'

-- ================================================================
-- 3. Final row counts
-- ================================================================
PRINT ''
PRINT '# FINAL ROW COUNTS'
PRINT '------------------------------------------------------------'

DECLARE @tbl NVARCHAR(128), @est BIGINT
DECLARE @tables TABLE (name NVARCHAR(128), seq INT)
INSERT @tables VALUES
    ('WatchProperty',1),('WatchOperation',2),('ProcessStep',3),
    ('ProcessSubstitute',4),('ProcessChain',5),('HistoryJob',6),
    ('HistoryChain',7),('ProcessInfo',8),('ProcessGroup',9)

DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM @tables ORDER BY seq
OPEN c
FETCH NEXT FROM c INTO @tbl
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @est = SUM(row_count) FROM sys.dm_db_partition_stats
    WHERE object_id = OBJECT_ID(@tbl) AND index_id IN (0,1)
    RAISERROR('  %-25s ~%I64d rows', 0, 1, @tbl, @est) WITH NOWAIT
    FETCH NEXT FROM c INTO @tbl
END
CLOSE c; DEALLOCATE c

-- ================================================================
-- 4. Log file size
-- ================================================================
PRINT ''
PRINT '# LOG FILE SIZE'
SELECT
    name AS LogicalName, size / 128 AS SizeMB,
    FILEPROPERTY(name, 'SpaceUsed') / 128 AS UsedMB
FROM sys.database_files WHERE type_desc = 'LOG'

PRINT ''
PRINT '============================================================'
PRINT ' Cleanup complete.'
PRINT ''
PRINT ' IMPORTANT: Take a FULL BACKUP now to restart the log chain!'
PRINT '============================================================'
GO
