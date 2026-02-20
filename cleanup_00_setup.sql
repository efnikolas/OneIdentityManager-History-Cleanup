-- ============================================================================
-- HDB Cleanup — STEP 0: Pre-cleanup setup
-- ============================================================================
-- Run this ONCE before running any of the per-table cleanup scripts.
-- It switches to SIMPLE recovery and creates indexes for performance.
--
-- IMPORTANT: BACKUP YOUR DATABASE BEFORE RUNNING THIS.
-- ============================================================================

USE [OneIMHDB3]          -- << Set your HDB database name
GO
SET NOCOUNT ON
GO

-- ================================================================
-- 1. Switch to SIMPLE recovery to prevent log bloat
-- ================================================================
DECLARE @Recovery NVARCHAR(60)
SELECT @Recovery = recovery_model_desc FROM sys.databases WHERE name = DB_NAME()

PRINT '# RECOVERY MODEL'
PRINT '  Current: ' + @Recovery

IF @Recovery <> 'SIMPLE'
BEGIN
    PRINT '  Switching to SIMPLE...'
    DECLARE @sql NVARCHAR(200) = 'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + ' SET RECOVERY SIMPLE'
    EXEC sp_executesql @sql
    PRINT '  Done. (Remember to run cleanup_99_finish.sql when all tables are done)'
END
ELSE
    PRINT '  Already SIMPLE — no change needed.'

-- Show log file size
PRINT ''
PRINT '# LOG FILE SIZE'
SELECT
    name AS LogicalName, size / 128 AS SizeMB,
    FILEPROPERTY(name, 'SpaceUsed') / 128 AS UsedMB
FROM sys.database_files WHERE type_desc = 'LOG'

-- ================================================================
-- 2. Create cleanup indexes for performance
-- ================================================================
PRINT ''
PRINT '# CREATING CLEANUP INDEXES (may take several minutes on large tables)...'
PRINT '------------------------------------------------------------'

-- Drop stale indexes from any previous cancelled run
DECLARE @ixDrop NVARCHAR(500)
DECLARE ix_clean CURSOR LOCAL FAST_FORWARD FOR
    SELECT 'DROP INDEX ' + QUOTENAME(i.name) + ' ON ' + QUOTENAME(OBJECT_NAME(i.object_id))
    FROM sys.indexes i
    WHERE i.name LIKE 'IX_Cleanup_%' AND i.is_hypothetical = 0
OPEN ix_clean
FETCH NEXT FROM ix_clean INTO @ixDrop
WHILE @@FETCH_STATUS = 0
BEGIN
    RAISERROR('  Dropping stale: %s', 0, 1, @ixDrop) WITH NOWAIT
    EXEC sp_executesql @ixDrop
    FETCH NEXT FROM ix_clean INTO @ixDrop
END
CLOSE ix_clean; DEALLOCATE ix_clean

-- FK-join indexes
IF OBJECT_ID('WatchProperty','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_WatchProp_FK')
    CREATE NONCLUSTERED INDEX IX_Cleanup_WatchProp_FK ON WatchProperty (UID_DialogWatchOperation)
PRINT '  IX_Cleanup_WatchProp_FK'

IF OBJECT_ID('WatchOperation','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_WatchOp_DateFK')
    CREATE NONCLUSTERED INDEX IX_Cleanup_WatchOp_DateFK ON WatchOperation (OperationDate) INCLUDE (UID_DialogWatchOperation)
PRINT '  IX_Cleanup_WatchOp_DateFK'

IF OBJECT_ID('ProcessSubstitute','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcSub_FK')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcSub_FK ON ProcessSubstitute (UID_ProcessInfoNew)
PRINT '  IX_Cleanup_ProcSub_FK'

IF OBJECT_ID('ProcessInfo','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcInfo_DateFK')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcInfo_DateFK ON ProcessInfo (FirstDate, LastDate) INCLUDE (UID_ProcessInfo)
PRINT '  IX_Cleanup_ProcInfo_DateFK'

-- Date column indexes
IF OBJECT_ID('WatchOperation','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_WatchOp_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_WatchOp_Date ON WatchOperation (OperationDate)
PRINT '  IX_Cleanup_WatchOp_Date'

IF OBJECT_ID('ProcessStep','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcStep_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcStep_Date ON ProcessStep (ThisDate)
PRINT '  IX_Cleanup_ProcStep_Date'

IF OBJECT_ID('ProcessChain','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcChain_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcChain_Date ON ProcessChain (ThisDate)
PRINT '  IX_Cleanup_ProcChain_Date'

IF OBJECT_ID('HistoryJob','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_HistJob_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_HistJob_Date ON HistoryJob (StartAt)
PRINT '  IX_Cleanup_HistJob_Date'

IF OBJECT_ID('HistoryChain','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_HistChain_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_HistChain_Date ON HistoryChain (FirstDate, LastDate)
PRINT '  IX_Cleanup_HistChain_Date'

IF OBJECT_ID('ProcessInfo','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcInfo_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcInfo_Date ON ProcessInfo (FirstDate, LastDate)
PRINT '  IX_Cleanup_ProcInfo_Date'

IF OBJECT_ID('ProcessGroup','U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Cleanup_ProcGroup_Date')
    CREATE NONCLUSTERED INDEX IX_Cleanup_ProcGroup_Date ON ProcessGroup (FirstDate, LastDate, ExportDate)
PRINT '  IX_Cleanup_ProcGroup_Date'

PRINT ''
PRINT '# PRE-FLIGHT — Estimated row counts'
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

PRINT ''
PRINT '============================================================'
PRINT ' Setup complete. Now run the per-table scripts in order:'
PRINT ''
PRINT '   MUST run first (FK children):'
PRINT '     cleanup_01_WatchProperty.sql'
PRINT '     cleanup_04_ProcessSubstitute.sql'
PRINT ''
PRINT '   Then their parents + independent tables (any order):'
PRINT '     cleanup_02_WatchOperation.sql'
PRINT '     cleanup_03_ProcessStep.sql'
PRINT '     cleanup_05_ProcessChain.sql'
PRINT '     cleanup_06_HistoryJob.sql'
PRINT '     cleanup_07_HistoryChain.sql'
PRINT ''
PRINT '   Then parents last:'
PRINT '     cleanup_08_ProcessInfo.sql     (after 04)'
PRINT '     cleanup_09_ProcessGroup.sql    (after 08)'
PRINT ''
PRINT '   Finally:'
PRINT '     cleanup_99_finish.sql'
PRINT '============================================================'
GO
