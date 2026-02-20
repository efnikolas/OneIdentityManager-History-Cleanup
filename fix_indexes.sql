-- ============================================================
-- OneIM HDB — Post-Cleanup Index & Statistics Maintenance
-- ============================================================
-- Analyzes fragmentation on HDB tables and intelligently
-- rebuilds (>30%) or reorganizes (5-30%) the indexes.
-- Automatically updates statistics afterwards.
-- ============================================================

USE [OneIMHDB3]; -- CHANGE THIS TO YOUR ACTUAL HDB NAME
GO
SET NOCOUNT ON;

PRINT '================================================';
PRINT 'HDB Post-Cleanup Index Maintenance';
PRINT '================================================';

-- 1. Define the target tables from the cleanup process
DECLARE @TargetTables TABLE (TableName NVARCHAR(128));
INSERT INTO @TargetTables (TableName)
VALUES ('WatchProperty'), ('WatchOperation'), ('ProcessStep'),
       ('ProcessSubstitute'), ('ProcessChain'), ('HistoryJob'),
       ('HistoryChain'), ('ProcessInfo'), ('ProcessGroup');

DECLARE @SchemaName NVARCHAR(128) = 'dbo';
DECLARE @TableName NVARCHAR(128);
DECLARE @IndexName NVARCHAR(128);
DECLARE @Fragmentation FLOAT;
DECLARE @SQL NVARCHAR(MAX);

RAISERROR('Analyzing current index fragmentation... (This may take a moment)', 0, 1) WITH NOWAIT;

-- 2. Cursor to evaluate index fragmentation using standard MS thresholds
DECLARE curIndexes CURSOR LOCAL FAST_FORWARD FOR
SELECT 
    t.name AS TableName,
    i.name AS IndexName,
    ps.avg_fragmentation_in_percent
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'SAMPLED') ps
INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN @TargetTables tt ON t.name = tt.TableName
WHERE ps.avg_fragmentation_in_percent >= 5.0 
  AND i.index_id > 0 -- Ignore heaps (tables without clustered indexes)
  AND ps.alloc_unit_type_desc = 'IN_ROW_DATA';

OPEN curIndexes;
FETCH NEXT FROM curIndexes INTO @TableName, @IndexName, @Fragmentation;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF @Fragmentation >= 30.0
    BEGIN
        -- High fragmentation: Rebuild the index
        SET @SQL = N'ALTER INDEX [' + @IndexName + N'] ON [' + @SchemaName + N'].[' + @TableName + N'] REBUILD;';
        RAISERROR('Rebuilding %s.%s (Fragmentation: %.2f%%)', 0, 1, @TableName, @IndexName, @Fragmentation) WITH NOWAIT;
    END
    ELSE
    BEGIN
        -- Moderate fragmentation: Reorganize the index
        SET @SQL = N'ALTER INDEX [' + @IndexName + N'] ON [' + @SchemaName + N'].[' + @TableName + N'] REORGANIZE;';
        RAISERROR('Reorganizing %s.%s (Fragmentation: %.2f%%)', 0, 1, @TableName, @IndexName, @Fragmentation) WITH NOWAIT;
    END

    -- Execute the dynamically generated maintenance command
    EXEC sp_executesql @SQL;

    FETCH NEXT FROM curIndexes INTO @TableName, @IndexName, @Fragmentation;
END

CLOSE curIndexes;
DEALLOCATE curIndexes;

PRINT '------------------------------------------------';

-- 3. Ensure statistics are updated so the Optimizer knows millions of rows are gone
RAISERROR('Updating statistics on all target tables...', 0, 1) WITH NOWAIT;

DECLARE curStats CURSOR LOCAL FAST_FORWARD FOR SELECT TableName FROM @TargetTables;
OPEN curStats;
FETCH NEXT FROM curStats INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = N'UPDATE STATISTICS [' + @SchemaName + N'].[' + @TableName + N'];';
    RAISERROR('  Updating stats for %s...', 0, 1, @TableName) WITH NOWAIT;
    EXEC sp_executesql @SQL;
    FETCH NEXT FROM curStats INTO @TableName;
END

CLOSE curStats; 
DEALLOCATE curStats;

PRINT '================================================';
PRINT 'Index and Statistics Maintenance Complete.';
PRINT '================================================';
GO
