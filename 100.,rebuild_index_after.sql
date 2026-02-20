-- ============================================================================
-- HDB Cleanup — Post-Cleanup Index & Statistics Maintenance
-- ============================================================================
-- Run this after the FULL Backup completes.
-- Rebuilds fragmented indexes and updates statistics for the tables that 
-- experienced massive batch deletes.
-- SKIPS the swap tables (WatchProperty, WatchOperation) as they are fresh.
-- ============================================================================

USE [OneIMHDB3]          -- << Set your HDB database name
GO
SET NOCOUNT ON
GO

DECLARE @TableName NVARCHAR(128)
DECLARE @Sql NVARCHAR(MAX)
DECLARE @Start DATETIME

PRINT '============================================================'
PRINT ' Starting Index & Statistics Maintenance'
PRINT '============================================================'
PRINT ''

-- Target only the tables that had batch deletes (skip the swap tables)
DECLARE curTables CURSOR LOCAL FAST_FORWARD FOR 
    SELECT name FROM sys.tables 
    WHERE name IN (
        'ProcessStep', 'ProcessSubstitute', 'ProcessChain', 
        'HistoryJob', 'HistoryChain', 'ProcessInfo', 'ProcessGroup'
    )

OPEN curTables
FETCH NEXT FROM curTables INTO @TableName

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @Start = GETDATE()
    RAISERROR('--- Table: %s ---', 0, 1, @TableName) WITH NOWAIT
    
    -- 1. Rebuild Indexes
    RAISERROR('  Rebuilding indexes (this compacts the data pages)...', 0, 1) WITH NOWAIT
    SET @Sql = 'ALTER INDEX ALL ON ' + QUOTENAME(@TableName) + ' REBUILD;'
    EXEC sp_executesql @Sql

    -- 2. Update Statistics
    RAISERROR('  Updating statistics (FULLSCAN)...', 0, 1) WITH NOWAIT
    SET @Sql = 'UPDATE STATISTICS ' + QUOTENAME(@TableName) + ' WITH FULLSCAN;'
    EXEC sp_executesql @Sql

    RAISERROR('  Finished %s in %d seconds.', 0, 1, @TableName, DATEDIFF(SECOND, @Start, GETDATE())) WITH NOWAIT
    PRINT ''

    FETCH NEXT FROM curTables INTO @TableName
END

CLOSE curTables
DEALLOCATE curTables

PRINT '============================================================'
PRINT ' Maintenance Complete!'
PRINT ' Your database is fully optimized.'
PRINT '============================================================'
GO
