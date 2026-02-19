-- ============================================================
-- One Identity Manager — History Database (HDB) Cleanup
-- ============================================================
-- Purges archived data older than 2 years from the OIM
-- History Database (TimeTrace database).
--
-- The HDB contains archived process monitoring, change
-- tracking, and job history data — NOT application tables
-- like DialogHistory or PersonWantsOrg (those are in the
-- live application database).
--
-- HDB Tables (from OIM Data Archiving Administration Guide):
--
--   Raw data (bulk):
--     RawJobHistory, RawProcess, RawProcessChain,
--     RawProcessGroup, RawProcessStep, RawProcessSubstitute,
--     RawWatchOperation, RawWatchProperty
--
--   Aggregated data:
--     HistoryChain, HistoryJob, ProcessChain, ProcessGroup,
--     ProcessInfo, ProcessStep, ProcessSubstitute,
--     WatchOperation, WatchProperty
--
--   Metadata (DO NOT DELETE):
--     SourceColumn, SourceDatabase, SourceTable
--
-- ⚠️ BACKUP YOUR DATABASE BEFORE RUNNING THIS SCRIPT
-- ============================================================

-- CHANGE THIS to your actual History Database name
USE [OneIMHDB]
GO

SET NOCOUNT ON
GO

-- ============================================================
-- CONFIGURATION
-- ============================================================
DECLARE @CutoffDate        DATETIME    = DATEADD(YEAR, -2, GETDATE())
DECLARE @BatchSize         INT         = 50000           -- ↑ raised from 10K for throughput
DECLARE @Deleted           INT
DECLARE @WhatIf            BIT         = 0
DECLARE @PreviewLimit      INT         = 0               -- 0 = return all rows in WhatIf preview
DECLARE @BatchDelay        VARCHAR(12) = '00:00:00'      -- pause between batches (HH:MM:SS), e.g. '00:00:01'
DECLARE @CreateTempIndexes BIT         = 1               -- create temp non-clustered indexes on date cols before delete

PRINT '================================================'
PRINT 'OIM History Database Cleanup'
PRINT 'Database:    ' + DB_NAME()
PRINT 'Cutoff date: ' + CONVERT(VARCHAR(20), @CutoffDate, 120)
PRINT 'Batch size:  ' + CAST(@BatchSize AS VARCHAR)
PRINT 'Batch delay: ' + @BatchDelay
PRINT 'Temp indexes:' + CAST(@CreateTempIndexes AS VARCHAR)
PRINT 'WhatIf:      ' + CAST(@WhatIf AS VARCHAR)
PRINT 'PreviewLimit:' + CAST(@PreviewLimit AS VARCHAR)
PRINT '================================================'
PRINT ''

-- ============================================================
-- PRE-FLIGHT: Count rows to purge per table
-- ============================================================
PRINT '# PRE-FLIGHT SUMMARY'
PRINT '================================================'

-- We dynamically find the best date column per table.
-- OIM HDB tables typically use columns like:
--   XDateInserted, XDateUpdated, StartDate, EndDate, etc.
-- We scan for the first available datetime column per table.

DECLARE @TableName   NVARCHAR(256)
DECLARE @DateCol     NVARCHAR(256)
DECLARE @SQL         NVARCHAR(MAX)
DECLARE @RowCount    BIGINT
DECLARE @PurgeCount  BIGINT

-- Tables to clean (everything except Source* metadata and non-HDB tables)
DECLARE @Tables TABLE (
    TableName  NVARCHAR(256),
    DateColumn NVARCHAR(256)
)

-- FK-join delete definitions for tables that have NO date column.
-- These are purged by joining to their parent table's date column.
DECLARE @FKJoinDeletes TABLE (
    ChildTable    NVARCHAR(256),
    ParentTable   NVARCHAR(256),
    ChildFK       NVARCHAR(256),
    ParentPK      NVARCHAR(256),
    ParentDateCol NVARCHAR(256)  -- resolved below
)
INSERT INTO @FKJoinDeletes (ChildTable, ParentTable, ChildFK, ParentPK) VALUES
    ('WatchProperty',       'WatchOperation',  'UID_DialogWatchOperation', 'UID_DialogWatchOperation'),
    ('ProcessSubstitute',   'ProcessInfo',     'UID_ProcessInfoNew',       'UID_ProcessInfo'),
    ('RawWatchProperty',    'RawWatchOperation','UID_DialogWatchOperation', 'UID_DialogWatchOperation'),
    ('RawProcessSubstitute','RawProcess',       'GenProcIDNew',             'GenProcID')

-- Discover date columns for each cleanable table
DECLARE disco_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT t.name
    FROM sys.tables t
    WHERE t.name NOT IN ('SourceColumn', 'SourceDatabase', 'SourceTable',
                          'nsecauth', 'nsecimport', 'sysdiagrams')
    ORDER BY t.name

OPEN disco_cur
FETCH NEXT FROM disco_cur INTO @TableName
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Find the best date column using OIM HDB-specific priority:
    --   OperationDate (WatchOperation), FirstDate (ProcessGroup/Info/HistoryChain),
    --   XDateInserted (Raw*), ThisDate (ProcessChain/Step), StartAt (HistoryJob/RawJobHistory),
    --   ExportDate (RawProcessGroup), then any other datetime column.
    SET @DateCol = NULL

    SELECT TOP 1 @DateCol = c.name
    FROM sys.columns c
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE c.object_id = OBJECT_ID(@TableName)
      AND ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date')
    ORDER BY
        CASE c.name
            WHEN 'OperationDate'  THEN 1
            WHEN 'FirstDate'      THEN 2
            WHEN 'XDateInserted'  THEN 3
            WHEN 'ThisDate'       THEN 4
            WHEN 'StartAt'        THEN 5
            WHEN 'ExportDate'     THEN 6
            WHEN 'XDateUpdated'   THEN 7
            ELSE 10
        END,
        c.column_id

    IF @DateCol IS NOT NULL
        INSERT INTO @Tables (TableName, DateColumn) VALUES (@TableName, @DateCol)
    ELSE IF EXISTS (SELECT 1 FROM @FKJoinDeletes WHERE ChildTable = @TableName)
        PRINT '  INFO ' + @TableName + ' has no date column — will purge via FK join'
    ELSE
        PRINT '  SKIP ' + @TableName + ' (no date column found)'

    FETCH NEXT FROM disco_cur INTO @TableName
END
CLOSE disco_cur
DEALLOCATE disco_cur

-- Resolve parent date columns for FK-join tables
UPDATE fk
SET fk.ParentDateCol = t.DateColumn
FROM @FKJoinDeletes fk
INNER JOIN @Tables t ON fk.ParentTable = t.TableName

-- Show pre-flight counts — tables with their own date column
DECLARE preflight_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT TableName, DateColumn FROM @Tables ORDER BY TableName

OPEN preflight_cur
FETCH NEXT FROM preflight_cur INTO @TableName, @DateCol
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = N'SELECT @total = COUNT(*), @old = ISNULL(SUM(CASE WHEN [' + @DateCol + N'] < @cutoff THEN 1 ELSE 0 END), 0) FROM [' + @TableName + N']'
    EXEC sp_executesql @SQL,
        N'@cutoff DATETIME, @total BIGINT OUTPUT, @old BIGINT OUTPUT',
        @CutoffDate, @RowCount OUTPUT, @PurgeCount OUTPUT

    PRINT '  ' + @TableName + ': ' + CAST(@RowCount AS VARCHAR) + ' total, ' + CAST(@PurgeCount AS VARCHAR) + ' to purge (by ' + @DateCol + ')'

    FETCH NEXT FROM preflight_cur INTO @TableName, @DateCol
END
CLOSE preflight_cur
DEALLOCATE preflight_cur

-- Show pre-flight counts — FK-joined tables (no date column)
DECLARE @FKChild   NVARCHAR(256)
DECLARE @FKParent  NVARCHAR(256)
DECLARE @FKChildCol NVARCHAR(256)
DECLARE @FKParentCol NVARCHAR(256)
DECLARE @FKDateCol  NVARCHAR(256)

DECLARE fk_pre CURSOR LOCAL FAST_FORWARD FOR
    SELECT ChildTable, ParentTable, ChildFK, ParentPK, ParentDateCol
    FROM @FKJoinDeletes WHERE ParentDateCol IS NOT NULL ORDER BY ChildTable

OPEN fk_pre
FETCH NEXT FROM fk_pre INTO @FKChild, @FKParent, @FKChildCol, @FKParentCol, @FKDateCol
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(@FKChild, 'U') IS NOT NULL
    BEGIN
        SET @SQL = N'SELECT @total = (SELECT COUNT(*) FROM [' + @FKChild + N']), ' +
            N'@old = (SELECT COUNT(*) FROM [' + @FKChild + N'] child ' +
            N'INNER JOIN [' + @FKParent + N'] parent ON child.[' + @FKChildCol + N'] = parent.[' + @FKParentCol + N'] ' +
            N'WHERE parent.[' + @FKDateCol + N'] < @cutoff)'
        EXEC sp_executesql @SQL,
            N'@cutoff DATETIME, @total BIGINT OUTPUT, @old BIGINT OUTPUT',
            @CutoffDate, @RowCount OUTPUT, @PurgeCount OUTPUT
        PRINT '  ' + @FKChild + ': ' + CAST(@RowCount AS VARCHAR) + ' total, ' + CAST(@PurgeCount AS VARCHAR) + ' to purge (via ' + @FKParent + '.' + @FKDateCol + ')'
    END
    FETCH NEXT FROM fk_pre INTO @FKChild, @FKParent, @FKChildCol, @FKParentCol, @FKDateCol
END
CLOSE fk_pre
DEALLOCATE fk_pre

PRINT ''
PRINT '================================================'
PRINT '# CLEANUP'
PRINT '================================================'

-- ============================================================
-- HELPER: Create temp indexes on date columns to avoid
-- repeated table scans during batched deletes.
-- ============================================================
DECLARE @IdxSQL NVARCHAR(MAX)
DECLARE @TempIndexes TABLE (IndexName NVARCHAR(256), TableName NVARCHAR(256))

IF @WhatIf = 0 AND @CreateTempIndexes = 1
BEGIN
    DECLARE idx_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT TableName, DateColumn FROM @Tables

    OPEN idx_cur
    FETCH NEXT FROM idx_cur INTO @TableName, @DateCol
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Only create if no existing index has this date column as leading key
        IF NOT EXISTS (
            SELECT 1 FROM sys.index_columns ic
            INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            WHERE ic.object_id = OBJECT_ID(@TableName)
              AND ic.column_id = (
                  SELECT column_id FROM sys.columns
                  WHERE object_id = OBJECT_ID(@TableName) AND name = @DateCol
              )
              AND ic.key_ordinal = 1
        )
        BEGIN
            DECLARE @IdxName NVARCHAR(256) = N'IX_Cleanup_' + @TableName + N'_' + @DateCol
            PRINT '  Creating temp index ' + @IdxName + '...'
            SET @IdxSQL = N'CREATE NONCLUSTERED INDEX [' + @IdxName + N'] ON [' + @TableName + N'] ([' + @DateCol + N'])'
            BEGIN TRY
                EXEC sp_executesql @IdxSQL
                INSERT INTO @TempIndexes (IndexName, TableName) VALUES (@IdxName, @TableName)
                PRINT '  OK ' + @IdxName
            END TRY
            BEGIN CATCH
                PRINT '  WARN Could not create index ' + @IdxName + ': ' + ERROR_MESSAGE()
            END CATCH
        END
        ELSE
            PRINT '  Index already exists on ' + @TableName + '.' + @DateCol + ' -- skipping'

        FETCH NEXT FROM idx_cur INTO @TableName, @DateCol
    END
    CLOSE idx_cur
    DEALLOCATE idx_cur

    PRINT ''
END

-- ============================================================
-- DELETE in dependency-safe order
-- Child tables (Raw*) before aggregated parent tables.
-- WatchProperty before WatchOperation, etc.
-- ============================================================

-- Define delete order (children first, parents last)
DECLARE @DeleteOrder TABLE (
    Seq        INT IDENTITY(1,1),
    TableName  NVARCHAR(256)
)

-- Raw tables first (children of aggregated tables)
INSERT INTO @DeleteOrder (TableName) VALUES ('RawWatchProperty')
INSERT INTO @DeleteOrder (TableName) VALUES ('RawWatchOperation')
INSERT INTO @DeleteOrder (TableName) VALUES ('RawProcessStep')
INSERT INTO @DeleteOrder (TableName) VALUES ('RawProcessSubstitute')
INSERT INTO @DeleteOrder (TableName) VALUES ('RawProcessChain')
INSERT INTO @DeleteOrder (TableName) VALUES ('RawProcess')
INSERT INTO @DeleteOrder (TableName) VALUES ('RawProcessGroup')
INSERT INTO @DeleteOrder (TableName) VALUES ('RawJobHistory')
-- Aggregated tables (parents)
INSERT INTO @DeleteOrder (TableName) VALUES ('WatchProperty')
INSERT INTO @DeleteOrder (TableName) VALUES ('WatchOperation')
INSERT INTO @DeleteOrder (TableName) VALUES ('ProcessStep')
INSERT INTO @DeleteOrder (TableName) VALUES ('ProcessSubstitute')
INSERT INTO @DeleteOrder (TableName) VALUES ('ProcessChain')
INSERT INTO @DeleteOrder (TableName) VALUES ('HistoryJob')
INSERT INTO @DeleteOrder (TableName) VALUES ('HistoryChain')
INSERT INTO @DeleteOrder (TableName) VALUES ('ProcessInfo')
INSERT INTO @DeleteOrder (TableName) VALUES ('ProcessGroup')

-- ============================================================
-- WHATIF PREVIEW (optional)
-- ============================================================
IF @WhatIf = 1
BEGIN
    PRINT ''
    PRINT '================================================'
    PRINT '# WHATIF PREVIEW — Rows that WOULD be deleted'
    PRINT '================================================'

    DECLARE @TopClause NVARCHAR(50) = CASE WHEN @PreviewLimit > 0
        THEN 'TOP (' + CAST(@PreviewLimit AS NVARCHAR(20)) + ') '
        ELSE '' END

    DECLARE preview_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT TableName FROM @DeleteOrder ORDER BY Seq

    OPEN preview_cur
    FETCH NEXT FROM preview_cur INTO @TableName
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF OBJECT_ID(@TableName, 'U') IS NULL
        BEGIN
            FETCH NEXT FROM preview_cur INTO @TableName
            CONTINUE
        END

        -- Determine date column for direct delete preview
        SET @DateCol = NULL
        SELECT @DateCol = DateColumn FROM @Tables WHERE TableName = @TableName

        -- FK-join preview
        IF EXISTS (SELECT 1 FROM @FKJoinDeletes WHERE ChildTable = @TableName AND ParentDateCol IS NOT NULL)
        BEGIN
            SELECT @FKChild = ChildTable, @FKParent = ParentTable,
                   @FKChildCol = ChildFK, @FKParentCol = ParentPK,
                   @FKDateCol = ParentDateCol
            FROM @FKJoinDeletes WHERE ChildTable = @TableName

            PRINT 'Preview ' + @TableName + ' (FK join -> ' + @FKParent + '.' + @FKDateCol + ' < cutoff)...'
            SET @SQL = N'SELECT ' + @TopClause + N'''' + @TableName + N''' AS TableName, child.*, parent.[' + @FKDateCol + N'] AS ParentDate ' +
                N'FROM [' + @TableName + N'] child ' +
                N'INNER JOIN [' + @FKParent + N'] parent ON child.[' + @FKChildCol + N'] = parent.[' + @FKParentCol + N'] ' +
                N'WHERE parent.[' + @FKDateCol + N'] < @cutoff ' +
                N'ORDER BY parent.[' + @FKDateCol + N'] ASC'
            EXEC sp_executesql @SQL, N'@cutoff DATETIME', @CutoffDate
        END
        ELSE IF @DateCol IS NOT NULL
        BEGIN
            PRINT 'Preview ' + @TableName + ' (WHERE ' + @DateCol + ' < cutoff)...'
            SET @SQL = N'SELECT ' + @TopClause + N'''' + @TableName + N''' AS TableName, * FROM [' + @TableName + N'] ' +
                N'WHERE [' + @DateCol + N'] < @cutoff ORDER BY [' + @DateCol + N'] ASC'
            EXEC sp_executesql @SQL, N'@cutoff DATETIME', @CutoffDate
        END
        ELSE
        BEGIN
            PRINT 'SKIP ' + @TableName + ' (no date column and no FK-join rule)'
        END

        FETCH NEXT FROM preview_cur INTO @TableName
    END
    CLOSE preview_cur
    DEALLOCATE preview_cur

    PRINT ''
    PRINT '================================================'
    PRINT 'WhatIf preview complete. No data was deleted.'
    PRINT '================================================'
    RETURN
END

DECLARE @Seq INT = 1
DECLARE @MaxSeq INT = (SELECT MAX(Seq) FROM @DeleteOrder)

WHILE @Seq <= @MaxSeq
BEGIN
    SELECT @TableName = TableName FROM @DeleteOrder WHERE Seq = @Seq

    -- Get the date column for this table
    SET @DateCol = NULL
    SELECT @DateCol = DateColumn FROM @Tables WHERE TableName = @TableName

    IF OBJECT_ID(@TableName, 'U') IS NULL
    BEGIN
        SET @Seq = @Seq + 1
        CONTINUE
    END

    -- Check if this table uses FK-join delete (no date column)
    IF EXISTS (SELECT 1 FROM @FKJoinDeletes WHERE ChildTable = @TableName AND ParentDateCol IS NOT NULL)
    BEGIN
        -- FK-join delete
        SELECT @FKChild = ChildTable, @FKParent = ParentTable,
               @FKChildCol = ChildFK, @FKParentCol = ParentPK,
               @FKDateCol = ParentDateCol
        FROM @FKJoinDeletes WHERE ChildTable = @TableName

        PRINT 'Cleaning ' + @TableName + ' (FK join -> ' + @FKParent + '.' + @FKDateCol + ' < cutoff)...'
        BEGIN TRY
            SET @Deleted = 1
            DECLARE @FKTotalDeleted BIGINT = 0
            DECLARE @FKStartTime   DATETIME = GETDATE()
            WHILE @Deleted > 0
            BEGIN
                SET @SQL = N'DELETE TOP (' + CAST(@BatchSize AS NVARCHAR) + N') child FROM [' + @TableName + N'] child ' +
                    N'INNER JOIN [' + @FKParent + N'] parent ON child.[' + @FKChildCol + N'] = parent.[' + @FKParentCol + N'] ' +
                    N'WHERE parent.[' + @FKDateCol + N'] < @cutoff'
                EXEC sp_executesql @SQL, N'@cutoff DATETIME', @CutoffDate
                SET @Deleted = @@ROWCOUNT
                SET @FKTotalDeleted = @FKTotalDeleted + @Deleted
                IF @Deleted > 0
                BEGIN
                    CHECKPOINT
                    DECLARE @FKElapsedSec INT = DATEDIFF(SECOND, @FKStartTime, GETDATE())
                    DECLARE @FKRate       BIGINT = CASE WHEN @FKElapsedSec > 0 THEN @FKTotalDeleted / @FKElapsedSec ELSE 0 END
                    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR) +
                          ' | total: ' + CAST(@FKTotalDeleted AS VARCHAR) +
                          ' | ' + CAST(@FKElapsedSec AS VARCHAR) + 's elapsed' +
                          ' | ~' + CAST(@FKRate AS VARCHAR) + ' rows/sec'
                    IF @BatchDelay <> '00:00:00'
                        WAITFOR DELAY @BatchDelay
                END
            END
            PRINT '  Done. ' + CAST(@FKTotalDeleted AS VARCHAR) + ' rows removed.'
        END TRY
        BEGIN CATCH
            PRINT '  ERROR: ' + ERROR_MESSAGE()
        END CATCH
    END
    ELSE IF @DateCol IS NOT NULL
    BEGIN
        -- Direct date-column delete
        PRINT 'Cleaning ' + @TableName + ' (WHERE ' + @DateCol + ' < cutoff)...'
        BEGIN TRY
            SET @Deleted = 1
            DECLARE @TotalDeleted BIGINT = 0
            DECLARE @StartTime   DATETIME = GETDATE()
            WHILE @Deleted > 0
            BEGIN
                SET @SQL = N'DELETE TOP (' + CAST(@BatchSize AS NVARCHAR) + N') FROM [' + @TableName + N'] WHERE [' + @DateCol + N'] < @cutoff'
                EXEC sp_executesql @SQL, N'@cutoff DATETIME', @CutoffDate
                SET @Deleted = @@ROWCOUNT
                SET @TotalDeleted = @TotalDeleted + @Deleted
                IF @Deleted > 0
                BEGIN
                    CHECKPOINT
                    DECLARE @ElapsedSec INT  = DATEDIFF(SECOND, @StartTime, GETDATE())
                    DECLARE @Rate       BIGINT = CASE WHEN @ElapsedSec > 0 THEN @TotalDeleted / @ElapsedSec ELSE 0 END
                    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR) +
                          ' | total: ' + CAST(@TotalDeleted AS VARCHAR) +
                          ' | ' + CAST(@ElapsedSec AS VARCHAR) + 's elapsed' +
                          ' | ~' + CAST(@Rate AS VARCHAR) + ' rows/sec'
                    IF @BatchDelay <> '00:00:00'
                        WAITFOR DELAY @BatchDelay
                END
            END
            PRINT '  Done. ' + CAST(@TotalDeleted AS VARCHAR) + ' rows removed.'
        END TRY
        BEGIN CATCH
            PRINT '  ERROR: ' + ERROR_MESSAGE()
        END CATCH
    END
    ELSE
    BEGIN
        PRINT 'SKIP ' + @TableName + ' (no date column and no FK-join rule)'
    END

    SET @Seq = @Seq + 1
END

-- ============================================================
-- POST-CLEANUP SUMMARY
-- ============================================================
PRINT ''
PRINT '================================================'
PRINT '# POST-CLEANUP SUMMARY'
PRINT '================================================'

DECLARE post_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT TableName FROM @Tables ORDER BY TableName

OPEN post_cur
FETCH NEXT FROM post_cur INTO @TableName
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = N'SELECT @cnt = COUNT(*) FROM [' + @TableName + N']'
    EXEC sp_executesql @SQL, N'@cnt BIGINT OUTPUT', @RowCount OUTPUT
    PRINT '  ' + @TableName + ': ' + CAST(@RowCount AS VARCHAR) + ' rows remaining'
    FETCH NEXT FROM post_cur INTO @TableName
END
CLOSE post_cur
DEALLOCATE post_cur

-- Also show FK-joined tables
DECLARE fk_post CURSOR LOCAL FAST_FORWARD FOR
    SELECT ChildTable FROM @FKJoinDeletes ORDER BY ChildTable

OPEN fk_post
FETCH NEXT FROM fk_post INTO @FKChild
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(@FKChild, 'U') IS NOT NULL
    BEGIN
        SET @SQL = N'SELECT @cnt = COUNT(*) FROM [' + @FKChild + N']'
        EXEC sp_executesql @SQL, N'@cnt BIGINT OUTPUT', @RowCount OUTPUT
        PRINT '  ' + @FKChild + ': ' + CAST(@RowCount AS VARCHAR) + ' rows remaining (FK-joined)'
    END
    FETCH NEXT FROM fk_post INTO @FKChild
END
CLOSE fk_post
DEALLOCATE fk_post

-- ============================================================
-- DROP TEMP INDEXES
-- ============================================================
DECLARE @DropIdx NVARCHAR(256)
DECLARE @DropTbl NVARCHAR(256)
DECLARE drop_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT IndexName, TableName FROM @TempIndexes
OPEN drop_cur
FETCH NEXT FROM drop_cur INTO @DropIdx, @DropTbl
WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        SET @SQL = N'DROP INDEX [' + @DropIdx + N'] ON [' + @DropTbl + N']'
        EXEC sp_executesql @SQL
        PRINT '  Dropped temp index ' + @DropIdx
    END TRY
    BEGIN CATCH
        PRINT '  WARN Could not drop ' + @DropIdx + ': ' + ERROR_MESSAGE()
    END CATCH
    FETCH NEXT FROM drop_cur INTO @DropIdx, @DropTbl
END
CLOSE drop_cur
DEALLOCATE drop_cur

PRINT ''
PRINT '================================================'
PRINT 'Cleanup complete. Consider running:'
PRINT '  EXEC sp_updatestats'
PRINT '================================================'
GO
