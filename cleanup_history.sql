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

-- ============================================================
-- CONFIGURATION
-- ============================================================
DECLARE @CutoffDate DATETIME = DATEADD(YEAR, -2, GETDATE())
DECLARE @BatchSize  INT      = 10000
DECLARE @Deleted    INT

PRINT '================================================'
PRINT 'OIM History Database Cleanup'
PRINT 'Database:    ' + DB_NAME()
PRINT 'Cutoff date: ' + CONVERT(VARCHAR(20), @CutoffDate, 120)
PRINT 'Batch size:  ' + CAST(@BatchSize AS VARCHAR)
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

-- Tables to clean (everything except Source* metadata)
DECLARE @Tables TABLE (
    TableName  NVARCHAR(256),
    DateColumn NVARCHAR(256)
)

-- Discover date columns for each cleanable table
DECLARE disco_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT t.name
    FROM sys.tables t
    WHERE t.name NOT IN ('SourceColumn', 'SourceDatabase', 'SourceTable')
    ORDER BY t.name

OPEN disco_cur
FETCH NEXT FROM disco_cur INTO @TableName
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Find the best date column (prefer XDateInserted, then others)
    SET @DateCol = NULL

    SELECT TOP 1 @DateCol = c.name
    FROM sys.columns c
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE c.object_id = OBJECT_ID(@TableName)
      AND ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date')
    ORDER BY
        CASE c.name
            WHEN 'XDateInserted' THEN 1
            WHEN 'XDateUpdated'  THEN 2
            WHEN 'StartDate'     THEN 3
            WHEN 'EndDate'       THEN 4
            ELSE 5
        END,
        c.column_id

    IF @DateCol IS NOT NULL
        INSERT INTO @Tables (TableName, DateColumn) VALUES (@TableName, @DateCol)
    ELSE
        PRINT '  SKIP ' + @TableName + ' (no date column found)'

    FETCH NEXT FROM disco_cur INTO @TableName
END
CLOSE disco_cur
DEALLOCATE disco_cur

-- Show pre-flight counts
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

PRINT ''
PRINT '================================================'
PRINT '# CLEANUP'
PRINT '================================================'

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
INSERT INTO @DeleteOrder (TableName) VALUES ('ProcessInfo')
INSERT INTO @DeleteOrder (TableName) VALUES ('ProcessGroup')
INSERT INTO @DeleteOrder (TableName) VALUES ('HistoryJob')
INSERT INTO @DeleteOrder (TableName) VALUES ('HistoryChain')

DECLARE @Seq INT = 1
DECLARE @MaxSeq INT = (SELECT MAX(Seq) FROM @DeleteOrder)

WHILE @Seq <= @MaxSeq
BEGIN
    SELECT @TableName = TableName FROM @DeleteOrder WHERE Seq = @Seq

    -- Get the date column for this table
    SET @DateCol = NULL
    SELECT @DateCol = DateColumn FROM @Tables WHERE TableName = @TableName

    IF @DateCol IS NOT NULL
    BEGIN
        PRINT 'Cleaning ' + @TableName + ' (WHERE ' + @DateCol + ' < cutoff)...'
        BEGIN TRY
            SET @Deleted = 1
            WHILE @Deleted > 0
            BEGIN
                SET @SQL = N'DELETE TOP (' + CAST(@BatchSize AS NVARCHAR) + N') FROM [' + @TableName + N'] WHERE [' + @DateCol + N'] < @cutoff'
                EXEC sp_executesql @SQL, N'@cutoff DATETIME', @CutoffDate
                SET @Deleted = @@ROWCOUNT
                IF @Deleted > 0
                    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR)
            END
            CHECKPOINT
            PRINT '  Done.'
        END TRY
        BEGIN CATCH
            PRINT '  ERROR: ' + ERROR_MESSAGE()
        END CATCH
    END
    ELSE
    BEGIN
        -- Table might not exist in this HDB version — skip silently
        IF OBJECT_ID(@TableName, 'U') IS NOT NULL
            PRINT 'SKIP ' + @TableName + ' (no date column)'
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

PRINT ''
PRINT '================================================'
PRINT 'Cleanup complete. Consider running:'
PRINT '  EXEC sp_updatestats'
PRINT '================================================'
GO
