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
DECLARE @BatchSize         INT         = 50000           -- starting default; overridden by benchmark if enabled
DECLARE @Deleted           INT
DECLARE @WhatIf            BIT         = 0
DECLARE @PreviewLimit      INT         = 0               -- 0 = return all rows in WhatIf preview
DECLARE @BatchDelay        VARCHAR(12) = '00:00:00'      -- pause between batches (HH:MM:SS), e.g. '00:00:01'
DECLARE @CreateTempIndexes BIT         = 1               -- create temp non-clustered indexes on date cols before delete
DECLARE @BenchmarkBatchSize BIT        = 1               -- auto-test batch sizes and pick fastest (set 0 to skip)

PRINT '================================================'
PRINT 'OIM History Database Cleanup'
PRINT 'Database:    ' + DB_NAME()
PRINT 'Cutoff date: ' + CONVERT(VARCHAR(20), @CutoffDate, 120)
PRINT 'Batch size:  ' + CAST(@BatchSize AS VARCHAR)
PRINT 'Batch delay: ' + @BatchDelay
PRINT 'Temp indexes:' + CAST(@CreateTempIndexes AS VARCHAR)
PRINT 'Benchmark:   ' + CAST(@BenchmarkBatchSize AS VARCHAR)
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
-- Uses sys.dm_db_partition_stats for instant row counts (no table scan)
-- and MIN/MAX on date column for quick range check.
DECLARE preflight_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT TableName, DateColumn FROM @Tables ORDER BY TableName

OPEN preflight_cur
FETCH NEXT FROM preflight_cur INTO @TableName, @DateCol
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = N'SELECT @total = SUM(p.row_count) ' +
        N'FROM sys.dm_db_partition_stats p ' +
        N'WHERE p.object_id = OBJECT_ID(''' + @TableName + N''') AND p.index_id IN (0,1)'
    EXEC sp_executesql @SQL, N'@total BIGINT OUTPUT', @RowCount OUTPUT

    -- Quick MIN/MAX to show date range without full scan
    DECLARE @MinDate VARCHAR(20) = '?', @MaxDate VARCHAR(20) = '?'
    SET @SQL = N'SELECT @mn = CONVERT(VARCHAR(20), MIN([' + @DateCol + N']), 120), ' +
        N'@mx = CONVERT(VARCHAR(20), MAX([' + @DateCol + N']), 120) FROM [' + @TableName + N']'
    EXEC sp_executesql @SQL,
        N'@mn VARCHAR(20) OUTPUT, @mx VARCHAR(20) OUTPUT',
        @MinDate OUTPUT, @MaxDate OUTPUT

    PRINT '  ' + @TableName + ': ~' + CAST(ISNULL(@RowCount, 0) AS VARCHAR) + ' rows (' + @DateCol + ': ' + ISNULL(@MinDate, '?') + ' to ' + ISNULL(@MaxDate, '?') + ')'

    FETCH NEXT FROM preflight_cur INTO @TableName, @DateCol
END
CLOSE preflight_cur
DEALLOCATE preflight_cur

-- Show pre-flight counts — FK-joined tables (estimated row counts only)
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
        SET @SQL = N'SELECT @total = SUM(p.row_count) ' +
            N'FROM sys.dm_db_partition_stats p ' +
            N'WHERE p.object_id = OBJECT_ID(''' + @FKChild + N''') AND p.index_id IN (0,1)'
        EXEC sp_executesql @SQL, N'@total BIGINT OUTPUT', @RowCount OUTPUT
        PRINT '  ' + @FKChild + ': ~' + CAST(ISNULL(@RowCount, 0) AS VARCHAR) + ' rows (purge via ' + @FKParent + '.' + @FKDateCol + ')'
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
-- BENCHMARK: Test multiple batch sizes on the largest table
-- to find the optimal throughput for this server's hardware.
-- Deletes real rows (they need to go anyway) during the test.
-- ============================================================
IF @WhatIf = 0 AND @BenchmarkBatchSize = 1
BEGIN
    -- Find the table with the most rows to purge (best sample)
    DECLARE @BenchTable   NVARCHAR(256)
    DECLARE @BenchDateCol NVARCHAR(256)
    DECLARE @BenchPurge   BIGINT

    SELECT TOP 1 @BenchTable = t.TableName, @BenchDateCol = t.DateColumn
    FROM @Tables t
    CROSS APPLY (
        SELECT COUNT(*) AS PurgeCount
        FROM sys.objects o
        WHERE o.object_id = OBJECT_ID(t.TableName)
    ) x
    ORDER BY t.TableName  -- placeholder; actual purge count below

    -- Get actual purge count for the largest table
    DECLARE @BenchSQL NVARCHAR(MAX)
    DECLARE @BestRate BIGINT = 0
    DECLARE @BestSize INT   = @BatchSize

    -- Find table with most rows to purge
    DECLARE @BenchCandidates TABLE (TableName NVARCHAR(256), DateColumn NVARCHAR(256), PurgeCount BIGINT)
    DECLARE @BenchTbl NVARCHAR(256), @BenchCol NVARCHAR(256)
    DECLARE @BenchCnt BIGINT

    DECLARE bench_disco CURSOR LOCAL FAST_FORWARD FOR
        SELECT TableName, DateColumn FROM @Tables
    OPEN bench_disco
    FETCH NEXT FROM bench_disco INTO @BenchTbl, @BenchCol
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @BenchSQL = N'SELECT @cnt = COUNT(*) FROM [' + @BenchTbl + N'] WHERE [' + @BenchCol + N'] < @cutoff'
        SET @BenchCnt = 0
        EXEC sp_executesql @BenchSQL, N'@cutoff DATETIME, @cnt BIGINT OUTPUT', @CutoffDate, @BenchCnt OUTPUT
        INSERT INTO @BenchCandidates VALUES (@BenchTbl, @BenchCol, @BenchCnt)
        FETCH NEXT FROM bench_disco INTO @BenchTbl, @BenchCol
    END
    CLOSE bench_disco
    DEALLOCATE bench_disco

    SELECT TOP 1 @BenchTable = TableName, @BenchDateCol = DateColumn, @BenchPurge = PurgeCount
    FROM @BenchCandidates ORDER BY PurgeCount DESC

    IF @BenchPurge >= 100000  -- need enough rows for a meaningful test
    BEGIN
        PRINT ''
        PRINT '================================================'
        PRINT '# BATCH SIZE BENCHMARK'
        PRINT '================================================'
        PRINT 'Target table: ' + @BenchTable + ' (' + CAST(@BenchPurge AS VARCHAR) + ' rows to purge)'
        PRINT 'Date column:  ' + @BenchDateCol
        PRINT ''
        PRINT 'Testing batch sizes (3 trial batches each)...'
        PRINT '------------------------------------------------'

        DECLARE @TestSizes TABLE (TestSize INT)
        INSERT INTO @TestSizes VALUES (5000),(10000),(25000),(50000),(100000),(250000),(500000)

        DECLARE @TestSize      INT
        DECLARE @TrialBatches  INT = 3
        DECLARE @TrialDeleted  INT
        DECLARE @TrialTotal    BIGINT
        DECLARE @TrialStart    DATETIME
        DECLARE @TrialMs       BIGINT
        DECLARE @TrialRate     BIGINT

        DECLARE bench_cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT TestSize FROM @TestSizes ORDER BY TestSize
        OPEN bench_cur
        FETCH NEXT FROM bench_cur INTO @TestSize
        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Skip sizes larger than remaining rows
            SET @BenchSQL = N'SELECT @cnt = COUNT(*) FROM [' + @BenchTable + N'] WHERE [' + @BenchDateCol + N'] < @cutoff'
            SET @BenchCnt = 0
            EXEC sp_executesql @BenchSQL, N'@cutoff DATETIME, @cnt BIGINT OUTPUT', @CutoffDate, @BenchCnt OUTPUT

            IF @BenchCnt < @TestSize
            BEGIN
                PRINT '  ' + CAST(@TestSize AS VARCHAR(10)) + ': SKIP (only ' + CAST(@BenchCnt AS VARCHAR) + ' rows remain)'
                FETCH NEXT FROM bench_cur INTO @TestSize
                CONTINUE
            END

            SET @TrialTotal = 0
            SET @TrialStart = GETDATE()

            DECLARE @Trial INT = 0
            WHILE @Trial < @TrialBatches
            BEGIN
                SET @BenchSQL = N'DELETE TOP (' + CAST(@TestSize AS NVARCHAR) + N') FROM [' + @BenchTable + N'] WHERE [' + @BenchDateCol + N'] < @cutoff'
                EXEC sp_executesql @BenchSQL, N'@cutoff DATETIME', @CutoffDate
                SET @TrialDeleted = @@ROWCOUNT
                SET @TrialTotal = @TrialTotal + @TrialDeleted
                CHECKPOINT
                IF @TrialDeleted = 0 BREAK
                SET @Trial = @Trial + 1
            END

            SET @TrialMs = DATEDIFF(MILLISECOND, @TrialStart, GETDATE())
            SET @TrialRate = CASE WHEN @TrialMs > 0 THEN (@TrialTotal * 1000) / @TrialMs ELSE 0 END

            DECLARE @EstHours VARCHAR(20) = ''
            IF @TrialRate > 0
            BEGIN
                DECLARE @EstSec BIGINT = (@BenchPurge - @TrialTotal) / @TrialRate
                SET @EstHours = CAST(@EstSec / 3600 AS VARCHAR) + 'h ' + CAST((@EstSec % 3600) / 60 AS VARCHAR) + 'm'
            END

            PRINT '  ' + RIGHT('       ' + CAST(@TestSize AS VARCHAR(10)), 7) +
                  ': ' + CAST(@TrialTotal AS VARCHAR) + ' rows in ' + CAST(@TrialMs AS VARCHAR) + 'ms' +
                  ' = ~' + CAST(@TrialRate AS VARCHAR) + ' rows/sec' +
                  '  (est. total: ' + @EstHours + ')'

            IF @TrialRate > @BestRate
            BEGIN
                SET @BestRate = @TrialRate
                SET @BestSize = @TestSize
            END

            FETCH NEXT FROM bench_cur INTO @TestSize
        END
        CLOSE bench_cur
        DEALLOCATE bench_cur

        PRINT '------------------------------------------------'
        PRINT 'Winner: ' + CAST(@BestSize AS VARCHAR) + ' rows/batch (~' + CAST(@BestRate AS VARCHAR) + ' rows/sec)'

        -- Apply the winning batch size
        SET @BatchSize = @BestSize
        PRINT 'Using @BatchSize = ' + CAST(@BatchSize AS VARCHAR) + ' for remaining cleanup.'
        PRINT '================================================'
        PRINT ''
    END
    ELSE
    BEGIN
        PRINT ''
        PRINT 'Benchmark skipped: largest table has < 100K rows to purge (' + CAST(ISNULL(@BenchPurge, 0) AS VARCHAR) + ')'
        PRINT 'Using default @BatchSize = ' + CAST(@BatchSize AS VARCHAR)
        PRINT ''
    END
END

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
        -- FK-join delete: materialize keys into temp table, then delete by key
        SELECT @FKChild = ChildTable, @FKParent = ParentTable,
               @FKChildCol = ChildFK, @FKParentCol = ParentPK,
               @FKDateCol = ParentDateCol
        FROM @FKJoinDeletes WHERE ChildTable = @TableName

        PRINT 'Cleaning ' + @TableName + ' (FK join -> ' + @FKParent + '.' + @FKDateCol + ' < cutoff)...'
        BEGIN TRY
            -- Step 1: Collect all child keys to delete into a temp table (one-time join)
            IF OBJECT_ID('tempdb..#FKKeysToDelete') IS NOT NULL DROP TABLE #FKKeysToDelete

            SET @SQL = N'SELECT child.[' + @FKChildCol + N'] AS KeyVal ' +
                N'INTO #FKKeysToDelete ' +
                N'FROM [' + @TableName + N'] child ' +
                N'INNER JOIN [' + @FKParent + N'] parent ON child.[' + @FKChildCol + N'] = parent.[' + @FKParentCol + N'] ' +
                N'WHERE parent.[' + @FKDateCol + N'] < @cutoff; ' +
                N'SELECT @@ROWCOUNT AS KeyCount'
            DECLARE @KeyCount BIGINT = 0
            -- We need real temp table so we use a different approach: build + exec
            SET @SQL = N'SELECT child.[' + @FKChildCol + N'] AS KeyVal ' +
                N'INTO #FKKeysToDelete ' +
                N'FROM [' + @TableName + N'] child ' +
                N'INNER JOIN [' + @FKParent + N'] parent ON child.[' + @FKChildCol + N'] = parent.[' + @FKParentCol + N'] ' +
                N'WHERE parent.[' + @FKDateCol + N'] < @cutoff; ' +
                N'' +
                N'CREATE CLUSTERED INDEX IX_FKKeys ON #FKKeysToDelete (KeyVal); ' +
                N'' +
                N'DECLARE @d INT = 1, @tot BIGINT = 0, @st DATETIME = GETDATE(); ' +
                N'DECLARE @kc BIGINT = (SELECT COUNT(*) FROM #FKKeysToDelete); ' +
                N'PRINT ''  Collected '' + CAST(@kc AS VARCHAR) + '' keys to delete''; ' +
                N'' +
                N'WHILE @d > 0 ' +
                N'BEGIN ' +
                N'  DELETE TOP (' + CAST(@BatchSize AS NVARCHAR) + N') t ' +
                N'  FROM [' + @TableName + N'] t ' +
                N'  INNER JOIN #FKKeysToDelete k ON t.[' + @FKChildCol + N'] = k.KeyVal; ' +
                N'  SET @d = @@ROWCOUNT; ' +
                N'  SET @tot = @tot + @d; ' +
                N'  IF @d > 0 BEGIN ' +
                N'    CHECKPOINT; ' +
                N'    DECLARE @sec INT = DATEDIFF(SECOND, @st, GETDATE()); ' +
                N'    DECLARE @r BIGINT = CASE WHEN @sec > 0 THEN @tot / @sec ELSE 0 END; ' +
                N'    DECLARE @pct INT = CASE WHEN @kc > 0 THEN (@tot * 100) / @kc ELSE 0 END; ' +
                N'    PRINT ''  Deleted batch: '' + CAST(@d AS VARCHAR) + '' | total: '' + CAST(@tot AS VARCHAR) + ''/'' + CAST(@kc AS VARCHAR) + '' ('' + CAST(@pct AS VARCHAR) + ''%)'' + '' | '' + CAST(@sec AS VARCHAR) + ''s elapsed | ~'' + CAST(@r AS VARCHAR) + '' rows/sec''; ' +
                N'    IF ''' + @BatchDelay + N''' <> ''00:00:00'' WAITFOR DELAY ''' + @BatchDelay + N'''; ' +
                N'  END ' +
                N'END; ' +
                N'PRINT ''  Done. '' + CAST(@tot AS VARCHAR) + '' rows removed.''; ' +
                N'DROP TABLE #FKKeysToDelete;'
            EXEC sp_executesql @SQL, N'@cutoff DATETIME', @CutoffDate
        END TRY
        BEGIN CATCH
            PRINT '  ERROR: ' + ERROR_MESSAGE()
            IF OBJECT_ID('tempdb..#FKKeysToDelete') IS NOT NULL DROP TABLE #FKKeysToDelete
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
