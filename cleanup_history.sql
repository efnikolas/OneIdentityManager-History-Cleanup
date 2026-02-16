-- ============================================================
-- One Identity Manager — Purge History Data Older Than 2 Years
-- ============================================================
-- ⚠️ BACKUP YOUR DATABASE BEFORE RUNNING THIS SCRIPT!
-- Run against the One Identity Manager HISTORY DATABASE
-- (the separate history/archive database, NOT the live
--  application database). All data here is historical —
-- no active/live records will be affected.
-- ============================================================

DECLARE @CutoffDate DATETIME = DATEADD(YEAR, -2, GETDATE());
DECLARE @BatchSize INT = 10000;
DECLARE @Deleted INT = 1;

PRINT 'Cutoff date: ' + CONVERT(VARCHAR, @CutoffDate, 120);
PRINT '================================================';

-- ── 1. DialogHistory (archived UI/process history) ─────
PRINT 'Cleaning DialogHistory...';
SET @Deleted = 1;
WHILE @Deleted > 0
BEGIN
    DELETE TOP (@BatchSize) FROM DialogHistory
    WHERE XDateInserted < @CutoffDate;
    SET @Deleted = @@ROWCOUNT;
    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
END

-- ── 2. DialogJournalDetail (purge before parent table) ─
-- Must be cleaned BEFORE DialogJournal to respect FK relationship.
PRINT 'Cleaning DialogJournalDetail...';
SET @Deleted = 1;
WHILE @Deleted > 0
BEGIN
    DELETE TOP (@BatchSize) djd
    FROM DialogJournalDetail djd
    INNER JOIN DialogJournal dj ON djd.UID_DialogJournal = dj.UID_DialogJournal
    WHERE dj.XDateInserted < @CutoffDate;
    SET @Deleted = @@ROWCOUNT;
    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
END

-- ── 3. DialogJournal (archived change journal / audit) ─
PRINT 'Cleaning DialogJournal...';
SET @Deleted = 1;
WHILE @Deleted > 0
BEGIN
    DELETE TOP (@BatchSize) FROM DialogJournal
    WHERE XDateInserted < @CutoffDate;
    SET @Deleted = @@ROWCOUNT;
    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
END

-- ── 3b. DialogJournalDetail (orphaned records) ─────────
-- Clean up any remaining orphans left from prior runs.
PRINT 'Cleaning orphaned DialogJournalDetail...';
SET @Deleted = 1;
WHILE @Deleted > 0
BEGIN
    DELETE TOP (@BatchSize) djd
    FROM DialogJournalDetail djd
    LEFT JOIN DialogJournal dj ON djd.UID_DialogJournal = dj.UID_DialogJournal
    WHERE dj.UID_DialogJournal IS NULL;
    SET @Deleted = @@ROWCOUNT;
    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
END

-- ── 4. JobHistory (archived job execution history) ─────
PRINT 'Cleaning JobHistory...';
SET @Deleted = 1;
WHILE @Deleted > 0
BEGIN
    DELETE TOP (@BatchSize) FROM JobHistory
    WHERE XDateInserted < @CutoffDate;
    SET @Deleted = @@ROWCOUNT;
    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
END

-- ── 5. PersonWantsOrg (archived request history) ───────
-- In the history DB all records are already historical/completed.
-- Safe to purge by age without filtering on OrderState.
PRINT 'Cleaning PersonWantsOrg older than 2 years...';
SET @Deleted = 1;
WHILE @Deleted > 0
BEGIN
    DELETE TOP (@BatchSize) FROM PersonWantsOrg
    WHERE XDateUpdated < @CutoffDate;
    SET @Deleted = @@ROWCOUNT;
    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
END

-- ── 6. QBMDBQueueHistory (archived DBQueue history) ────
PRINT 'Cleaning QBMDBQueueHistory...';
SET @Deleted = 1;
WHILE @Deleted > 0
BEGIN
    DELETE TOP (@BatchSize) FROM QBMDBQueueHistory
    WHERE XDateInserted < @CutoffDate;
    SET @Deleted = @@ROWCOUNT;
    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
END

-- ── 7. QBMProcessHistory (archived process logs) ───────
PRINT 'Cleaning QBMProcessHistory...';
SET @Deleted = 1;
WHILE @Deleted > 0
BEGIN
    DELETE TOP (@BatchSize) FROM QBMProcessHistory
    WHERE XDateInserted < @CutoffDate;
    SET @Deleted = @@ROWCOUNT;
    PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
END

-- ── 8. Dynamic: Find ALL remaining history tables ──────
PRINT '';
PRINT '================================================';
PRINT 'Scanning all remaining history tables...';
PRINT '================================================';

DECLARE @TableName NVARCHAR(256);
DECLARE @SQL NVARCHAR(MAX);
DECLARE @Count BIGINT;

DECLARE table_cursor CURSOR FOR
    SELECT t.TABLE_NAME
    FROM INFORMATION_SCHEMA.COLUMNS c
    JOIN INFORMATION_SCHEMA.TABLES t
        ON c.TABLE_NAME = t.TABLE_NAME AND t.TABLE_TYPE = 'BASE TABLE'
    WHERE c.COLUMN_NAME = 'XDateInserted'
      AND t.TABLE_NAME LIKE '%History%'
      AND t.TABLE_NAME NOT IN (
          'DialogHistory', 'DialogJournal', 'DialogJournalDetail',
          'JobHistory', 'PersonWantsOrg',
          'QBMDBQueueHistory', 'QBMProcessHistory'
      )
    ORDER BY t.TABLE_NAME;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Count first (dry run)
    SET @SQL = N'SELECT @cnt = COUNT(*) FROM [' + @TableName + N'] WHERE XDateInserted < @cutoff';
    EXEC sp_executesql @SQL, N'@cutoff DATETIME, @cnt BIGINT OUTPUT', @CutoffDate, @Count OUTPUT;

    IF @Count > 0
    BEGIN
        PRINT 'Table: ' + @TableName + ' — ' + CAST(@Count AS VARCHAR) + ' rows to purge';

        -- ⚠️ UNCOMMENT BELOW TO ACTUALLY DELETE:
        /*
        SET @Deleted = 1;
        WHILE @Deleted > 0
        BEGIN
            SET @SQL = N'DELETE TOP (' + CAST(@BatchSize AS NVARCHAR) + N') FROM [' + @TableName + N'] WHERE XDateInserted < @cutoff';
            EXEC sp_executesql @SQL, N'@cutoff DATETIME', @CutoffDate;
            SET @Deleted = @@ROWCOUNT;
        END
        PRINT '  ✓ Purged ' + @TableName;
        */
    END

    FETCH NEXT FROM table_cursor INTO @TableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

PRINT '';
PRINT '================================================';
PRINT 'Cleanup complete. Consider running:';
PRINT '  EXEC sp_updatestats;';
PRINT '  -- and index maintenance on affected tables';
PRINT '================================================';
