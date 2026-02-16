-- ============================================================
-- One Identity Manager — Purge History Data Older Than 2 Years
-- ============================================================
-- ⚠️ BACKUP YOUR DATABASE BEFORE RUNNING THIS SCRIPT!
-- Run against the One Identity Manager HISTORY DATABASE
-- (the separate history/archive database, NOT the live
--  application database). All data here is historical —
-- no active/live records will be affected.
--
-- Features:
--   • Pre-flight summary (counts only, no data touched)
--   • Batched deletes with CHECKPOINT to control log growth
--   • FK-safe ordering (children before parents)
--   • Orphan cleanup for DialogJournalDetail
--   • PersonWantsOrg child tables handled (PWOHelperPWO, etc.)
--   • Dynamic scan for any additional *History* tables
--   • Error handling per table — one failure won't stop the rest
-- ============================================================

DECLARE @CutoffDate DATETIME = DATEADD(YEAR, -2, GETDATE());
DECLARE @BatchSize  INT      = 10000;
DECLARE @Deleted    INT      = 1;

PRINT '================================================';
PRINT 'Cutoff date: ' + CONVERT(VARCHAR, @CutoffDate, 120);
PRINT 'Batch size:  ' + CAST(@BatchSize AS VARCHAR);
PRINT '================================================';
PRINT '';

-- ============================================================
-- PRE-FLIGHT SUMMARY — counts only, nothing deleted
-- ============================================================
PRINT '#  PRE-FLIGHT SUMMARY';
PRINT '================================================';

SELECT 'DialogHistory' AS TableName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END) AS RowsToPurge,
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END) AS RowsToKeep
FROM DialogHistory
UNION ALL
SELECT 'DialogJournal',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM DialogJournal
UNION ALL
SELECT 'DialogJournalDetail',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM DialogJournalDetail
UNION ALL
SELECT 'JobHistory',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM JobHistory
UNION ALL
SELECT 'PersonWantsOrg',
    COUNT(*),
    SUM(CASE WHEN XDateUpdated < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateUpdated >= @CutoffDate THEN 1 ELSE 0 END)
FROM PersonWantsOrg
UNION ALL
SELECT 'QBMDBQueueHistory',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM QBMDBQueueHistory
UNION ALL
SELECT 'QBMProcessHistory',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM QBMProcessHistory
UNION ALL
SELECT 'QBMDBQueueSlotHistory',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM QBMDBQueueSlotHistory
ORDER BY TableName;

PRINT '';
PRINT '================================================';
PRINT '#  STARTING CLEANUP';
PRINT '================================================';

-- ── 1. DialogHistory (archived UI/process history) ─────
PRINT 'Cleaning DialogHistory...';
BEGIN TRY
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM DialogHistory
        WHERE XDateInserted < @CutoffDate;
        SET @Deleted = @@ROWCOUNT;
        IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
    END
    CHECKPOINT;
    PRINT '  Done.';
END TRY
BEGIN CATCH
    PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
END CATCH

-- ── 2. DialogJournalDetail (purge before parent table) ─
-- Must be cleaned BEFORE DialogJournal to respect FK.
PRINT 'Cleaning DialogJournalDetail (by parent age)...';
BEGIN TRY
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) djd
        FROM DialogJournalDetail djd
        INNER JOIN DialogJournal dj ON djd.UID_DialogJournal = dj.UID_DialogJournal
        WHERE dj.XDateInserted < @CutoffDate;
        SET @Deleted = @@ROWCOUNT;
        IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
    END
    CHECKPOINT;
    PRINT '  Done.';
END TRY
BEGIN CATCH
    PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
END CATCH

-- ── 3. DialogJournal (archived change journal / audit) ─
PRINT 'Cleaning DialogJournal...';
BEGIN TRY
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM DialogJournal
        WHERE XDateInserted < @CutoffDate;
        SET @Deleted = @@ROWCOUNT;
        IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
    END
    CHECKPOINT;
    PRINT '  Done.';
END TRY
BEGIN CATCH
    PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
END CATCH

-- ── 3b. DialogJournalDetail (orphaned records) ─────────
-- Clean up any remaining orphans left from prior runs.
PRINT 'Cleaning orphaned DialogJournalDetail...';
BEGIN TRY
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) djd
        FROM DialogJournalDetail djd
        LEFT JOIN DialogJournal dj ON djd.UID_DialogJournal = dj.UID_DialogJournal
        WHERE dj.UID_DialogJournal IS NULL;
        SET @Deleted = @@ROWCOUNT;
        IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
    END
    CHECKPOINT;
    PRINT '  Done.';
END TRY
BEGIN CATCH
    PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
END CATCH

-- ── 4. JobHistory (archived job execution history) ─────
PRINT 'Cleaning JobHistory...';
BEGIN TRY
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM JobHistory
        WHERE XDateInserted < @CutoffDate;
        SET @Deleted = @@ROWCOUNT;
        IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
    END
    CHECKPOINT;
    PRINT '  Done.';
END TRY
BEGIN CATCH
    PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
END CATCH

-- ── 5. PersonWantsOrg child tables ─────────────────────
-- OIM may create child tables under PersonWantsOrg. These
-- must be purged FIRST or the parent delete will hit FKs.
-- We check IF each table exists because not all HDBs have them.

-- 5a. PWOHelperPWO (request delegation/helper links)
IF OBJECT_ID('PWOHelperPWO', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning PWOHelperPWO...';
    BEGIN TRY
        SET @Deleted = 1;
        WHILE @Deleted > 0
        BEGIN
            DELETE TOP (@BatchSize) c
            FROM PWOHelperPWO c
            INNER JOIN PersonWantsOrg p ON c.UID_PersonWantsOrg = p.UID_PersonWantsOrg
            WHERE p.XDateUpdated < @CutoffDate;
            SET @Deleted = @@ROWCOUNT;
            IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
        END
        CHECKPOINT;
        PRINT '  Done.';
    END TRY
    BEGIN CATCH
        PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
    END CATCH
END

-- 5b. PersonWantsOrgHasObject (requested resources)
IF OBJECT_ID('PersonWantsOrgHasObject', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning PersonWantsOrgHasObject...';
    BEGIN TRY
        SET @Deleted = 1;
        WHILE @Deleted > 0
        BEGIN
            DELETE TOP (@BatchSize) c
            FROM PersonWantsOrgHasObject c
            INNER JOIN PersonWantsOrg p ON c.UID_PersonWantsOrg = p.UID_PersonWantsOrg
            WHERE p.XDateUpdated < @CutoffDate;
            SET @Deleted = @@ROWCOUNT;
            IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
        END
        CHECKPOINT;
        PRINT '  Done.';
    END TRY
    BEGIN CATCH
        PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
    END CATCH
END

-- 5c. PWODecisionStep (approval step history)
IF OBJECT_ID('PWODecisionStep', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning PWODecisionStep...';
    BEGIN TRY
        SET @Deleted = 1;
        WHILE @Deleted > 0
        BEGIN
            DELETE TOP (@BatchSize) c
            FROM PWODecisionStep c
            INNER JOIN PersonWantsOrg p ON c.UID_PersonWantsOrg = p.UID_PersonWantsOrg
            WHERE p.XDateUpdated < @Í;
            SET @Deleted = @@ROWCOUNT;
            IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
        END
        CHECKPOINT;
        PRINT '  Done.';
    END TRY
    BEGIN CATCH
        PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
    END CATCH
END

-- 5d. PWODecisionHistory (approval decision records)
IF OBJECT_ID('PWODecisionHistory', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning PWODecisionHistory...';
    BEGIN TRY
        SET @Deleted = 1;
        WHILE @Deleted > 0
        BEGIN
            DELETE TOP (@BatchSize) c
            FROM PWODecisionHistory c
            INNER JOIN PersonWantsOrg p ON c.UID_PersonWantsOrg = p.UID_PersonWantsOrg
            WHERE p.XDateUpdated < @CutoffDate;
            SET @Deleted = @@ROWCOUNT;
            IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
        END
        CHECKPOINT;
        PRINT '  Done.';
    END TRY
    BEGIN CATCH
        PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
    END CATCH
END

-- 5e. PWORulerOfStep (rule-based step assignments)
IF OBJECT_ID('PWORulerOfStep', 'U') IS NOT NULL
BEGIN
    PRINT 'Cleaning PWORulerOfStep...';
    BEGIN TRY
        SET @Deleted = 1;
        WHILE @Deleted > 0
        BEGIN
            DELETE TOP (@BatchSize) c
            FROM PWORulerOfStep c
            INNER JOIN PersonWantsOrg p ON c.UID_PersonWantsOrg = p.UID_PersonWantsOrg
            WHERE p.XDateUpdated < @CutoffDate;
            SET @Deleted = @@ROWCOUNT;
            IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
        END
        CHECKPOINT;
        PRINT '  Done.';
    END TRY
    BEGIN CATCH
        PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
    END CATCH
END

-- ── 5f. PersonWantsOrg (archived request history) ──────
-- All records in the HDB are historical/completed.
-- Safe to purge by age without filtering on OrderState.
PRINT 'Cleaning PersonWantsOrg...';
BEGIN TRY
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM PersonWantsOrg
        WHERE XDateUpdated < @CutoffDate;
        SET @Deleted = @@ROWCOUNT;
        IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
    END
    CHECKPOINT;
    PRINT '  Done.';
END TRY
BEGIN CATCH
    PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
END CATCH

-- ── 6. QBMDBQueueHistory (archived DBQueue history) ────
PRINT 'Cleaning QBMDBQueueHistory...';
BEGIN TRY
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM QBMDBQueueHistory
        WHERE XDateInserted < @CutoffDate;
        SET @Deleted = @@ROWCOUNT;
        IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
    END
    CHECKPOINT;
    PRINT '  Done.';
END TRY
BEGIN CATCH
    PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
END CATCH

-- ── 7. QBMProcessHistory (archived process logs) ───────
PRINT 'Cleaning QBMProcessHistory...';
BEGIN TRY
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM QBMProcessHistory
        WHERE XDateInserted < @CutoffDate;
        SET @Deleted = @@ROWCOUNT;
        IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
    END
    CHECKPOINT;
    PRINT '  Done.';
END TRY
BEGIN CATCH
    PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
END CATCH

-- ── 8. QBMDBQueueSlotHistory (archived slot history) ───
PRINT 'Cleaning QBMDBQueueSlotHistory...';
BEGIN TRY
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@BatchSize) FROM QBMDBQueueSlotHistory
        WHERE XDateInserted < @CutoffDate;
        SET @Deleted = @@ROWCOUNT;
        IF @Deleted > 0 PRINT '  Deleted batch: ' + CAST(@Deleted AS VARCHAR);
    END
    CHECKPOINT;
    PRINT '  Done.';
END TRY
BEGIN CATCH
    PRINT '  ⚠ ERROR: ' + ERROR_MESSAGE();
END CATCH

-- ── 9. Dynamic: Find ALL remaining history tables ──────
PRINT '';
PRINT '================================================';
PRINT '#  DYNAMIC SCAN — additional history tables';
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
          'PWOHelperPWO', 'PersonWantsOrgHasObject',
          'PWODecisionStep', 'PWODecisionHistory', 'PWORulerOfStep',
          'QBMDBQueueHistory', 'QBMProcessHistory', 'QBMDBQueueSlotHistory'
      )
    ORDER BY t.TABLE_NAME;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
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
            CHECKPOINT;
            PRINT '  Purged ' + @TableName;
            */
        END
        ELSE
        BEGIN
            PRINT 'Table: ' + @TableName + ' — 0 rows (nothing to purge)';
        END
    END TRY
    BEGIN CATCH
        PRINT '  ⚠ ERROR scanning ' + @TableName + ': ' + ERROR_MESSAGE();
    END CATCH

    FETCH NEXT FROM table_cursor INTO @TableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

-- ============================================================
-- POST-CLEANUP SUMMARY
-- ============================================================
PRINT '';
PRINT '================================================';
PRINT '#  POST-CLEANUP SUMMARY';
PRINT '================================================';

SELECT 'DialogHistory' AS TableName, COUNT(*) AS RemainingRows FROM DialogHistory
UNION ALL SELECT 'DialogJournal',       COUNT(*) FROM DialogJournal
UNION ALL SELECT 'DialogJournalDetail', COUNT(*) FROM DialogJournalDetail
UNION ALL SELECT 'JobHistory',          COUNT(*) FROM JobHistory
UNION ALL SELECT 'PersonWantsOrg',      COUNT(*) FROM PersonWantsOrg
UNION ALL SELECT 'QBMDBQueueHistory',   COUNT(*) FROM QBMDBQueueHistory
UNION ALL SELECT 'QBMProcessHistory',   COUNT(*) FROM QBMProcessHistory
UNION ALL SELECT 'QBMDBQueueSlotHistory', COUNT(*) FROM QBMDBQueueSlotHistory
ORDER BY TableName;

PRINT '';
PRINT '================================================';
PRINT 'Cleanup complete. Consider running:';
PRINT '  EXEC sp_updatestats;';
PRINT '  -- and index maintenance on affected tables';
PRINT '================================================';
