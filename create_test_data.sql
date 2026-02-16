-- ============================================================
-- One Identity Manager — Insert Prefixed Test Data into an HDB
-- ============================================================
-- Inserts test data INTO AN EXISTING History Database with a
-- 'TEST_CLEANUP_' prefix on all text fields so it can be
-- easily identified and removed afterward.
--
-- Usage:
--   1. Change the USE statement below to your target HDB
--   2. Run this script to insert test data
--   3. Run cleanup_history.sql to test the purge
--   4. Run remove_test_data.sql to remove any remaining
--      TEST_CLEANUP_ data (including rows newer than 2 years)
--
-- All test rows use 'TEST_CLEANUP_' as a prefix so they
-- will NOT collide with real historical data.
-- ============================================================

-- ⚠️ CHANGE THIS to your actual History Database name
USE [OneIMHDB];
GO

PRINT '================================================';
PRINT 'Inserting TEST_CLEANUP_ prefixed test data...';
PRINT '================================================';

DECLARE @i INT;
DECLARE @RowDate DATETIME;
DECLARE @ParentUID UNIQUEIDENTIFIER;
DECLARE @Prefix NVARCHAR(20) = N'TEST_CLEANUP_';

-- ── DialogHistory: 50,000 rows ─────────────────────────
PRINT 'Inserting DialogHistory (50,000 rows)...';
SET @i = 0;
WHILE @i < 50000
BEGIN
    -- Spread evenly over 4 years (1460 days)
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO DialogHistory (UID_DialogTable, ObjectKey, ChangeType, DisplayValue, XDateInserted, XDateUpdated, XUserInserted, XUserUpdated)
    VALUES (
        NEWID(),
        N'<Key><T>Person</T><P>' + CAST(NEWID() AS NVARCHAR(36)) + N'</P></Key>',
        CASE @i % 3 WHEN 0 THEN N'Insert' WHEN 1 THEN N'Update' ELSE N'Delete' END,
        @Prefix + N'DialogHistory_' + CAST(@i AS NVARCHAR),
        @RowDate, @RowDate,
        @Prefix + N'User', @Prefix + N'User'
    );
    SET @i += 1;
END
PRINT '  -> DialogHistory: 50,000 rows inserted.';

-- ── DialogJournal + DialogJournalDetail: 30,000 + 90,000 rows ──
PRINT 'Inserting DialogJournal (30,000) + DialogJournalDetail (90,000)...';
SET @i = 0;
WHILE @i < 30000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    SET @ParentUID = NEWID();

    INSERT INTO DialogJournal (UID_DialogJournal, UID_DialogTable, ObjectKey, ChangeType, DisplayValue, XDateInserted, XDateUpdated, XUserInserted, XUserUpdated)
    VALUES (
        @ParentUID,
        NEWID(),
        N'<Key><T>ADSAccount</T><P>' + CAST(NEWID() AS NVARCHAR(36)) + N'</P></Key>',
        CASE @i % 2 WHEN 0 THEN N'Update' ELSE N'Insert' END,
        @Prefix + N'DialogJournal_' + CAST(@i AS NVARCHAR),
        @RowDate, @RowDate,
        @Prefix + N'User', @Prefix + N'User'
    );

    -- 3 detail rows per journal entry
    INSERT INTO DialogJournalDetail (UID_DialogJournal, ColumnName, OldValue, NewValue, XDateInserted, XDateUpdated)
    VALUES
        (@ParentUID, @Prefix + N'DisplayName', @Prefix + N'OldA_' + CAST(@i AS NVARCHAR), @Prefix + N'NewA_' + CAST(@i AS NVARCHAR), @RowDate, @RowDate),
        (@ParentUID, @Prefix + N'Department',  @Prefix + N'OldB_' + CAST(@i AS NVARCHAR), @Prefix + N'NewB_' + CAST(@i AS NVARCHAR), @RowDate, @RowDate),
        (@ParentUID, @Prefix + N'Email',       @Prefix + N'OldC_' + CAST(@i AS NVARCHAR), @Prefix + N'NewC_' + CAST(@i AS NVARCHAR), @RowDate, @RowDate);

    SET @i += 1;
END
PRINT '  -> DialogJournal: 30,000 rows inserted.';
PRINT '  -> DialogJournalDetail: 90,000 rows inserted.';

-- ── JobHistory: 40,000 rows ────────────────────────────
PRINT 'Inserting JobHistory (40,000 rows)...';
SET @i = 0;
WHILE @i < 40000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO JobHistory (UID_Job, JobName, JobResult, ErrorMessage, ServerName, XDateInserted, XDateUpdated, XUserInserted, XUserUpdated)
    VALUES (
        NEWID(),
        @Prefix + N'SyncJob_' + CAST(@i % 50 AS NVARCHAR),
        CASE WHEN @i % 10 = 0 THEN N'Error' ELSE N'Success' END,
        CASE WHEN @i % 10 = 0 THEN @Prefix + N'Timeout connecting to target' ELSE NULL END,
        @Prefix + N'JOBSERVER' + CAST((@i % 3) + 1 AS NVARCHAR),
        @RowDate, @RowDate,
        @Prefix + N'SYSTEM', @Prefix + N'SYSTEM'
    );
    SET @i += 1;
END
PRINT '  -> JobHistory: 40,000 rows inserted.';

-- ── PersonWantsOrg: 20,000 rows ───────────────────────
PRINT 'Inserting PersonWantsOrg (20,000 rows)...';
SET @i = 0;
WHILE @i < 20000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO PersonWantsOrg (UID_Person, UID_Org, OrderState, OrderReason, XDateInserted, XDateUpdated, XUserInserted, XUserUpdated)
    VALUES (
        NEWID(),
        NEWID(),
        CASE @i % 4 WHEN 0 THEN N'Closed' WHEN 1 THEN N'Canceled' WHEN 2 THEN N'Denied' ELSE N'Dismissed' END,
        @Prefix + N'Request_' + CAST(@i AS NVARCHAR),
        @RowDate, @RowDate,
        @Prefix + N'User', @Prefix + N'User'
    );
    SET @i += 1;
END
PRINT '  -> PersonWantsOrg: 20,000 rows inserted.';

-- ── QBMDBQueueHistory: 60,000 rows ────────────────────
PRINT 'Inserting QBMDBQueueHistory (60,000 rows)...';
SET @i = 0;
WHILE @i < 60000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO QBMDBQueueHistory (SlotName, TaskName, ObjectKey, Result, ErrorMessage, XDateInserted, XDateUpdated)
    VALUES (
        @Prefix + N'Slot_' + CAST((@i % 8) + 1 AS NVARCHAR),
        @Prefix + N'QBMTask_' + CAST(@i % 25 AS NVARCHAR),
        N'<Key><T>Person</T><P>' + CAST(NEWID() AS NVARCHAR(36)) + N'</P></Key>',
        CASE WHEN @i % 20 = 0 THEN N'Error' ELSE N'Success' END,
        CASE WHEN @i % 20 = 0 THEN @Prefix + N'DBQueue failed' ELSE NULL END,
        @RowDate, @RowDate
    );
    SET @i += 1;
END
PRINT '  -> QBMDBQueueHistory: 60,000 rows inserted.';

-- ── QBMProcessHistory: 35,000 rows ────────────────────
PRINT 'Inserting QBMProcessHistory (35,000 rows)...';
SET @i = 0;
WHILE @i < 35000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO QBMProcessHistory (ProcessName, ProcessState, GenProcID, ErrorMessage, XDateInserted, XDateUpdated)
    VALUES (
        @Prefix + N'Process_' + CAST(@i % 30 AS NVARCHAR),
        CASE @i % 5 WHEN 0 THEN N'Error' WHEN 1 THEN N'Frozen' ELSE N'Finished' END,
        @Prefix + CAST(NEWID() AS NVARCHAR(36)),
        CASE WHEN @i % 5 = 0 THEN @Prefix + N'Process failed' ELSE NULL END,
        @RowDate, @RowDate
    );
    SET @i += 1;
END
PRINT '  -> QBMProcessHistory: 35,000 rows inserted.';

-- ── QBMDBQueueSlotHistory: 10,000 rows ────────────────
PRINT 'Inserting QBMDBQueueSlotHistory (10,000 rows)...';
SET @i = 0;
WHILE @i < 10000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO QBMDBQueueSlotHistory (SlotName, SlotNumber, IsActive, XDateInserted, XDateUpdated)
    VALUES (
        @Prefix + N'Slot_' + CAST((@i % 8) + 1 AS NVARCHAR),
        (@i % 8) + 1,
        CASE WHEN @i % 3 = 0 THEN 0 ELSE 1 END,
        @RowDate, @RowDate
    );
    SET @i += 1;
END
PRINT '  -> QBMDBQueueSlotHistory: 10,000 rows inserted.';

-- ============================================================
-- Summary — show what was inserted
-- ============================================================
PRINT '';
PRINT '================================================';
PRINT 'Test data summary (TEST_CLEANUP_ rows only):';
PRINT '================================================';

DECLARE @CutoffDate DATETIME = DATEADD(YEAR, -2, GETDATE());

SELECT 'DialogHistory' AS TableName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END) AS OlderThan2Yrs,
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END) AS NewerThan2Yrs
FROM DialogHistory WHERE XUserInserted LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'DialogJournal',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM DialogJournal WHERE XUserInserted LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'DialogJournalDetail',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM DialogJournalDetail WHERE ColumnName LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'JobHistory',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM JobHistory WHERE XUserInserted LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'PersonWantsOrg',
    COUNT(*),
    SUM(CASE WHEN XDateUpdated < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateUpdated >= @CutoffDate THEN 1 ELSE 0 END)
FROM PersonWantsOrg WHERE XUserInserted LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'QBMDBQueueHistory',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM QBMDBQueueHistory WHERE SlotName LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'QBMProcessHistory',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM QBMProcessHistory WHERE ProcessName LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'QBMDBQueueSlotHistory',
    COUNT(*),
    SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END),
    SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END)
FROM QBMDBQueueSlotHistory WHERE SlotName LIKE N'TEST_CLEANUP_%'
ORDER BY TableName;

PRINT '';
PRINT '================================================';
PRINT 'Done. ~335,000 test rows inserted with TEST_CLEANUP_ prefix.';
PRINT '';
PRINT 'Next steps:';
PRINT '  1. Run cleanup_history.sql to test the purge';
PRINT '  2. Verify old TEST_CLEANUP_ rows were deleted';
PRINT '  3. Run remove_test_data.sql to remove ALL remaining';
PRINT '     TEST_CLEANUP_ rows (including recent ones)';
PRINT '================================================';
GO
