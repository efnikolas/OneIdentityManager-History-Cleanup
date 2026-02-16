-- ============================================================
-- One Identity Manager — Remove ALL TEST_CLEANUP_ Test Data
-- ============================================================
-- Removes every row inserted by create_test_data.sql.
-- Identifies test data by the 'TEST_CLEANUP_' prefix on
-- text columns — real HDB data is never touched.
--
-- Run this AFTER you have finished testing cleanup_history.sql
-- to leave the HDB exactly as it was before testing.
-- ============================================================

-- ⚠️ CHANGE THIS to your actual History Database name
USE [OneIMHDB];
GO

PRINT '================================================';
PRINT 'Removing ALL TEST_CLEANUP_ test data...';
PRINT '================================================';

DECLARE @Deleted INT;

-- ── 1. DialogJournalDetail (must go before DialogJournal — FK) ──
PRINT 'Removing DialogJournalDetail...';
DELETE FROM DialogJournalDetail
WHERE ColumnName LIKE N'TEST_CLEANUP_%';
SET @Deleted = @@ROWCOUNT;
PRINT '  -> DialogJournalDetail: ' + CAST(@Deleted AS VARCHAR) + ' rows removed.';

-- ── 2. DialogJournal ────────────────────────────────────
PRINT 'Removing DialogJournal...';
DELETE FROM DialogJournal
WHERE XUserInserted LIKE N'TEST_CLEANUP_%';
SET @Deleted = @@ROWCOUNT;
PRINT '  -> DialogJournal: ' + CAST(@Deleted AS VARCHAR) + ' rows removed.';

-- ── 3. DialogHistory ────────────────────────────────────
PRINT 'Removing DialogHistory...';
DELETE FROM DialogHistory
WHERE XUserInserted LIKE N'TEST_CLEANUP_%';
SET @Deleted = @@ROWCOUNT;
PRINT '  -> DialogHistory: ' + CAST(@Deleted AS VARCHAR) + ' rows removed.';

-- ── 4. JobHistory ───────────────────────────────────────
PRINT 'Removing JobHistory...';
DELETE FROM JobHistory
WHERE XUserInserted LIKE N'TEST_CLEANUP_%';
SET @Deleted = @@ROWCOUNT;
PRINT '  -> JobHistory: ' + CAST(@Deleted AS VARCHAR) + ' rows removed.';

-- ── 5. PersonWantsOrg ───────────────────────────────────
PRINT 'Removing PersonWantsOrg...';
DELETE FROM PersonWantsOrg
WHERE XUserInserted LIKE N'TEST_CLEANUP_%';
SET @Deleted = @@ROWCOUNT;
PRINT '  -> PersonWantsOrg: ' + CAST(@Deleted AS VARCHAR) + ' rows removed.';

-- ── 6. QBMDBQueueHistory ────────────────────────────────
PRINT 'Removing QBMDBQueueHistory...';
DELETE FROM QBMDBQueueHistory
WHERE SlotName LIKE N'TEST_CLEANUP_%';
SET @Deleted = @@ROWCOUNT;
PRINT '  -> QBMDBQueueHistory: ' + CAST(@Deleted AS VARCHAR) + ' rows removed.';

-- ── 7. QBMProcessHistory ────────────────────────────────
PRINT 'Removing QBMProcessHistory...';
DELETE FROM QBMProcessHistory
WHERE ProcessName LIKE N'TEST_CLEANUP_%';
SET @Deleted = @@ROWCOUNT;
PRINT '  -> QBMProcessHistory: ' + CAST(@Deleted AS VARCHAR) + ' rows removed.';

-- ── 8. QBMDBQueueSlotHistory ────────────────────────────
PRINT 'Removing QBMDBQueueSlotHistory...';
DELETE FROM QBMDBQueueSlotHistory
WHERE SlotName LIKE N'TEST_CLEANUP_%';
SET @Deleted = @@ROWCOUNT;
PRINT '  -> QBMDBQueueSlotHistory: ' + CAST(@Deleted AS VARCHAR) + ' rows removed.';

-- ── Verify nothing remains ──────────────────────────────
PRINT '';
PRINT '================================================';
PRINT 'Verification — remaining TEST_CLEANUP_ rows:';
PRINT '================================================';

SELECT 'DialogHistory' AS TableName, COUNT(*) AS Remaining FROM DialogHistory WHERE XUserInserted LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'DialogJournal',           COUNT(*) FROM DialogJournal        WHERE XUserInserted LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'DialogJournalDetail',     COUNT(*) FROM DialogJournalDetail  WHERE ColumnName    LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'JobHistory',              COUNT(*) FROM JobHistory           WHERE XUserInserted LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'PersonWantsOrg',          COUNT(*) FROM PersonWantsOrg       WHERE XUserInserted LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'QBMDBQueueHistory',       COUNT(*) FROM QBMDBQueueHistory    WHERE SlotName      LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'QBMProcessHistory',       COUNT(*) FROM QBMProcessHistory    WHERE ProcessName   LIKE N'TEST_CLEANUP_%'
UNION ALL
SELECT 'QBMDBQueueSlotHistory',   COUNT(*) FROM QBMDBQueueSlotHistory WHERE SlotName     LIKE N'TEST_CLEANUP_%'
ORDER BY TableName;

PRINT '';
PRINT 'All TEST_CLEANUP_ test data has been removed.';
PRINT 'Your HDB is back to its original state.';
GO
