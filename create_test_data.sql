-- ============================================================
-- One Identity Manager — Create Test History Database & Data
-- ============================================================
-- Creates a standalone test database that mirrors the OIM
-- History Database schema (tables only — no OIM install needed).
-- Populates with bulk test data spanning 4 years so you can
-- validate the cleanup scripts.
--
-- Usage:
--   1. Run this script in SSMS against any SQL Server instance
--   2. Run cleanup_history.sql against the resulting TestOneIMHDB
--   3. Verify: old data (>2 yrs) deleted, recent data retained
--
-- ⚠️ This is a TEST database only — do NOT run against a real HDB.
-- ============================================================

USE master;
GO

-- Drop if exists from a previous test run
IF DB_ID('TestOneIMHDB') IS NOT NULL
BEGIN
    ALTER DATABASE TestOneIMHDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE TestOneIMHDB;
END
GO

CREATE DATABASE TestOneIMHDB;
GO

USE TestOneIMHDB;
GO

PRINT '================================================';
PRINT 'Creating test schema...';
PRINT '================================================';

-- ── DialogHistory ──────────────────────────────────────
CREATE TABLE DialogHistory (
    UID_DialogHistory   UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    UID_DialogTable     UNIQUEIDENTIFIER NULL,
    ObjectKey           NVARCHAR(512)    NULL,
    ChangeType          NVARCHAR(64)     NULL,      -- Insert / Update / Delete
    DisplayValue        NVARCHAR(512)    NULL,
    XDateInserted       DATETIME         NOT NULL,
    XDateUpdated        DATETIME         NOT NULL,
    XUserInserted       NVARCHAR(64)     NULL,
    XUserUpdated        NVARCHAR(64)     NULL
);

-- ── DialogJournal ──────────────────────────────────────
CREATE TABLE DialogJournal (
    UID_DialogJournal   UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    UID_DialogTable     UNIQUEIDENTIFIER NULL,
    ObjectKey           NVARCHAR(512)    NULL,
    ChangeType          NVARCHAR(64)     NULL,
    UID_Person          UNIQUEIDENTIFIER NULL,
    DisplayValue        NVARCHAR(512)    NULL,
    XDateInserted       DATETIME         NOT NULL,
    XDateUpdated        DATETIME         NOT NULL,
    XUserInserted       NVARCHAR(64)     NULL,
    XUserUpdated        NVARCHAR(64)     NULL
);

-- ── DialogJournalDetail ────────────────────────────────
CREATE TABLE DialogJournalDetail (
    UID_DialogJournalDetail UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    UID_DialogJournal       UNIQUEIDENTIFIER NOT NULL,
    ColumnName              NVARCHAR(256)    NULL,
    OldValue                NVARCHAR(MAX)    NULL,
    NewValue                NVARCHAR(MAX)    NULL,
    XDateInserted           DATETIME         NOT NULL,
    XDateUpdated            DATETIME         NOT NULL,
    CONSTRAINT FK_DJD_DJ FOREIGN KEY (UID_DialogJournal)
        REFERENCES DialogJournal (UID_DialogJournal)
        ON DELETE CASCADE
);

-- ── JobHistory ─────────────────────────────────────────
CREATE TABLE JobHistory (
    UID_JobHistory      UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    UID_Job             UNIQUEIDENTIFIER NULL,
    JobName             NVARCHAR(256)    NULL,
    JobResult           NVARCHAR(64)     NULL,       -- Success / Error
    ErrorMessage        NVARCHAR(MAX)    NULL,
    ServerName          NVARCHAR(256)    NULL,
    XDateInserted       DATETIME         NOT NULL,
    XDateUpdated        DATETIME         NOT NULL,
    XUserInserted       NVARCHAR(64)     NULL,
    XUserUpdated        NVARCHAR(64)     NULL
);

-- ── PersonWantsOrg ─────────────────────────────────────
CREATE TABLE PersonWantsOrg (
    UID_PersonWantsOrg  UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    UID_Person          UNIQUEIDENTIFIER NULL,
    UID_Org             UNIQUEIDENTIFIER NULL,
    OrderState          NVARCHAR(64)     NULL,       -- Closed / Canceled / Denied / Dismissed
    OrderReason         NVARCHAR(512)    NULL,
    XDateInserted       DATETIME         NOT NULL,
    XDateUpdated        DATETIME         NOT NULL,
    XUserInserted       NVARCHAR(64)     NULL,
    XUserUpdated        NVARCHAR(64)     NULL
);

-- ── QBMDBQueueHistory ──────────────────────────────────
CREATE TABLE QBMDBQueueHistory (
    UID_QBMDBQueueHistory UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    SlotName              NVARCHAR(256)    NULL,
    TaskName              NVARCHAR(256)    NULL,
    ObjectKey             NVARCHAR(512)    NULL,
    Result                NVARCHAR(64)     NULL,
    ErrorMessage          NVARCHAR(MAX)    NULL,
    XDateInserted         DATETIME         NOT NULL,
    XDateUpdated          DATETIME         NOT NULL
);

-- ── QBMProcessHistory ──────────────────────────────────
CREATE TABLE QBMProcessHistory (
    UID_QBMProcessHistory UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    ProcessName           NVARCHAR(256)    NULL,
    ProcessState          NVARCHAR(64)     NULL,
    GenProcID             NVARCHAR(64)     NULL,
    ErrorMessage          NVARCHAR(MAX)    NULL,
    XDateInserted         DATETIME         NOT NULL,
    XDateUpdated          DATETIME         NOT NULL
);

-- ── QBMDBQueueSlotHistory (extra history table) ────────
CREATE TABLE QBMDBQueueSlotHistory (
    UID_QBMDBQueueSlotHistory UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    SlotName                  NVARCHAR(256)    NULL,
    SlotNumber                INT              NULL,
    IsActive                  BIT              NULL,
    XDateInserted             DATETIME         NOT NULL,
    XDateUpdated              DATETIME         NOT NULL
);

PRINT '✓ Schema created.';
PRINT '';

-- ============================================================
-- Populate with test data
-- ============================================================
-- Generates rows spread across 4 years:
--   ~50% older than 2 years (should be purged)
--   ~50% within the last 2 years (should be retained)
-- ============================================================

PRINT '================================================';
PRINT 'Inserting test data (this may take a moment)...';
PRINT '================================================';

DECLARE @i INT;
DECLARE @RowDate DATETIME;
DECLARE @UID UNIQUEIDENTIFIER;
DECLARE @ParentUID UNIQUEIDENTIFIER;

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
        '<Key><T>Person</T><P>' + CAST(NEWID() AS NVARCHAR(36)) + '</P></Key>',
        CASE @i % 3 WHEN 0 THEN 'Insert' WHEN 1 THEN 'Update' ELSE 'Delete' END,
        'Test record ' + CAST(@i AS NVARCHAR),
        @RowDate, @RowDate, 'TestUser', 'TestUser'
    );
    SET @i += 1;
END
PRINT '  ✓ DialogHistory: 50,000 rows inserted.';

-- ── DialogJournal + DialogJournalDetail: 30,000 + 90,000 rows ──
PRINT 'Inserting DialogJournal (30,000 rows) + DialogJournalDetail (90,000 rows)...';
SET @i = 0;
WHILE @i < 30000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    SET @ParentUID = NEWID();

    INSERT INTO DialogJournal (UID_DialogJournal, UID_DialogTable, ObjectKey, ChangeType, DisplayValue, XDateInserted, XDateUpdated, XUserInserted, XUserUpdated)
    VALUES (
        @ParentUID,
        NEWID(),
        '<Key><T>ADSAccount</T><P>' + CAST(NEWID() AS NVARCHAR(36)) + '</P></Key>',
        CASE @i % 2 WHEN 0 THEN 'Update' ELSE 'Insert' END,
        'Journal entry ' + CAST(@i AS NVARCHAR),
        @RowDate, @RowDate, 'TestUser', 'TestUser'
    );

    -- 3 detail rows per journal entry
    INSERT INTO DialogJournalDetail (UID_DialogJournal, ColumnName, OldValue, NewValue, XDateInserted, XDateUpdated)
    VALUES
        (@ParentUID, 'DisplayName', 'Old Value A-' + CAST(@i AS NVARCHAR), 'New Value A-' + CAST(@i AS NVARCHAR), @RowDate, @RowDate),
        (@ParentUID, 'Department',  'Old Value B-' + CAST(@i AS NVARCHAR), 'New Value B-' + CAST(@i AS NVARCHAR), @RowDate, @RowDate),
        (@ParentUID, 'EmailAddress','Old Value C-' + CAST(@i AS NVARCHAR), 'New Value C-' + CAST(@i AS NVARCHAR), @RowDate, @RowDate);

    SET @i += 1;
END
PRINT '  ✓ DialogJournal: 30,000 rows inserted.';
PRINT '  ✓ DialogJournalDetail: 90,000 rows inserted.';

-- ── JobHistory: 40,000 rows ────────────────────────────
PRINT 'Inserting JobHistory (40,000 rows)...';
SET @i = 0;
WHILE @i < 40000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO JobHistory (UID_Job, JobName, JobResult, ErrorMessage, ServerName, XDateInserted, XDateUpdated, XUserInserted, XUserUpdated)
    VALUES (
        NEWID(),
        'SyncJob_' + CAST(@i % 50 AS NVARCHAR),
        CASE WHEN @i % 10 = 0 THEN 'Error' ELSE 'Success' END,
        CASE WHEN @i % 10 = 0 THEN 'Timeout connecting to target system' ELSE NULL END,
        'JOBSERVER' + CAST((@i % 3) + 1 AS NVARCHAR),
        @RowDate, @RowDate, 'SYSTEM', 'SYSTEM'
    );
    SET @i += 1;
END
PRINT '  ✓ JobHistory: 40,000 rows inserted.';

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
        CASE @i % 4 WHEN 0 THEN 'Closed' WHEN 1 THEN 'Canceled' WHEN 2 THEN 'Denied' ELSE 'Dismissed' END,
        'Test request ' + CAST(@i AS NVARCHAR),
        @RowDate, @RowDate, 'TestUser', 'TestUser'
    );
    SET @i += 1;
END
PRINT '  ✓ PersonWantsOrg: 20,000 rows inserted.';

-- ── QBMDBQueueHistory: 60,000 rows ────────────────────
PRINT 'Inserting QBMDBQueueHistory (60,000 rows)...';
SET @i = 0;
WHILE @i < 60000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO QBMDBQueueHistory (SlotName, TaskName, ObjectKey, Result, ErrorMessage, XDateInserted, XDateUpdated)
    VALUES (
        'Slot_' + CAST((@i % 8) + 1 AS NVARCHAR),
        'QBMTask_' + CAST(@i % 25 AS NVARCHAR),
        '<Key><T>Person</T><P>' + CAST(NEWID() AS NVARCHAR(36)) + '</P></Key>',
        CASE WHEN @i % 20 = 0 THEN 'Error' ELSE 'Success' END,
        CASE WHEN @i % 20 = 0 THEN 'DBQueue processing failed' ELSE NULL END,
        @RowDate, @RowDate
    );
    SET @i += 1;
END
PRINT '  ✓ QBMDBQueueHistory: 60,000 rows inserted.';

-- ── QBMProcessHistory: 35,000 rows ────────────────────
PRINT 'Inserting QBMProcessHistory (35,000 rows)...';
SET @i = 0;
WHILE @i < 35000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO QBMProcessHistory (ProcessName, ProcessState, GenProcID, ErrorMessage, XDateInserted, XDateUpdated)
    VALUES (
        'Process_' + CAST(@i % 30 AS NVARCHAR),
        CASE @i % 5 WHEN 0 THEN 'Error' WHEN 1 THEN 'Frozen' ELSE 'Finished' END,
        CAST(NEWID() AS NVARCHAR(36)),
        CASE WHEN @i % 5 = 0 THEN 'Process execution failed' ELSE NULL END,
        @RowDate, @RowDate
    );
    SET @i += 1;
END
PRINT '  ✓ QBMProcessHistory: 35,000 rows inserted.';

-- ── QBMDBQueueSlotHistory: 10,000 rows ────────────────
PRINT 'Inserting QBMDBQueueSlotHistory (10,000 rows)...';
SET @i = 0;
WHILE @i < 10000
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO QBMDBQueueSlotHistory (SlotName, SlotNumber, IsActive, XDateInserted, XDateUpdated)
    VALUES (
        'Slot_' + CAST((@i % 8) + 1 AS NVARCHAR),
        (@i % 8) + 1,
        CASE WHEN @i % 3 = 0 THEN 0 ELSE 1 END,
        @RowDate, @RowDate
    );
    SET @i += 1;
END
PRINT '  ✓ QBMDBQueueSlotHistory: 10,000 rows inserted.';

-- ── Insert some orphaned DialogJournalDetail rows ──────
PRINT 'Inserting orphaned DialogJournalDetail rows (500 rows)...';

-- Temporarily drop FK to allow orphans
ALTER TABLE DialogJournalDetail DROP CONSTRAINT FK_DJD_DJ;

SET @i = 0;
WHILE @i < 500
BEGIN
    SET @RowDate = DATEADD(DAY, -(@i % 1460), GETDATE());
    INSERT INTO DialogJournalDetail (UID_DialogJournal, ColumnName, OldValue, NewValue, XDateInserted, XDateUpdated)
    VALUES (
        NEWID(),  -- non-existent parent
        'OrphanColumn_' + CAST(@i AS NVARCHAR),
        'OrphanOld', 'OrphanNew',
        @RowDate, @RowDate
    );
    SET @i += 1;
END

-- Re-add FK (with NOCHECK so orphans are allowed to remain)
ALTER TABLE DialogJournalDetail WITH NOCHECK
    ADD CONSTRAINT FK_DJD_DJ FOREIGN KEY (UID_DialogJournal)
        REFERENCES DialogJournal (UID_DialogJournal)
        ON DELETE CASCADE;

PRINT '  ✓ Orphaned DialogJournalDetail: 500 rows inserted.';

-- ============================================================
-- Summary
-- ============================================================
PRINT '';
PRINT '================================================';
PRINT 'Test data summary:';
PRINT '================================================';

DECLARE @CutoffDate DATETIME = DATEADD(YEAR, -2, GETDATE());

DECLARE @tbl TABLE (TableName NVARCHAR(256), TotalRows BIGINT, OlderThan2Yrs BIGINT, NewerThan2Yrs BIGINT);

INSERT INTO @tbl
SELECT 'DialogHistory',       COUNT(*), SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END), SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END) FROM DialogHistory UNION ALL
SELECT 'DialogJournal',       COUNT(*), SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END), SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END) FROM DialogJournal UNION ALL
SELECT 'DialogJournalDetail', COUNT(*), SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END), SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END) FROM DialogJournalDetail UNION ALL
SELECT 'JobHistory',          COUNT(*), SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END), SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END) FROM JobHistory UNION ALL
SELECT 'PersonWantsOrg',      COUNT(*), SUM(CASE WHEN XDateUpdated  < @CutoffDate THEN 1 ELSE 0 END), SUM(CASE WHEN XDateUpdated  >= @CutoffDate THEN 1 ELSE 0 END) FROM PersonWantsOrg UNION ALL
SELECT 'QBMDBQueueHistory',   COUNT(*), SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END), SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END) FROM QBMDBQueueHistory UNION ALL
SELECT 'QBMProcessHistory',   COUNT(*), SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END), SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END) FROM QBMProcessHistory UNION ALL
SELECT 'QBMDBQueueSlotHistory', COUNT(*), SUM(CASE WHEN XDateInserted < @CutoffDate THEN 1 ELSE 0 END), SUM(CASE WHEN XDateInserted >= @CutoffDate THEN 1 ELSE 0 END) FROM QBMDBQueueSlotHistory;

SELECT * FROM @tbl ORDER BY TableName;

PRINT '';
PRINT '================================================';
PRINT 'TestOneIMHDB is ready. To test the cleanup:';
PRINT '';
PRINT '  USE TestOneIMHDB;';
PRINT '  -- then run cleanup_history.sql';
PRINT '';
PRINT 'To test with PowerShell:';
PRINT '  .\Invoke-OIMHistoryCleanup.ps1 -SqlServer "localhost" -Database "TestOneIMHDB" -WhatIf';
PRINT '================================================';
GO
