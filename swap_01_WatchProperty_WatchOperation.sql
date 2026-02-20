-- ============================================================================
-- Swap-table Cleanup: WatchProperty + WatchOperation
-- ============================================================================
-- Strategy: Instead of deleting ~461M rows (slow), copy ~171M keeper rows
-- into new tables, drop the originals, rename the new ones, then recreate
-- indexes and FK constraints.
--
-- Prerequisites:
--   * cleanup_00_setup.sql already executed (recovery model = SIMPLE)
--   * STOP any running delete script on WatchProperty/WatchOperation first!
--
-- Estimated time: 1-3 hours (vs ~11 days for batched deletes)
-- ============================================================================

USE [OneIMHDB3]
GO
SET NOCOUNT ON
GO

-- ============================================================
-- Configuration
-- ============================================================
DECLARE @Cutoff DATETIME = DATEADD(YEAR, -2, GETDATE())

PRINT '============================================================'
PRINT ' Swap-table Cleanup: WatchProperty + WatchOperation'
PRINT '============================================================'
PRINT 'Cutoff date : ' + CONVERT(VARCHAR(30), @Cutoff, 121)
PRINT 'Started at  : ' + CONVERT(VARCHAR(30), SYSDATETIME(), 121)
PRINT ''

-- ============================================================
-- Safety: verify recovery model is SIMPLE (minimal logging)
-- ============================================================
IF (SELECT recovery_model_desc FROM sys.databases WHERE name = DB_NAME()) <> 'SIMPLE'
BEGIN
    PRINT '!! WARNING: Recovery model is not SIMPLE — switching now'
    ALTER DATABASE [OneIMHDB3] SET RECOVERY SIMPLE
END
PRINT 'Recovery model: SIMPLE  (SELECT INTO will be minimally logged)'
PRINT ''

-- ============================================================
-- Safety: abort if _keep tables already exist (leftover state)
-- ============================================================
IF OBJECT_ID('WatchOperation_keep', 'U') IS NOT NULL
    OR OBJECT_ID('WatchProperty_keep', 'U') IS NOT NULL
BEGIN
    RAISERROR('Swap tables already exist! Drop WatchOperation_keep / WatchProperty_keep first, or resume from the appropriate phase.', 16, 1)
    RETURN
END

-- ============================================================
-- BEFORE counts
-- ============================================================
PRINT '=== BEFORE ==='
DECLARE @WO_Before BIGINT, @WP_Before BIGINT
SELECT @WO_Before = COUNT_BIG(*) FROM WatchOperation
SELECT @WP_Before = COUNT_BIG(*) FROM WatchProperty
PRINT 'WatchOperation : ' + FORMAT(@WO_Before, 'N0') + ' rows'
PRINT 'WatchProperty  : ' + FORMAT(@WP_Before, 'N0') + ' rows'
PRINT ''

-- ============================================================
-- Phase 1 — Copy keeper WatchOperation rows
-- ============================================================
PRINT '=== Phase 1: SELECT INTO WatchOperation_keep ==='
DECLARE @PhaseStart DATETIME2 = SYSDATETIME()
DECLARE @Rows BIGINT

SELECT *
INTO   WatchOperation_keep
FROM   WatchOperation
WHERE  OperationDate IS NULL
   OR  OperationDate >= @Cutoff

SET @Rows = @@ROWCOUNT
PRINT 'Rows copied  : ' + FORMAT(@Rows, 'N0')
PRINT 'Elapsed      : ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'
CHECKPOINT
PRINT ''

-- ============================================================
-- Phase 2 — Copy keeper WatchProperty rows
-- ============================================================
-- Keep every WatchProperty whose parent WatchOperation was kept.
-- Orphan rows (UID_DialogWatchOperation not in WatchOperation) are dropped
-- since their age cannot be determined and HDB_1251 is already broken.
-- ============================================================
PRINT '=== Phase 2: SELECT INTO WatchProperty_keep ==='
SET @PhaseStart = SYSDATETIME()

SELECT wp.*
INTO   WatchProperty_keep
FROM   WatchProperty wp
INNER JOIN WatchOperation_keep wok
    ON wp.UID_DialogWatchOperation = wok.UID_DialogWatchOperation

SET @Rows = @@ROWCOUNT
PRINT 'Rows copied  : ' + FORMAT(@Rows, 'N0')
PRINT 'Elapsed      : ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'
CHECKPOINT
PRINT ''

-- ============================================================
-- Phase 3 — Drop FK constraints from original tables
-- ============================================================
PRINT '=== Phase 3: Drop FK constraints ==='

-- HDB_1251: WatchProperty → WatchOperation (disabled / untrusted)
IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'HDB_1251')
    ALTER TABLE WatchProperty DROP CONSTRAINT HDB_1251
PRINT '  Dropped HDB_1251 (WatchProperty -> WatchOperation)'

-- HDB_1248: WatchProperty → SourceColumn
IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'HDB_1248')
    ALTER TABLE WatchProperty DROP CONSTRAINT HDB_1248
PRINT '  Dropped HDB_1248 (WatchProperty -> SourceColumn)'

-- HDB_1411: WatchOperation → ProcessInfo
IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'HDB_1411')
    ALTER TABLE WatchOperation DROP CONSTRAINT HDB_1411
PRINT '  Dropped HDB_1411 (WatchOperation -> ProcessInfo)'

PRINT ''

-- ============================================================
-- Phase 4 — Drop originals, rename keepers
-- ============================================================
PRINT '=== Phase 4: Swap tables ==='

DROP TABLE WatchProperty
PRINT '  Dropped WatchProperty'

EXEC sp_rename 'WatchProperty_keep', 'WatchProperty'
PRINT '  Renamed WatchProperty_keep -> WatchProperty'

DROP TABLE WatchOperation
PRINT '  Dropped WatchOperation'

EXEC sp_rename 'WatchOperation_keep', 'WatchOperation'
PRINT '  Renamed WatchOperation_keep -> WatchOperation'

PRINT ''

-- ============================================================
-- Phase 5 — Recreate primary keys (clustered indexes)
-- ============================================================
PRINT '=== Phase 5: Recreate PKs ==='
SET @PhaseStart = SYSDATETIME()

ALTER TABLE WatchOperation
    ADD CONSTRAINT [PK__WatchOpe__EFA600DC5D1E4928]
    PRIMARY KEY CLUSTERED (UID_DialogWatchOperation)
PRINT '  PK on WatchOperation — ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'
CHECKPOINT

SET @PhaseStart = SYSDATETIME()
ALTER TABLE WatchProperty
    ADD CONSTRAINT [PK_WatchPro__36D143DA54ED0CF7]
    PRIMARY KEY CLUSTERED (UID_DialogWatchProperty)
PRINT '  PK on WatchProperty  — ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'
CHECKPOINT
PRINT ''

-- ============================================================
-- Phase 6 — Recreate nonclustered indexes
-- ============================================================
PRINT '=== Phase 6: Recreate nonclustered indexes ==='

-- WatchOperation (4 NC indexes)
SET @PhaseStart = SYSDATETIME()
CREATE NONCLUSTERED INDEX [HDB_XI1WatchOperation]
    ON WatchOperation (OperationType)
PRINT '  HDB_XI1WatchOperation (OperationType) — ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'
CHECKPOINT

SET @PhaseStart = SYSDATETIME()
CREATE NONCLUSTERED INDEX [HDB_XI2WatchOperation]
    ON WatchOperation (OperationDate)
PRINT '  HDB_XI2WatchOperation (OperationDate) — ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'
CHECKPOINT

SET @PhaseStart = SYSDATETIME()
CREATE NONCLUSTERED INDEX [GEN_XIA3E5FEF0C1E74C80F10F7070]
    ON WatchOperation (ObjectKeyOfRow)
PRINT '  GEN_XIA3E5FEF0C1E74C80F10F7070 (ObjectKeyOfRow) — ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'
CHECKPOINT

SET @PhaseStart = SYSDATETIME()
CREATE NONCLUSTERED INDEX [GEN_XXA413A8C522C8354D6A0F8EB4]
    ON WatchOperation (UID_ProcessInfo)
PRINT '  GEN_XXA413A8C522C8354D6A0F8EB4 (UID_ProcessInfo) — ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'
CHECKPOINT

-- WatchProperty (2 NC indexes)
SET @PhaseStart = SYSDATETIME()
CREATE NONCLUSTERED INDEX [GEN_XXA559BB4E7B6583C248259607]
    ON WatchProperty (UID_DialogColumn)
PRINT '  GEN_XXA559BB4E7B6583C248259607 (UID_DialogColumn) — ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'
CHECKPOINT

SET @PhaseStart = SYSDATETIME()
CREATE NONCLUSTERED INDEX [GEN_XX8B556642497224EB13048BA3]
    ON WatchProperty (UID_DialogWatchOperation)
PRINT '  GEN_XX8B556642497224EB13048BA3 (UID_DialogWatchOperation) — ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'
CHECKPOINT
PRINT ''

-- ============================================================
-- Phase 7 — Recreate FK constraints
-- ============================================================
PRINT '=== Phase 7: Recreate FK constraints ==='

-- HDB_1411: WatchOperation → ProcessInfo  (enabled, trusted)
SET @PhaseStart = SYSDATETIME()
ALTER TABLE WatchOperation WITH CHECK
    ADD CONSTRAINT [HDB_1411]
    FOREIGN KEY (UID_ProcessInfo)
    REFERENCES ProcessInfo (UID_ProcessInfo)
PRINT '  HDB_1411 (WatchOperation -> ProcessInfo) [trusted] — ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'

-- HDB_1248: WatchProperty → SourceColumn  (enabled, trusted)
SET @PhaseStart = SYSDATETIME()
ALTER TABLE WatchProperty WITH CHECK
    ADD CONSTRAINT [HDB_1248]
    FOREIGN KEY (UID_DialogColumn)
    REFERENCES SourceColumn (UID_DialogColumn)
PRINT '  HDB_1248 (WatchProperty -> SourceColumn) [trusted] — ' + CAST(DATEDIFF(SECOND, @PhaseStart, SYSDATETIME()) AS VARCHAR) + ' sec'

-- HDB_1251: WatchProperty → WatchOperation  (disabled + untrusted on live DB)
ALTER TABLE WatchProperty WITH NOCHECK
    ADD CONSTRAINT [HDB_1251]
    FOREIGN KEY (UID_DialogWatchOperation)
    REFERENCES WatchOperation (UID_DialogWatchOperation)
ALTER TABLE WatchProperty NOCHECK CONSTRAINT [HDB_1251]
PRINT '  HDB_1251 (WatchProperty -> WatchOperation) [disabled, untrusted]'

PRINT ''

-- ============================================================
-- AFTER counts
-- ============================================================
PRINT '=== AFTER ==='
DECLARE @WO_After BIGINT, @WP_After BIGINT
SELECT @WO_After = COUNT_BIG(*) FROM WatchOperation
SELECT @WP_After = COUNT_BIG(*) FROM WatchProperty
PRINT 'WatchOperation : ' + FORMAT(@WO_After, 'N0') + ' rows  (deleted ' + FORMAT(@WO_Before - @WO_After, 'N0') + ')'
PRINT 'WatchProperty  : ' + FORMAT(@WP_After, 'N0') + ' rows  (deleted ' + FORMAT(@WP_Before - @WP_After, 'N0') + ')'
PRINT ''
PRINT '============================================================'
PRINT ' Swap-table cleanup complete!'
PRINT ' Finished at: ' + CONVERT(VARCHAR(30), SYSDATETIME(), 121)
PRINT '============================================================'
GO
