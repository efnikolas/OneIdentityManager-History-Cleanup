-- ============================================================================
-- EMERGENCY: Shrink bloated transaction log after failed/large cleanup runs
-- ============================================================================
-- Run this ONCE to reclaim disk space from a bloated .ldf file.
--
-- How it works:
--   1. Switches to SIMPLE recovery (allows CHECKPOINT to truncate the log)
--   2. Performs CHECKPOINT to clear the log
--   3. Shrinks the log file to a reasonable size
--   4. Switches back to FULL recovery (so your log chain is intact going forward)
--
-- WARNING: Switching to SIMPLE breaks the log backup chain. After running this,
--          take a FULL backup immediately to start a new chain.
-- ============================================================================

USE [OneIMHDB3]          -- << Set your HDB database name
GO

-- Step 1: Show current log file size
PRINT '# CURRENT LOG FILE SIZE'
PRINT '------------------------------------------------------------'
SELECT
    name            AS LogicalName,
    type_desc       AS FileType,
    physical_name   AS FilePath,
    size / 128      AS SizeMB,
    FILEPROPERTY(name, 'SpaceUsed') / 128 AS UsedMB,
    (size - FILEPROPERTY(name, 'SpaceUsed')) / 128 AS FreeMB
FROM sys.database_files
WHERE type_desc = 'LOG'
GO

-- Step 2: Show current recovery model
PRINT ''
PRINT '# CURRENT RECOVERY MODEL'
SELECT name, recovery_model_desc
FROM sys.databases
WHERE name = DB_NAME()
GO

-- Step 3: Switch to SIMPLE → CHECKPOINT → Shrink → back to FULL
PRINT ''
PRINT 'Switching to SIMPLE recovery model...'
ALTER DATABASE [OneIMHDB3] SET RECOVERY SIMPLE
GO

CHECKPOINT
GO

PRINT 'Shrinking log file...'
-- Find the log file logical name and shrink it
DECLARE @logName NVARCHAR(128)
SELECT @logName = name FROM sys.database_files WHERE type_desc = 'LOG'
DBCC SHRINKFILE (@logName, 1024)   -- shrink to ~1 GB (adjust as needed)
GO

PRINT 'Switching back to FULL recovery model...'
ALTER DATABASE [OneIMHDB3] SET RECOVERY FULL
GO

-- Step 4: Show result
PRINT ''
PRINT '# LOG FILE SIZE AFTER SHRINK'
PRINT '------------------------------------------------------------'
SELECT
    name            AS LogicalName,
    type_desc       AS FileType,
    physical_name   AS FilePath,
    size / 128      AS SizeMB,
    FILEPROPERTY(name, 'SpaceUsed') / 128 AS UsedMB,
    (size - FILEPROPERTY(name, 'SpaceUsed')) / 128 AS FreeMB
FROM sys.database_files
WHERE type_desc = 'LOG'
GO

PRINT ''
PRINT '============================================================'
PRINT ' IMPORTANT: Take a FULL BACKUP now to restart the log chain!'
PRINT '============================================================'
GO
