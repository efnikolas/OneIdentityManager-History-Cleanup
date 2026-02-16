-- ============================================================
-- One Identity Manager — HDB Age Audit
-- ============================================================
-- Run this BEFORE cleanup to see exactly what date ranges
-- exist in each HDB table and how many rows fall before
-- vs after the 2-year cutoff.
--
-- Shows per table:
--   • Date column used
--   • Oldest and newest record dates
--   • Total row count
--   • Rows older than cutoff (will be purged)
--   • Rows within retention (will be kept)
--   • Year-by-year breakdown
--
-- This script is READ-ONLY — it does not modify any data.
-- ============================================================

-- CHANGE THIS to your History Database
USE [OneIMHDB]
GO

SET NOCOUNT ON

DECLARE @CutoffDate DATETIME = DATEADD(YEAR, -2, GETDATE())

PRINT '================================================'
PRINT '  HDB Age Audit — Read-Only'
PRINT '  Database:    ' + DB_NAME()
PRINT '  Cutoff date: ' + CONVERT(VARCHAR(20), @CutoffDate, 120)
PRINT '  Run at:      ' + CONVERT(VARCHAR(30), GETDATE(), 120)
PRINT '================================================'
PRINT ''

-- ============================================================
-- SECTION 1: Per-table summary (oldest, newest, purge counts)
-- ============================================================
PRINT '--- 1. Per-Table Date Range & Purge Counts ---'
PRINT ''

DECLARE @TableName NVARCHAR(256)
DECLARE @DateCol   NVARCHAR(256)
DECLARE @SQL       NVARCHAR(MAX)

DECLARE @Oldest    DATETIME
DECLARE @Newest    DATETIME
DECLARE @Total     BIGINT
DECLARE @ToPurge   BIGINT
DECLARE @ToKeep    BIGINT

-- Summary results table
DECLARE @Summary TABLE (
    TableName   NVARCHAR(256),
    DateColumn  NVARCHAR(256),
    OldestDate  DATETIME,
    NewestDate  DATETIME,
    TotalRows   BIGINT,
    RowsToPurge BIGINT,
    RowsToKeep  BIGINT
)

DECLARE tbl_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT t.name
    FROM sys.tables t
    WHERE t.name NOT IN ('SourceColumn', 'SourceDatabase', 'SourceTable',
                          'nsecauth', 'nsecimport', 'sysdiagrams')
    ORDER BY t.name

OPEN tbl_cur
FETCH NEXT FROM tbl_cur INTO @TableName
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Find best date column
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
    BEGIN
        SET @SQL = N'
            SELECT
                @oldest  = MIN([' + @DateCol + N']),
                @newest  = MAX([' + @DateCol + N']),
                @total   = COUNT(*),
                @purge   = SUM(CASE WHEN [' + @DateCol + N'] < @cutoff THEN 1 ELSE 0 END),
                @keep    = SUM(CASE WHEN [' + @DateCol + N'] >= @cutoff THEN 1 ELSE 0 END)
            FROM [' + @TableName + N']'

        EXEC sp_executesql @SQL,
            N'@cutoff DATETIME, @oldest DATETIME OUTPUT, @newest DATETIME OUTPUT, @total BIGINT OUTPUT, @purge BIGINT OUTPUT, @keep BIGINT OUTPUT',
            @CutoffDate, @Oldest OUTPUT, @Newest OUTPUT, @Total OUTPUT, @ToPurge OUTPUT, @ToKeep OUTPUT

        INSERT INTO @Summary VALUES (@TableName, @DateCol, @Oldest, @Newest, @Total, @ToPurge, @ToKeep)
    END
    ELSE
        PRINT '  SKIP ' + @TableName + ' (no date column found)'

    FETCH NEXT FROM tbl_cur INTO @TableName
END
CLOSE tbl_cur
DEALLOCATE tbl_cur

-- Display summary
SELECT
    TableName,
    DateColumn,
    CONVERT(VARCHAR(10), OldestDate, 120)  AS OldestRecord,
    CONVERT(VARCHAR(10), NewestDate, 120)  AS NewestRecord,
    TotalRows,
    RowsToPurge,
    RowsToKeep,
    CASE WHEN TotalRows > 0
         THEN CAST(CAST(RowsToPurge * 100.0 / TotalRows AS DECIMAL(5,1)) AS VARCHAR) + '%'
         ELSE '0%'
    END AS PurgePercent
FROM @Summary
ORDER BY TableName

-- Grand totals
SELECT
    SUM(TotalRows)   AS GrandTotalRows,
    SUM(RowsToPurge) AS GrandTotalToPurge,
    SUM(RowsToKeep)  AS GrandTotalToKeep,
    CASE WHEN SUM(TotalRows) > 0
         THEN CAST(CAST(SUM(RowsToPurge) * 100.0 / SUM(TotalRows) AS DECIMAL(5,1)) AS VARCHAR) + '%'
         ELSE '0%'
    END AS OverallPurgePercent
FROM @Summary

PRINT ''

-- ============================================================
-- SECTION 2: Year-by-year breakdown per table
-- ============================================================
PRINT '--- 2. Year-by-Year Row Distribution ---'
PRINT ''

DECLARE yr_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT TableName, DateColumn FROM @Summary WHERE TotalRows > 0 ORDER BY TableName

OPEN yr_cur
FETCH NEXT FROM yr_cur INTO @TableName, @DateCol
WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '>> ' + @TableName + ' (by ' + @DateCol + '):'

    SET @SQL = N'
        SELECT
            YEAR([' + @DateCol + N'])  AS [Year],
            COUNT(*)                   AS [RowCount],
            MIN([' + @DateCol + N'])   AS EarliestInYear,
            MAX([' + @DateCol + N'])   AS LatestInYear,
            CASE WHEN YEAR([' + @DateCol + N']) < YEAR(@cutoff)
                 THEN ''PURGE''
                 WHEN YEAR([' + @DateCol + N']) = YEAR(@cutoff)
                 THEN ''PARTIAL''
                 ELSE ''KEEP''
            END AS [Action]
        FROM [' + @TableName + N']
        GROUP BY YEAR([' + @DateCol + N'])
        ORDER BY YEAR([' + @DateCol + N'])'

    EXEC sp_executesql @SQL, N'@cutoff DATETIME', @CutoffDate

    FETCH NEXT FROM yr_cur INTO @TableName, @DateCol
END
CLOSE yr_cur
DEALLOCATE yr_cur

PRINT ''

-- ============================================================
-- SECTION 3: Sample of oldest 5 rows per table
-- ============================================================
PRINT '--- 3. Oldest 5 Records Per Table (spot-check) ---'
PRINT ''

DECLARE old_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT TableName, DateColumn FROM @Summary WHERE TotalRows > 0 ORDER BY TableName

OPEN old_cur
FETCH NEXT FROM old_cur INTO @TableName, @DateCol
WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '>> ' + @TableName + ' — oldest 5:'

    SET @SQL = N'SELECT TOP 5 * FROM [' + @TableName + N'] ORDER BY [' + @DateCol + N'] ASC'
    EXEC sp_executesql @SQL

    FETCH NEXT FROM old_cur INTO @TableName, @DateCol
END
CLOSE old_cur
DEALLOCATE old_cur

PRINT ''
PRINT '================================================'
PRINT '  Audit complete. No data was modified.'
PRINT '================================================'
GO
