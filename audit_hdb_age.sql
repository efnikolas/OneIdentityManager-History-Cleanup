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
    -- Find best date column (with COALESCE fallback for NULL-prone columns)
    SET @DateCol = NULL
    
    -- Check if table has both a primary date col AND a LastDate/EndAt fallback
    DECLARE @FallbackCol NVARCHAR(256) = NULL
    
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
            WHEN 'ImportDate'     THEN 7
            WHEN 'XDateUpdated'   THEN 8
            ELSE 10
        END,
        c.column_id

    -- Find fallback date columns (LastDate, then ExportDate)
    DECLARE @FallbackCol2 NVARCHAR(256) = NULL

    SELECT TOP 1 @FallbackCol = c.name
    FROM sys.columns c
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE c.object_id = OBJECT_ID(@TableName)
      AND ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date')
      AND c.name IN ('LastDate', 'EndAt')
    ORDER BY CASE c.name WHEN 'LastDate' THEN 1 WHEN 'EndAt' THEN 2 ELSE 3 END

    -- Find second fallback (ExportDate) for tables where FirstDate AND LastDate can both be NULL
    IF EXISTS (
        SELECT 1 FROM sys.columns c
        INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        WHERE c.object_id = OBJECT_ID(@TableName)
          AND c.name = 'ExportDate'
          AND ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date')
    )
        SET @FallbackCol2 = 'ExportDate'

    IF @DateCol IS NOT NULL
    BEGIN
        DECLARE @DateExpr NVARCHAR(512)
        IF @FallbackCol IS NOT NULL AND @FallbackCol2 IS NOT NULL AND @FallbackCol <> @DateCol
            SET @DateExpr = N'COALESCE([' + @DateCol + N'], [' + @FallbackCol + N'], [' + @FallbackCol2 + N'])'
        ELSE IF @FallbackCol IS NOT NULL AND @FallbackCol <> @DateCol
            SET @DateExpr = N'COALESCE([' + @DateCol + N'], [' + @FallbackCol + N'])'
        ELSE IF @FallbackCol2 IS NOT NULL AND @FallbackCol2 <> @DateCol
            SET @DateExpr = N'COALESCE([' + @DateCol + N'], [' + @FallbackCol2 + N'])'
        ELSE
            SET @DateExpr = N'[' + @DateCol + N']'

        SET @SQL = N'
            SELECT
                @oldest  = MIN(' + @DateExpr + N'),
                @newest  = MAX(' + @DateExpr + N'),
                @total   = COUNT(*),
                @purge   = SUM(CASE WHEN ' + @DateExpr + N' < @cutoff THEN 1 ELSE 0 END),
                @keep    = SUM(CASE WHEN ' + @DateExpr + N' >= @cutoff THEN 1 ELSE 0 END)
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
    -- Rebuild COALESCE expression for this table
    DECLARE @YrExpr NVARCHAR(512)
    DECLARE @Fb1 NVARCHAR(256) = NULL, @Fb2 NVARCHAR(256) = NULL

    SELECT TOP 1 @Fb1 = c.name
    FROM sys.columns c INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE c.object_id = OBJECT_ID(@TableName) AND c.name IN ('LastDate','EndAt')
      AND ty.name IN ('datetime','datetime2','smalldatetime','date')
    ORDER BY CASE c.name WHEN 'LastDate' THEN 1 ELSE 2 END

    IF EXISTS (SELECT 1 FROM sys.columns c INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
              WHERE c.object_id = OBJECT_ID(@TableName) AND c.name = 'ExportDate'
                AND ty.name IN ('datetime','datetime2','smalldatetime','date'))
        SET @Fb2 = 'ExportDate'

    IF @Fb1 IS NOT NULL AND @Fb2 IS NOT NULL AND @Fb1 <> @DateCol
        SET @YrExpr = N'COALESCE([' + @DateCol + N'],[' + @Fb1 + N'],[' + @Fb2 + N'])'
    ELSE IF @Fb1 IS NOT NULL AND @Fb1 <> @DateCol
        SET @YrExpr = N'COALESCE([' + @DateCol + N'],[' + @Fb1 + N'])'
    ELSE IF @Fb2 IS NOT NULL AND @Fb2 <> @DateCol
        SET @YrExpr = N'COALESCE([' + @DateCol + N'],[' + @Fb2 + N'])'
    ELSE
        SET @YrExpr = N'[' + @DateCol + N']'

    PRINT '>> ' + @TableName + ' (by ' + @DateCol + '):'

    SET @SQL = N'
        SELECT
            YEAR(' + @YrExpr + N')  AS [Year],
            COUNT(*)                   AS [RowCount],
            MIN(' + @YrExpr + N')   AS EarliestInYear,
            MAX(' + @YrExpr + N')   AS LatestInYear,
            CASE WHEN YEAR(' + @YrExpr + N') < YEAR(@cutoff)
                 THEN ''PURGE''
                 WHEN YEAR(' + @YrExpr + N') = YEAR(@cutoff)
                 THEN ''PARTIAL''
                 ELSE ''KEEP''
            END AS [Action]
        FROM [' + @TableName + N']
        GROUP BY YEAR(' + @YrExpr + N')
        ORDER BY YEAR(' + @YrExpr + N')'

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

    -- Use COALESCE for ordering so NULL-date rows sort correctly
    DECLARE @OrdExpr NVARCHAR(512)
    DECLARE @Of1 NVARCHAR(256) = NULL, @Of2 NVARCHAR(256) = NULL
    SELECT TOP 1 @Of1 = c.name FROM sys.columns c INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE c.object_id = OBJECT_ID(@TableName) AND c.name IN ('LastDate','EndAt') AND ty.name IN ('datetime','datetime2','smalldatetime','date')
    ORDER BY CASE c.name WHEN 'LastDate' THEN 1 ELSE 2 END
    IF EXISTS (SELECT 1 FROM sys.columns c INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
              WHERE c.object_id = OBJECT_ID(@TableName) AND c.name = 'ExportDate' AND ty.name IN ('datetime','datetime2','smalldatetime','date'))
        SET @Of2 = 'ExportDate'
    IF @Of1 IS NOT NULL AND @Of2 IS NOT NULL AND @Of1 <> @DateCol
        SET @OrdExpr = N'COALESCE([' + @DateCol + N'],[' + @Of1 + N'],[' + @Of2 + N'])'
    ELSE IF @Of1 IS NOT NULL AND @Of1 <> @DateCol
        SET @OrdExpr = N'COALESCE([' + @DateCol + N'],[' + @Of1 + N'])'
    ELSE IF @Of2 IS NOT NULL AND @Of2 <> @DateCol
        SET @OrdExpr = N'COALESCE([' + @DateCol + N'],[' + @Of2 + N'])'
    ELSE
        SET @OrdExpr = N'[' + @DateCol + N']'

    SET @SQL = N'SELECT TOP 5 * FROM [' + @TableName + N'] ORDER BY ' + @OrdExpr + N' ASC'
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
