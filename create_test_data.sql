-- ============================================================
-- One Identity Manager — HDB Test Data Generator
-- ============================================================
-- Inserts TEST_CLEANUP_ prefixed rows into HDB tables so
-- you can safely test the cleanup script against a real
-- History Database without affecting real data.
--
-- The script inspects each HDB table, discovers its columns,
-- and inserts rows with identifiable prefixed values spread
-- across multiple years so some rows fall before the 2-year
-- cutoff and some fall after it.
--
-- ⚠️ Run against a NON-PRODUCTION HDB or a copy.
-- ============================================================

-- CHANGE THIS to your test History Database
USE [OneIMHDB]
GO

SET NOCOUNT ON

DECLARE @Prefix NVARCHAR(20) = N'TEST_CLEANUP_'
DECLARE @Now    DATETIME     = GETDATE()

-- Date buckets: rows spread across 4 years
-- Years 3-4 ago → should be purged (before 2-year cutoff)
-- Years 0-1 ago → should be kept (within 2-year cutoff)
DECLARE @Dates TABLE (Label VARCHAR(20), DateVal DATETIME)
INSERT INTO @Dates VALUES
    ('4y_ago', DATEADD(YEAR, -4, @Now)),
    ('3y_ago', DATEADD(YEAR, -3, @Now)),
    ('2y_ago', DATEADD(DAY,  -1, DATEADD(YEAR, -2, @Now))),  -- just over 2 years
    ('1y_ago', DATEADD(YEAR, -1, @Now)),
    ('recent', DATEADD(MONTH, -1, @Now))

PRINT '================================================'
PRINT 'HDB Test Data Generator'
PRINT 'Database: ' + DB_NAME()
PRINT 'Prefix:   ' + @Prefix
PRINT '================================================'
PRINT ''

-- ============================================================
-- Dynamic insertion: for each non-metadata table, insert
-- test rows using discovered column structure.
-- ============================================================
DECLARE @TableName  NVARCHAR(256)
DECLARE @DateCol    NVARCHAR(256)
DECLARE @SQL        NVARCHAR(MAX)
DECLARE @Cols       NVARCHAR(MAX)
DECLARE @Vals       NVARCHAR(MAX)
DECLARE @DateLabel  VARCHAR(20)
DECLARE @DateVal    DATETIME
DECLARE @BatchNum   INT
DECLARE @InsertCount INT

-- Tables to populate (skip metadata Source* tables)
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
    -- Find the best date column
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

    IF @DateCol IS NULL
    BEGIN
        PRINT 'SKIP ' + @TableName + ' — no date column found'
        FETCH NEXT FROM tbl_cur INTO @TableName
        CONTINUE
    END

    PRINT 'Inserting test data into ' + @TableName + '...'

    -- Build column list for this table (skip identity and computed columns)
    SET @Cols = ''
    SELECT @Cols = @Cols + QUOTENAME(c.name) + ','
    FROM sys.columns c
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE c.object_id = OBJECT_ID(@TableName)
      AND c.is_identity = 0
      AND c.is_computed = 0
    ORDER BY c.column_id

    -- Remove trailing comma
    SET @Cols = LEFT(@Cols, LEN(@Cols) - 1)

    -- For each date bucket, insert a few rows
    SET @BatchNum = 0

    DECLARE dt_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT Label, DateVal FROM @Dates

    OPEN dt_cur
    FETCH NEXT FROM dt_cur INTO @DateLabel, @DateVal
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @BatchNum = @BatchNum + 1

        -- Build a dynamic INSERT with default values per column type
        SET @Vals = ''
        SELECT @Vals = @Vals +
            CASE
                -- The date column we're targeting → use our date value
                WHEN c.name = @DateCol THEN '@dateVal,'
                -- Other datetime columns → also use the date value
                WHEN ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date', 'datetimeoffset')
                    THEN '@dateVal,'
                -- Uniqueidentifier → generate a new GUID
                WHEN ty.name = 'uniqueidentifier'
                    THEN 'NEWID(),'
                -- String types → prefix + table + seq
                WHEN ty.name IN ('nvarchar', 'varchar', 'nchar', 'char', 'ntext', 'text')
                    THEN 'LEFT(@prefix + ''' + @TableName + '_' + @DateLabel + '_' + c.name + ''', '
                         + CASE
                               WHEN c.max_length = -1 THEN '100'
                               WHEN ty.name IN ('nvarchar', 'nchar') THEN CAST(c.max_length / 2 AS VARCHAR)
                               ELSE CAST(c.max_length AS VARCHAR)
                           END + '),'
                -- Integer types
                WHEN ty.name IN ('int', 'smallint', 'tinyint')
                    THEN CAST(@BatchNum AS VARCHAR) + ','
                -- Bigint
                WHEN ty.name = 'bigint'
                    THEN CAST(@BatchNum AS VARCHAR) + ','
                -- Bit
                WHEN ty.name = 'bit'
                    THEN '0,'
                -- Float/decimal/numeric
                WHEN ty.name IN ('float', 'real', 'decimal', 'numeric', 'money', 'smallmoney')
                    THEN CAST(@BatchNum AS VARCHAR) + '.0,'
                -- Binary/image → skip (use DEFAULT or NULL)
                WHEN ty.name IN ('binary', 'varbinary', 'image')
                    THEN CASE WHEN c.is_nullable = 1 THEN 'NULL,' ELSE '0x00,' END
                -- XML
                WHEN ty.name = 'xml'
                    THEN 'CAST(''<test/>'' AS XML),'
                -- Anything else
                ELSE CASE WHEN c.is_nullable = 1 THEN 'NULL,' ELSE 'DEFAULT,' END
            END
        FROM sys.columns c
        INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        WHERE c.object_id = OBJECT_ID(@TableName)
          AND c.is_identity = 0
          AND c.is_computed = 0
        ORDER BY c.column_id

        SET @Vals = LEFT(@Vals, LEN(@Vals) - 1)

        -- Insert 10 rows per date bucket = 50 rows per table
        SET @SQL = N'
DECLARE @i INT = 1
WHILE @i <= 10
BEGIN
    BEGIN TRY
        INSERT INTO [' + @TableName + N'] (' + @Cols + N')
        VALUES (' + @Vals + N')
    END TRY
    BEGIN CATCH
        -- Silently skip constraint violations
        IF @i = 1
            PRINT ''  Warning: '' + ERROR_MESSAGE()
    END CATCH
    SET @i = @i + 1
END
'
        BEGIN TRY
            EXEC sp_executesql @SQL,
                N'@prefix NVARCHAR(20), @dateVal DATETIME',
                @Prefix, @DateVal
        END TRY
        BEGIN CATCH
            PRINT '  Error (' + @DateLabel + '): ' + ERROR_MESSAGE()
        END CATCH

        FETCH NEXT FROM dt_cur INTO @DateLabel, @DateVal
    END CLOSE dt_cur
    DEALLOCATE dt_cur

    -- Report how many rows were inserted
    SET @SQL = N'SELECT @cnt = COUNT(*) FROM [' + @TableName + N'] WHERE [' + @DateCol + N'] IN (SELECT DateVal FROM @dt)'
    -- Simplified count: just count rows matching our prefix in any string column
    DECLARE @StringCol NVARCHAR(256) = NULL
    SELECT TOP 1 @StringCol = c.name
    FROM sys.columns c
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE c.object_id = OBJECT_ID(@TableName)
      AND ty.name IN ('nvarchar', 'varchar', 'nchar', 'char')
      AND c.max_length >= 26  -- long enough to hold our prefix
    ORDER BY c.column_id

    IF @StringCol IS NOT NULL
    BEGIN
        SET @SQL = N'SELECT @cnt = COUNT(*) FROM [' + @TableName + N'] WHERE [' + @StringCol + N'] LIKE @prefix + ''%'''
        EXEC sp_executesql @SQL, N'@prefix NVARCHAR(20), @cnt INT OUTPUT', @Prefix, @InsertCount OUTPUT
        PRINT '  ' + @TableName + ': ' + CAST(ISNULL(@InsertCount, 0) AS VARCHAR) + ' test rows inserted'
    END
    ELSE
        PRINT '  ' + @TableName + ': rows inserted (no string column to verify count)'

    FETCH NEXT FROM tbl_cur INTO @TableName
END
CLOSE tbl_cur
DEALLOCATE tbl_cur

PRINT ''
PRINT '================================================'
PRINT 'Test data insertion complete.'
PRINT 'All test data has prefix: ' + @Prefix
PRINT ''
PRINT 'Expected per table: up to 50 rows'
PRINT '  30 rows = OLD (3-4 years ago, should be purged)'
PRINT '  20 rows = RECENT (0-1 years ago, should be kept)'
PRINT '================================================'
GO
