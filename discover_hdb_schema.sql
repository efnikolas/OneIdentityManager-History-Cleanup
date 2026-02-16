-- ============================================================
-- One Identity Manager — HDB Schema Discovery Script
-- ============================================================
-- Run this against an OIM History Database (TimeTrace DB)
-- to inspect table structure, row counts, date columns,
-- and foreign key relationships.
--
-- Compatible with SQL Server 2012+.
-- ============================================================

PRINT '==========================================='
PRINT '  HDB Schema Discovery'
PRINT '  Database: ' + DB_NAME()
PRINT '  Run at:   ' + CONVERT(VARCHAR(30), GETDATE(), 120)
PRINT '==========================================='
GO

-- 1. Tables and Row Counts
PRINT ''
PRINT '--- 1. Tables and Row Counts ---'
SELECT
    t.name           AS TableName,
    p.rows           AS ApproxRowCount,
    CAST(ROUND(SUM(a.total_pages) * 8.0 / 1024, 2) AS DECIMAL(12,2)) AS SizeMB
FROM sys.tables t
INNER JOIN sys.indexes i      ON t.object_id = i.object_id AND i.index_id <= 1
INNER JOIN sys.partitions p   ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
GROUP BY t.name, p.rows
ORDER BY t.name
GO

-- 2. Foreign Key Relationships
PRINT ''
PRINT '--- 2. Foreign Key Relationships ---'
SELECT
    fk.name                              AS FK_Name,
    OBJECT_NAME(fk.parent_object_id)     AS ChildTable,
    cp.name                              AS ChildColumn,
    OBJECT_NAME(fk.referenced_object_id) AS ParentTable,
    cr.name                              AS ParentColumn
FROM sys.foreign_keys fk
INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
ORDER BY ChildTable, ParentTable
GO

-- 3. Date/Time Columns per Table
PRINT ''
PRINT '--- 3. Date/Time Columns ---'
SELECT
    t.name  AS TableName,
    c.name  AS ColumnName,
    ty.name AS DataType
FROM sys.tables t
INNER JOIN sys.columns c ON t.object_id = c.object_id
INNER JOIN sys.types ty  ON c.user_type_id = ty.user_type_id
WHERE ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date', 'datetimeoffset')
ORDER BY t.name, c.column_id
GO

-- 4. All Columns per Table
PRINT ''
PRINT '--- 4. All Columns ---'
SELECT
    t.name          AS TableName,
    c.name          AS ColumnName,
    ty.name         AS DataType,
    c.max_length    AS MaxLength,
    c.is_nullable   AS Nullable,
    c.column_id     AS OrdinalPos
FROM sys.tables t
INNER JOIN sys.columns c ON t.object_id = c.object_id
INNER JOIN sys.types ty  ON c.user_type_id = ty.user_type_id
ORDER BY t.name, c.column_id
GO

-- 5. Sample Data (top 3 rows from each table, oldest first by date)
PRINT ''
PRINT '--- 5. Sample Data ---'

DECLARE @tbl NVARCHAR(128)
DECLARE @col NVARCHAR(128)
DECLARE @sql NVARCHAR(MAX)

DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT t.name AS TableName, c.DateColumn
    FROM sys.tables t
    CROSS APPLY (
        SELECT TOP 1 c2.name AS DateColumn
        FROM sys.columns c2
        INNER JOIN sys.types ty ON c2.user_type_id = ty.user_type_id
        WHERE c2.object_id = t.object_id
          AND ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date')
        ORDER BY
            CASE c2.name
                WHEN 'XDateInserted' THEN 1
                WHEN 'XDateUpdated'  THEN 2
                WHEN 'StartDate'     THEN 3
                WHEN 'EndDate'       THEN 4
                ELSE 5
            END,
            c2.column_id
    ) c
    ORDER BY t.name

OPEN cur
FETCH NEXT FROM cur INTO @tbl, @col

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT ''
    PRINT '>> ' + @tbl + ' (ordered by ' + @col + '):'
    SET @sql = N'SELECT TOP 3 * FROM [' + @tbl + N'] ORDER BY [' + @col + N'] ASC'
    EXEC sp_executesql @sql
    FETCH NEXT FROM cur INTO @tbl, @col
END

CLOSE cur
DEALLOCATE cur
GO

PRINT ''
PRINT '==========================================='
PRINT '  Discovery complete.'
PRINT '==========================================='
GO
