-- ============================================================
-- One Identity Manager — HDB Schema Discovery (Simplified)
-- ============================================================
-- Run against your History Database to discover tables,
-- row counts, foreign keys, and date columns.
-- ============================================================

-- CHANGE THIS to your actual History Database name
USE [OneIMHDB]
GO

-- 1. Tables and row counts
SELECT t.name AS TableName, p.[rows] AS [RowCount]
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0, 1)
ORDER BY p.[rows] DESC
GO

-- 2. Foreign keys
SELECT
    fk.name AS FK_Name,
    tp.name AS ChildTable,
    cp.name AS ChildColumn,
    tr.name AS ParentTable,
    cr.name AS ParentColumn
FROM sys.foreign_keys fk
INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
INNER JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
ORDER BY tp.name
GO

-- 3. Date columns per table
SELECT t.name AS TableName, c.name AS ColumnName, ty.name AS DataType
FROM sys.columns c
INNER JOIN sys.tables t ON c.object_id = t.object_id
INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
WHERE ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date')
ORDER BY t.name, c.column_id
GO

-- 4. All columns per table
SELECT t.name AS TableName, c.name AS ColumnName, ty.name AS DataType, c.max_length AS MaxLen, c.is_nullable AS Nullable
FROM sys.columns c
INNER JOIN sys.tables t ON c.object_id = t.object_id
INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
ORDER BY t.name, c.column_id
GO

-- 5. Sample data (top 3 rows per table)
DECLARE @tbl NVARCHAR(256)
DECLARE @sql NVARCHAR(500)

DECLARE cur CURSOR FOR
    SELECT name FROM sys.tables ORDER BY name

OPEN cur
FETCH NEXT FROM cur INTO @tbl

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'SELECT TOP 3 * FROM [' + @tbl + N']'
    EXEC sp_executesql @sql
    FETCH NEXT FROM cur INTO @tbl
END

CLOSE cur
DEALLOCATE cur
GO
