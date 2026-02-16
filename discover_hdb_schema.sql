-- ============================================================
-- One Identity Manager — HDB Schema Discovery
-- ============================================================
-- Run this against your History Database (HDB) to discover
-- the actual table structure, row counts, foreign keys,
-- and date columns available for cleanup filtering.
--
-- Share the output to determine the correct cleanup targets.
-- ============================================================

-- ⚠️ CHANGE THIS to your actual History Database name
USE [OneIMHDB];
GO

PRINT '================================================';
PRINT '1. ALL TABLES WITH ROW COUNTS';
PRINT '================================================';

SELECT
    s.name                                              AS SchemaName,
    t.name                                              AS TableName,
    p.rows                                              AS RowCount,
    CAST((SUM(a.total_pages) * 8.0) / 1024 AS DECIMAL(12,2)) AS SizeMB
FROM sys.tables t
JOIN sys.schemas s          ON t.schema_id   = s.schema_id
JOIN sys.indexes i          ON t.object_id   = i.object_id
JOIN sys.partitions p       ON i.object_id   = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE i.index_id IN (0, 1)   -- heap or clustered
GROUP BY s.name, t.name, p.rows
ORDER BY p.rows DESC;

PRINT '';
PRINT '================================================';
PRINT '2. FOREIGN KEY RELATIONSHIPS';
PRINT '================================================';

SELECT
    fk.name     AS FK_Name,
    tp.name     AS ChildTable,
    cp.name     AS ChildColumn,
    tr.name     AS ParentTable,
    cr.name     AS ParentColumn,
    fk.delete_referential_action_desc AS OnDelete
FROM sys.foreign_keys fk
JOIN sys.tables tp               ON fk.parent_object_id     = tp.object_id
JOIN sys.tables tr               ON fk.referenced_object_id = tr.object_id
JOIN sys.foreign_key_columns fkc ON fk.object_id            = fkc.constraint_object_id
JOIN sys.columns cp              ON fkc.parent_object_id     = cp.object_id
                                AND fkc.parent_column_id     = cp.column_id
JOIN sys.columns cr              ON fkc.referenced_object_id = cr.object_id
                                AND fkc.referenced_column_id = cr.column_id
ORDER BY tp.name, fk.name;

PRINT '';
PRINT '================================================';
PRINT '3. DATE / DATETIME COLUMNS PER TABLE';
PRINT '   (candidates for age-based cleanup filtering)';
PRINT '================================================';

SELECT
    t.name  AS TableName,
    c.name  AS ColumnName,
    ty.name AS DataType
FROM sys.columns c
JOIN sys.tables t  ON c.object_id = t.object_id
JOIN sys.types ty  ON c.user_type_id = ty.user_type_id
WHERE ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date', 'datetimeoffset')
ORDER BY t.name, c.column_id;

PRINT '';
PRINT '================================================';
PRINT '4. FULL COLUMN LIST PER TABLE';
PRINT '   (complete schema detail)';
PRINT '================================================';

SELECT
    t.name                                       AS TableName,
    c.name                                       AS ColumnName,
    ty.name                                      AS DataType,
    c.max_length                                 AS MaxLength,
    c.is_nullable                                AS IsNullable,
    CASE WHEN pk.column_id IS NOT NULL THEN 1
         ELSE 0 END                              AS IsPK,
    CASE WHEN fkc.parent_column_id IS NOT NULL THEN 1
         ELSE 0 END                              AS IsFK
FROM sys.columns c
JOIN sys.tables t   ON c.object_id = t.object_id
JOIN sys.types ty   ON c.user_type_id = ty.user_type_id
LEFT JOIN (
    SELECT ic.object_id, ic.column_id
    FROM sys.index_columns ic
    JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    WHERE i.is_primary_key = 1
) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
LEFT JOIN sys.foreign_key_columns fkc
    ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
ORDER BY t.name, c.column_id;

PRINT '';
PRINT '================================================';
PRINT '5. INDEXES ON ALL TABLES';
PRINT '   (shows which columns have indexes for efficient cleanup)';
PRINT '================================================';

SELECT
    t.name           AS TableName,
    i.name           AS IndexName,
    i.type_desc      AS IndexType,
    i.is_unique      AS IsUnique,
    STUFF((
        SELECT ', ' + c2.name
        FROM sys.index_columns ic2
        JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
        WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id
        ORDER BY ic2.key_ordinal
        FOR XML PATH('')
    ), 1, 2, '')    AS IndexColumns
FROM sys.indexes i
JOIN sys.tables t ON i.object_id = t.object_id
WHERE i.type > 0  -- exclude heaps
GROUP BY t.name, i.name, i.type_desc, i.is_unique, i.object_id, i.index_id
ORDER BY t.name, i.name;

PRINT '';
PRINT '================================================';
PRINT '6. SAMPLE DATA (TOP 5 ROWS PER TABLE)';
PRINT '   Helps identify what each table stores';
PRINT '================================================';

DECLARE @tbl NVARCHAR(256);
DECLARE @sql NVARCHAR(MAX);

DECLARE sample_cursor CURSOR FOR
    SELECT t.name
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo'
    ORDER BY t.name;

OPEN sample_cursor;
FETCH NEXT FROM sample_cursor INTO @tbl;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '--- ' + @tbl + ' ---';
    SET @sql = N'SELECT TOP 5 * FROM [' + @tbl + N']';
    EXEC sp_executesql @sql;
    FETCH NEXT FROM sample_cursor INTO @tbl;
END

CLOSE sample_cursor;
DEALLOCATE sample_cursor;

PRINT '';
PRINT '================================================';
PRINT 'Discovery complete. Share these results to build';
PRINT 'the correct cleanup script for your HDB schema.';
PRINT '================================================';
GO
