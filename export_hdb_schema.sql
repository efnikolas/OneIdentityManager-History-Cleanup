-- ==============================================================================
-- One Identity Manager — HDB Schema Export
-- ==============================================================================
-- Run this against your OIM History Database (TimeTrace DB).
-- It outputs the full schema in a readable format that can be copied and saved.
--
-- Compatible with SQL Server 2012+.
-- ==============================================================================

SET NOCOUNT ON
GO

PRINT '=============================================================================='
PRINT '  One Identity Manager — History Database (HDB) Schema Export'
PRINT '  Database : ' + DB_NAME()
PRINT '  Server   : ' + @@SERVERNAME
PRINT '  Exported : ' + CONVERT(VARCHAR(30), GETDATE(), 120)
PRINT '  Version  : ' + CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(30))
PRINT '=============================================================================='
PRINT ''
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Tables, Row Counts and Sizes
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '┌──────────────────────────────────────────────────────────────────────────┐'
PRINT '│  1. TABLES, ROW COUNTS AND SIZES                                        │'
PRINT '└──────────────────────────────────────────────────────────────────────────┘'
SELECT
    t.name           AS [Table],
    p.rows           AS [RowCount],
    CAST(ROUND(SUM(a.total_pages) * 8.0 / 1024, 2) AS DECIMAL(12,2)) AS [SizeMB]
FROM sys.tables t
INNER JOIN sys.indexes i      ON t.object_id = i.object_id AND i.index_id <= 1
INNER JOIN sys.partitions p   ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
GROUP BY t.name, p.rows
ORDER BY t.name
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Complete Column Definitions (all tables)
-- ─────────────────────────────────────────────────────────────────────────────
PRINT ''
PRINT '┌──────────────────────────────────────────────────────────────────────────┐'
PRINT '│  2. COMPLETE COLUMN DEFINITIONS                                         │'
PRINT '└──────────────────────────────────────────────────────────────────────────┘'
SELECT
    t.name                                    AS [Table],
    c.column_id                               AS [#],
    c.name                                    AS [Column],
    UPPER(ty.name)
        + CASE
            WHEN ty.name IN ('varchar','nvarchar','char','nchar')
                 THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX'
                            ELSE CAST(CASE WHEN ty.name LIKE 'n%'
                                           THEN c.max_length / 2
                                           ELSE c.max_length END AS VARCHAR) END + ')'
            WHEN ty.name IN ('decimal','numeric')
                 THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
            ELSE ''
          END                                 AS [DataType],
    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS [Nullable],
    CASE WHEN ic.is_primary_key = 1 THEN 'PK' ELSE '' END AS [PK],
    CASE WHEN fkc.parent_column_id IS NOT NULL
         THEN 'FK -> ' + OBJECT_NAME(fkc.referenced_object_id)
              + '.' + rc.name
         ELSE '' END                          AS [ForeignKey]
FROM sys.tables t
INNER JOIN sys.columns c  ON t.object_id = c.object_id
INNER JOIN sys.types ty   ON c.user_type_id = ty.user_type_id
LEFT JOIN (
    SELECT ic2.object_id, ic2.column_id, CAST(1 AS BIT) AS is_primary_key
    FROM sys.index_columns ic2
    INNER JOIN sys.indexes i2 ON ic2.object_id = i2.object_id AND ic2.index_id = i2.index_id
    WHERE i2.is_primary_key = 1
) ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
LEFT JOIN sys.foreign_key_columns fkc ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
LEFT JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
ORDER BY t.name, c.column_id
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Date/Time Columns Only (quick reference for cleanup scripts)
-- ─────────────────────────────────────────────────────────────────────────────
PRINT ''
PRINT '┌──────────────────────────────────────────────────────────────────────────┐'
PRINT '│  3. DATE/TIME COLUMNS (for cleanup script reference)                    │'
PRINT '└──────────────────────────────────────────────────────────────────────────┘'
SELECT
    t.name  AS [Table],
    c.name  AS [DateColumn],
    UPPER(ty.name) AS [DataType],
    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS [Nullable]
FROM sys.tables t
INNER JOIN sys.columns c  ON t.object_id = c.object_id
INNER JOIN sys.types ty   ON c.user_type_id = ty.user_type_id
WHERE ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date', 'datetimeoffset')
ORDER BY t.name, c.column_id
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Foreign Key Relationships
-- ─────────────────────────────────────────────────────────────────────────────
PRINT ''
PRINT '┌──────────────────────────────────────────────────────────────────────────┐'
PRINT '│  4. FOREIGN KEY RELATIONSHIPS                                           │'
PRINT '└──────────────────────────────────────────────────────────────────────────┘'
SELECT
    fk.name                              AS [FK_Name],
    OBJECT_NAME(fk.parent_object_id)     AS [ChildTable],
    cp.name                              AS [ChildColumn],
    OBJECT_NAME(fk.referenced_object_id) AS [ParentTable],
    cr.name                              AS [ParentColumn],
    CASE fk.delete_referential_action
        WHEN 0 THEN 'NO ACTION'
        WHEN 1 THEN 'CASCADE'
        WHEN 2 THEN 'SET NULL'
        WHEN 3 THEN 'SET DEFAULT'
    END                                  AS [OnDelete]
FROM sys.foreign_keys fk
INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
ORDER BY OBJECT_NAME(fk.parent_object_id), OBJECT_NAME(fk.referenced_object_id)
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Indexes
-- ─────────────────────────────────────────────────────────────────────────────
PRINT ''
PRINT '┌──────────────────────────────────────────────────────────────────────────┐'
PRINT '│  5. INDEXES                                                             │'
PRINT '└──────────────────────────────────────────────────────────────────────────┘'
SELECT
    t.name                        AS [Table],
    i.name                        AS [IndexName],
    CASE WHEN i.is_primary_key = 1 THEN 'PK'
         WHEN i.is_unique = 1     THEN 'UNIQUE'
         ELSE 'NON-UNIQUE' END   AS [Type],
    CASE WHEN i.type = 1 THEN 'CLUSTERED'
         WHEN i.type = 2 THEN 'NONCLUSTERED'
         ELSE CAST(i.type AS VARCHAR) END AS [IndexType],
    STUFF((
        SELECT ', ' + c2.name
        FROM sys.index_columns ic2
        INNER JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
        WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id
        ORDER BY ic2.key_ordinal
        FOR XML PATH('')
    ), 1, 2, '')                  AS [Columns]
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
WHERE i.index_id > 0
ORDER BY t.name, i.index_id
GO

PRINT ''
PRINT '=============================================================================='
PRINT '  Schema export complete.'
PRINT '  Copy this output and save as HDB_SCHEMA_REFERENCE.md'
PRINT '=============================================================================='
GO
