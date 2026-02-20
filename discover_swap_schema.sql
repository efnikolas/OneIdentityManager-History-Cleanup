-- ============================================================================
-- HDB Schema Discovery — Indexes and FK constraints for swap-table planning
-- ============================================================================
-- Run this in SSMS before creating swap-table cleanup scripts.
-- Shows indexes and FK constraints for WatchProperty and WatchOperation.
-- ============================================================================

USE [OneIMHDB3]          -- << Set your HDB database name
GO
SET NOCOUNT ON
GO

-- 1. WatchProperty indexes
PRINT '# WatchProperty Indexes'
PRINT '------------------------------------------------------------'
SELECT i.name, i.type_desc, i.is_unique, i.is_primary_key,
       STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('WatchProperty') AND i.name NOT LIKE 'IX_Cleanup_%'
GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key

-- 2. WatchOperation indexes
PRINT ''
PRINT '# WatchOperation Indexes'
PRINT '------------------------------------------------------------'
SELECT i.name, i.type_desc, i.is_unique, i.is_primary_key,
       STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('WatchOperation') AND i.name NOT LIKE 'IX_Cleanup_%'
GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key

-- 3. FK constraints involving both tables
PRINT ''
PRINT '# FK Constraints (WatchProperty + WatchOperation)'
PRINT '------------------------------------------------------------'
SELECT fk.name AS FK_Name,
       OBJECT_NAME(fk.parent_object_id) AS ChildTable,
       STRING_AGG(cp.name, ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ChildColumns,
       OBJECT_NAME(fk.referenced_object_id) AS ParentTable,
       STRING_AGG(cr.name, ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ParentColumns
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
WHERE fk.parent_object_id IN (OBJECT_ID('WatchProperty'), OBJECT_ID('WatchOperation'))
   OR fk.referenced_object_id IN (OBJECT_ID('WatchProperty'), OBJECT_ID('WatchOperation'))
GROUP BY fk.name, fk.parent_object_id, fk.referenced_object_id
GO
