-- ============================================================================
-- HDB Schema Discovery — Indexes and FK constraints for swap-table planning
-- ============================================================================
-- Run this in SSMS before creating swap-table cleanup scripts.
-- Shows indexes and FK constraints for WatchProperty and WatchOperation.
--
-- Captures: key columns, included columns, filtered indexes, FK cascade
-- rules, and FK disabled/untrusted state — everything needed to recreate
-- the schema exactly on a swap table.
-- ============================================================================

USE [OneIMHDB3]          -- << Set your HDB database name
GO
SET NOCOUNT ON
GO

-- 1. WatchProperty indexes (key columns vs included columns + filters)
PRINT '# WatchProperty Indexes'
PRINT '------------------------------------------------------------'
SELECT i.name, i.type_desc, i.is_unique, i.is_primary_key,
       STRING_AGG(CASE WHEN ic.is_included_column = 0 THEN c.name END, ', ')
           WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns,
       STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN c.name END, ', ')
           WITHIN GROUP (ORDER BY ic.index_column_id) AS included_columns,
       i.filter_definition
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('WatchProperty') AND i.name NOT LIKE 'IX_Cleanup_%'
GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key, i.filter_definition

-- 2. WatchOperation indexes (key columns vs included columns + filters)
PRINT ''
PRINT '# WatchOperation Indexes'
PRINT '------------------------------------------------------------'
SELECT i.name, i.type_desc, i.is_unique, i.is_primary_key,
       STRING_AGG(CASE WHEN ic.is_included_column = 0 THEN c.name END, ', ')
           WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns,
       STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN c.name END, ', ')
           WITHIN GROUP (ORDER BY ic.index_column_id) AS included_columns,
       i.filter_definition
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('WatchOperation') AND i.name NOT LIKE 'IX_Cleanup_%'
GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key, i.filter_definition

-- 3. FK constraints (with cascade rules + disabled/untrusted state)
PRINT ''
PRINT '# FK Constraints (WatchProperty + WatchOperation)'
PRINT '------------------------------------------------------------'
SELECT fk.name AS FK_Name,
       OBJECT_NAME(fk.parent_object_id) AS ChildTable,
       STRING_AGG(cp.name, ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ChildColumns,
       OBJECT_NAME(fk.referenced_object_id) AS ParentTable,
       STRING_AGG(cr.name, ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ParentColumns,
       fk.delete_referential_action_desc AS OnDelete,
       fk.update_referential_action_desc AS OnUpdate,
       fk.is_disabled,
       fk.is_not_trusted
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
WHERE fk.parent_object_id IN (OBJECT_ID('WatchProperty'), OBJECT_ID('WatchOperation'))
   OR fk.referenced_object_id IN (OBJECT_ID('WatchProperty'), OBJECT_ID('WatchOperation'))
GROUP BY fk.name, fk.parent_object_id, fk.referenced_object_id,
         fk.delete_referential_action_desc, fk.update_referential_action_desc,
         fk.is_disabled, fk.is_not_trusted
GO
