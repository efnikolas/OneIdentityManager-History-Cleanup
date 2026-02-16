-- ============================================================
-- One Identity Manager — HDB Schema Discovery (File Output)
-- ============================================================
-- Writes all results to C:\temp\hdb_schema.txt
-- Change the output path and database name below as needed.
-- ============================================================

-- CHANGE THESE TWO VALUES:
USE [OneIMHDB]
GO

-- Output file path (folder must exist)
DECLARE @OutFile NVARCHAR(500) = N'C:\temp\hdb_schema.txt'

-- Create/clear the file
DECLARE @cmd NVARCHAR(1000)
SET @cmd = N'echo. > ' + @OutFile
EXEC xp_cmdshell @cmd, no_output

-- Helper: append a line to the file
-- We use bcp/sqlcmd style but simplest is xp_cmdshell echo
DECLARE @line NVARCHAR(4000)

-- ── HEADER ──
SET @cmd = N'echo ================================================ >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output
SET @cmd = N'echo HDB SCHEMA DISCOVERY >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output
SET @cmd = N'echo Database: ' + DB_NAME() + N' >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output
SET @cmd = N'echo Date: ' + CONVERT(NVARCHAR, GETDATE(), 120) + N' >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output
SET @cmd = N'echo ================================================ >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output

-- ── 1. TABLES AND ROW COUNTS ──
SET @cmd = N'echo. >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output
SET @cmd = N'echo === 1. TABLES AND ROW COUNTS === >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output

DECLARE @tname NVARCHAR(256)
DECLARE @rows BIGINT

DECLARE tbl_cur CURSOR FOR
    SELECT t.name, p.[rows]
    FROM sys.tables t
    INNER JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.index_id IN (0, 1)
    ORDER BY p.[rows] DESC

OPEN tbl_cur
FETCH NEXT FROM tbl_cur INTO @tname, @rows
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @cmd = N'echo ' + @tname + N' | ' + CAST(@rows AS NVARCHAR) + N' >> ' + @OutFile
    EXEC xp_cmdshell @cmd, no_output
    FETCH NEXT FROM tbl_cur INTO @tname, @rows
END
CLOSE tbl_cur
DEALLOCATE tbl_cur

-- ── 2. FOREIGN KEYS ──
SET @cmd = N'echo. >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output
SET @cmd = N'echo === 2. FOREIGN KEYS === >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output

DECLARE @fkname NVARCHAR(256)
DECLARE @child NVARCHAR(256)
DECLARE @childcol NVARCHAR(256)
DECLARE @parent NVARCHAR(256)
DECLARE @parentcol NVARCHAR(256)

DECLARE fk_cur CURSOR FOR
    SELECT fk.name, tp.name, cp.name, tr.name, cr.name
    FROM sys.foreign_keys fk
    INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
    INNER JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
    INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
    ORDER BY tp.name

OPEN fk_cur
FETCH NEXT FROM fk_cur INTO @fkname, @child, @childcol, @parent, @parentcol
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @cmd = N'echo ' + @child + N'.' + @childcol + N' -> ' + @parent + N'.' + @parentcol + N' (' + @fkname + N') >> ' + @OutFile
    EXEC xp_cmdshell @cmd, no_output
    FETCH NEXT FROM fk_cur INTO @fkname, @child, @childcol, @parent, @parentcol
END
CLOSE fk_cur
DEALLOCATE fk_cur

-- ── 3. DATE COLUMNS ──
SET @cmd = N'echo. >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output
SET @cmd = N'echo === 3. DATE COLUMNS === >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output

DECLARE @colname NVARCHAR(256)
DECLARE @dtype NVARCHAR(50)

DECLARE dt_cur CURSOR FOR
    SELECT t.name, c.name, ty.name
    FROM sys.columns c
    INNER JOIN sys.tables t ON c.object_id = t.object_id
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date')
    ORDER BY t.name, c.column_id

OPEN dt_cur
FETCH NEXT FROM dt_cur INTO @tname, @colname, @dtype
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @cmd = N'echo ' + @tname + N' | ' + @colname + N' | ' + @dtype + N' >> ' + @OutFile
    EXEC xp_cmdshell @cmd, no_output
    FETCH NEXT FROM dt_cur INTO @tname, @colname, @dtype
END
CLOSE dt_cur
DEALLOCATE dt_cur

-- ── 4. ALL COLUMNS ──
SET @cmd = N'echo. >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output
SET @cmd = N'echo === 4. ALL COLUMNS === >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output

DECLARE @maxlen INT
DECLARE @nullable BIT

DECLARE col_cur CURSOR FOR
    SELECT t.name, c.name, ty.name, c.max_length, c.is_nullable
    FROM sys.columns c
    INNER JOIN sys.tables t ON c.object_id = t.object_id
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    ORDER BY t.name, c.column_id

OPEN col_cur
FETCH NEXT FROM col_cur INTO @tname, @colname, @dtype, @maxlen, @nullable
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @cmd = N'echo ' + @tname + N' | ' + @colname + N' | ' + @dtype + N'(' + CAST(@maxlen AS NVARCHAR) + N') | nullable=' + CAST(@nullable AS NVARCHAR) + N' >> ' + @OutFile
    EXEC xp_cmdshell @cmd, no_output
    FETCH NEXT FROM col_cur INTO @tname, @colname, @dtype, @maxlen, @nullable
END
CLOSE col_cur
DEALLOCATE col_cur

-- ── DONE ──
SET @cmd = N'echo. >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output
SET @cmd = N'echo ================================================ >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output
SET @cmd = N'echo Discovery complete. Share this file. >> ' + @OutFile
EXEC xp_cmdshell @cmd, no_output

PRINT 'Results written to: ' + @OutFile
GO
