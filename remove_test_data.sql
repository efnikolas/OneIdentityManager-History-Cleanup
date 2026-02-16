-- ============================================================
-- One Identity Manager — Remove HDB Test Data
-- ============================================================
-- Removes all rows inserted by create_test_data.sql by
-- looking for the TEST_CLEANUP_ prefix in string columns.
--
-- Deletes in FK-safe order (children before parents).
-- ============================================================

-- CHANGE THIS to your test History Database
USE [OneIMHDB]
GO

SET NOCOUNT ON

DECLARE @Prefix NVARCHAR(20) = N'TEST_CLEANUP_'

PRINT '================================================'
PRINT 'Removing HDB test data'
PRINT 'Database: ' + DB_NAME()
PRINT 'Prefix:   ' + @Prefix
PRINT '================================================'
PRINT ''

-- FK-safe delete order (same as cleanup_history.sql)
DECLARE @DeleteOrder TABLE (Seq INT IDENTITY(1,1), TableName NVARCHAR(256))
INSERT INTO @DeleteOrder (TableName) VALUES
    ('RawWatchProperty'), ('RawWatchOperation'),
    ('RawProcessStep'), ('RawProcessSubstitute'),
    ('RawProcessChain'), ('RawProcess'), ('RawProcessGroup'),
    ('RawJobHistory'),
    ('WatchProperty'), ('WatchOperation'),
    ('ProcessStep'), ('ProcessSubstitute'),
    ('ProcessChain'), ('ProcessInfo'), ('ProcessGroup'),
    ('HistoryJob'), ('HistoryChain')

DECLARE @TableName  NVARCHAR(256)
DECLARE @StringCol  NVARCHAR(256)
DECLARE @SQL        NVARCHAR(MAX)
DECLARE @Deleted    INT
DECLARE @Seq        INT = 1
DECLARE @MaxSeq     INT = (SELECT MAX(Seq) FROM @DeleteOrder)

WHILE @Seq <= @MaxSeq
BEGIN
    SELECT @TableName = TableName FROM @DeleteOrder WHERE Seq = @Seq

    IF OBJECT_ID(@TableName, 'U') IS NULL
    BEGIN
        SET @Seq = @Seq + 1
        CONTINUE
    END

    -- Find a string column long enough to hold the prefix
    SET @StringCol = NULL
    SELECT TOP 1 @StringCol = c.name
    FROM sys.columns c
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE c.object_id = OBJECT_ID(@TableName)
      AND ty.name IN ('nvarchar', 'varchar', 'nchar', 'char')
      AND c.max_length >= 26
    ORDER BY c.column_id

    IF @StringCol IS NULL
    BEGIN
        PRINT 'SKIP ' + @TableName + ' — no searchable string column'
        SET @Seq = @Seq + 1
        CONTINUE
    END

    SET @SQL = N'DELETE FROM [' + @TableName + N'] WHERE [' + @StringCol + N'] LIKE @prefix + ''%'''

    BEGIN TRY
        EXEC sp_executesql @SQL, N'@prefix NVARCHAR(20)', @Prefix
        SET @Deleted = @@ROWCOUNT
        PRINT @TableName + ': ' + CAST(@Deleted AS VARCHAR) + ' test rows removed'
    END TRY
    BEGIN CATCH
        PRINT @TableName + ': ERROR — ' + ERROR_MESSAGE()
    END CATCH

    SET @Seq = @Seq + 1
END

PRINT ''
PRINT '================================================'
PRINT 'Test data removal complete.'
PRINT '================================================'
GO
