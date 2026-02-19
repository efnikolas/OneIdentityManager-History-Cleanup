# ============================================================
# One Identity Manager — HDB Cleanup PowerShell Wrapper
# ============================================================
# Purges archived data older than N years from one or more
# OIM History Databases (TimeTrace databases).
#
# Handles two kinds of HDB tables:
#   • Tables WITH a date column  → delete by date directly
#   • Tables WITHOUT a date column (WatchProperty,
#     ProcessSubstitute, RawWatchProperty, RawProcessSubstitute)
#     → delete via FK join to the parent table's date column
#
# Usage:
#   # Dry run (report only):
#   .\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIMHDB" -WhatIf
#
#   # Actual cleanup (2-year retention, default):
#   .\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIMHDB"
#
#   # Multiple HDBs:
#   .\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIMHDB","OneIMHDB2","OneIMHDB3" -WhatIf
#
#   # With encrypted connection (SQL Server requires SSL):
#   .\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIMHDB" -Encrypt -WhatIf
#
#   # Encrypted but skip certificate validation (self-signed cert):
#   .\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIMHDB" -Encrypt -TrustServerCertificate -WhatIf
# ============================================================

param(
    [Parameter(Mandatory = $true)]
    [string]$SqlServer,

    [Parameter(Mandatory = $true)]
    [string[]]$Database,

    [int]$RetentionYears = 2,

    [int]$BatchSize = 50000,

    [int]$BatchDelaySec = 0,

    [switch]$BenchmarkBatchSize,

    [switch]$CreateTempIndexes,

    [switch]$Encrypt,

    [switch]$TrustServerCertificate,

    [switch]$WhatIf
)

$CutoffDate = (Get-Date).AddYears(-$RetentionYears)
$cutoffStr  = $CutoffDate.ToString("yyyy-MM-dd")
$LogFile    = Join-Path $PSScriptRoot "cleanup_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

# Build common connection parameters (splatted into every Invoke-Sqlcmd call)
$connParams = @{ ServerInstance = $SqlServer; ErrorAction = 'Stop' }
if ($Encrypt)                { $connParams['Encrypt'] = 'Mandatory' }
if ($TrustServerCertificate) { $connParams['TrustServerCertificate'] = $true }

function Write-Log {
    param([string]$Message, [string]$Color = "White")
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $entry = "[$ts] $Message"
    Write-Host $entry -ForegroundColor $Color
    Add-Content -Path $LogFile -Value $entry
}

# Tables to skip (metadata + non-HDB tables)
$skipTables = @(
    'SourceColumn', 'SourceDatabase', 'SourceTable',  # OIM metadata
    'nsecauth', 'nsecimport',                          # Custom non-HDB tables
    'sysdiagrams'                                      # SQL Server system table
)

# FK-safe delete order: children first, parents last
# Tables WITHOUT a date column are included here — they will be handled
# via FK-join deletes to their parent table's date column.
$deleteOrder = @(
    # Raw tables (children first)
    'RawWatchProperty',      # FK -> RawWatchOperation.UID_DialogWatchOperation (no date col)
    'RawWatchOperation',     # FK -> RawProcess.GenProcID
    'RawProcessStep',        # FK -> RawProcessChain.UID_Tree
    'RawProcessSubstitute',  # FK -> RawProcess.GenProcID (no date col)
    'RawProcessChain',       # FK -> RawProcess.GenProcID
    'RawProcess',            # FK -> RawProcessGroup.GenProcIDGroup
    'RawProcessGroup',       # root raw table
    'RawJobHistory',         # FK -> RawProcess.GenProcID
    # Processed tables (children first)
    'WatchProperty',         # FK -> WatchOperation.UID_DialogWatchOperation (NO DATE COL)
    'WatchOperation',        # FK -> ProcessInfo.UID_ProcessInfo
    'ProcessStep',           # FK -> ProcessChain.UID_ProcessChain
    'ProcessSubstitute',     # FK -> ProcessInfo.UID_ProcessInfoNew/Origin (NO DATE COL)
    'ProcessChain',          # FK -> ProcessInfo.UID_ProcessInfo
    'HistoryJob',            # FK -> HistoryChain.UID_HistoryChain
    'HistoryChain',          # FK -> ProcessInfo.UID_ProcessInfo
    'ProcessInfo',           # FK -> ProcessGroup.UID_ProcessGroup
    'ProcessGroup'           # root processed table
)

# Tables that have NO date column and must be purged via FK join to a parent.
# Format: ChildTable -> @{ ParentTable; ChildFK; ParentPK; ParentDateCol }
# The ParentDateCol is resolved dynamically; these define the join path.
$fkJoinDeletes = @{
    'WatchProperty' = @{
        ParentTable  = 'WatchOperation'
        ChildFK      = 'UID_DialogWatchOperation'
        ParentPK     = 'UID_DialogWatchOperation'
        ParentDateCol = $null  # resolved at runtime
    }
    'ProcessSubstitute' = @{
        ParentTable  = 'ProcessInfo'
        ChildFK      = 'UID_ProcessInfoNew'
        ParentPK     = 'UID_ProcessInfo'
        ParentDateCol = $null
    }
    'RawWatchProperty' = @{
        ParentTable  = 'RawWatchOperation'
        ChildFK      = 'UID_DialogWatchOperation'
        ParentPK     = 'UID_DialogWatchOperation'
        ParentDateCol = $null
    }
    'RawProcessSubstitute' = @{
        ParentTable  = 'RawProcess'
        ChildFK      = 'GenProcIDNew'
        ParentPK     = 'GenProcID'
        ParentDateCol = $null
    }
}

Write-Log "================================================" "Cyan"
Write-Log "OIM History Database Cleanup" "Cyan"
Write-Log "================================================" "Cyan"
Write-Log "Server:      $SqlServer"
Write-Log "Databases:   $($Database -join ', ')"
Write-Log "Cutoff:      $CutoffDate (retain last $RetentionYears years)"
Write-Log "BatchSize:   $BatchSize"
Write-Log "BatchDelay:  ${BatchDelaySec}s"
Write-Log "TempIndexes: $CreateTempIndexes"
Write-Log "Benchmark:   $BenchmarkBatchSize"
Write-Log "Encrypt:     $Encrypt"
Write-Log "TrustCert:   $TrustServerCertificate"
Write-Log "WhatIf:      $WhatIf"
Write-Log "Log File:    $LogFile"
Write-Log "================================================" "Cyan"

$allDbsTotal = 0

foreach ($db in $Database) {
    Write-Log "" "White"
    Write-Log "=============================================" "Magenta"
    Write-Log "Processing database: $db" "Magenta"
    Write-Log "=============================================" "Magenta"

    $dbTotal = 0

    $skipList = ($skipTables | ForEach-Object { "'$_'" }) -join ','

    # Discover date columns for all HDB tables.
    # Priority: OperationDate (WatchOperation), FirstDate (ProcessGroup/Info),
    # XDateInserted (Raw*), ThisDate (ProcessChain/Step), StartAt (HistoryJob),
    # ExportDate, then any other datetime column by ordinal position.
    $discoverQuery = "SELECT t.name AS TableName, c.name AS DateColumn " +
        "FROM sys.tables t " +
        "CROSS APPLY ( " +
        "SELECT TOP 1 c.name FROM sys.columns c " +
        "INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id " +
        "WHERE c.object_id = t.object_id " +
        "AND ty.name IN ('datetime','datetime2','smalldatetime','date') " +
        "ORDER BY CASE c.name " +
        "WHEN 'OperationDate'  THEN 1 " +
        "WHEN 'FirstDate'      THEN 2 " +
        "WHEN 'XDateInserted'  THEN 3 " +
        "WHEN 'ThisDate'       THEN 4 " +
        "WHEN 'StartAt'        THEN 5 " +
        "WHEN 'ExportDate'     THEN 6 " +
        "WHEN 'XDateUpdated'   THEN 7 " +
        "ELSE 10 END, " +
        "c.column_id) c " +
        "WHERE t.name NOT IN ($skipList) " +
        "ORDER BY t.name"

    # Also discover tables that have NO date column (for FK-join deletes)
    $noDateQuery = "SELECT t.name AS TableName " +
        "FROM sys.tables t " +
        "WHERE t.name NOT IN ($skipList) " +
        "AND NOT EXISTS ( " +
        "SELECT 1 FROM sys.columns c " +
        "INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id " +
        "WHERE c.object_id = t.object_id " +
        "AND ty.name IN ('datetime','datetime2','smalldatetime','date') " +
        ") ORDER BY t.name"

    try {
        $tableInfo   = Invoke-Sqlcmd @connParams -Database $db -Query $discoverQuery
        $noDateTables = Invoke-Sqlcmd @connParams -Database $db -Query $noDateQuery
    }
    catch {
        Write-Log "  ERROR discovering tables: $($_.Exception.Message)" "Red"
        continue
    }

    # Build a lookup hashtable: TableName -> DateColumn
    $dateColumns = @{}
    foreach ($row in $tableInfo) {
        $dateColumns[$row.TableName] = $row.DateColumn
    }

    # Resolve parent date columns for FK-join tables
    foreach ($childTbl in @($fkJoinDeletes.Keys)) {
        $fk = $fkJoinDeletes[$childTbl]
        if ($dateColumns.ContainsKey($fk.ParentTable)) {
            $fk.ParentDateCol = $dateColumns[$fk.ParentTable]
        }
    }

    # Log tables without date columns
    if ($noDateTables) {
        foreach ($row in $noDateTables) {
            $tblName = $row.TableName
            if ($fkJoinDeletes.ContainsKey($tblName)) {
                $fk = $fkJoinDeletes[$tblName]
                Write-Log "  INFO $tblName has no date column — will purge via FK join to $($fk.ParentTable).$($fk.ParentDateCol)" "DarkCyan"
            }
            else {
                Write-Log "  WARN $tblName has no date column and no FK-join rule — will be SKIPPED" "Yellow"
            }
        }
    }

    # ── Create temp indexes on date columns EARLY so pre-flight MIN/MAX, benchmark, and deletes all benefit ──
    $tempIndexes = @()
    if (-not $WhatIf -and $CreateTempIndexes) {
        Write-Log "" "White"
        Write-Log "  Creating temp indexes on date columns..." "Cyan"

        # First, clean up any stale IX_Cleanup_* indexes left from prior cancelled runs
        try {
            $staleQuery = "SELECT i.name AS IdxName, OBJECT_NAME(i.object_id) AS TblName " +
                "FROM sys.indexes i WHERE i.name LIKE 'IX_Cleanup_%' AND i.type = 2"
            $staleIndexes = Invoke-Sqlcmd @connParams -Database $db -Query $staleQuery
            foreach ($si in $staleIndexes) {
                try {
                    Invoke-Sqlcmd @connParams -Database $db -Query "DROP INDEX [$($si.IdxName)] ON [$($si.TblName)]"
                    Write-Log "    Dropped stale index $($si.IdxName) on $($si.TblName)" "DarkGray"
                } catch {
                    Write-Log "    WARN Could not drop stale $($si.IdxName): $($_.Exception.Message)" "Yellow"
                }
            }
        } catch { }

        foreach ($row in $tableInfo) {
            $tbl = $row.TableName
            $col = $row.DateColumn
            $idxName = "IX_Cleanup_${tbl}_${col}"
            try {
                $idxCheckQuery = "SELECT CASE WHEN EXISTS ( " +
                    "SELECT 1 FROM sys.index_columns ic " +
                    "INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
                    "WHERE ic.object_id = OBJECT_ID('$tbl') " +
                    "AND ic.column_id = (SELECT column_id FROM sys.columns WHERE object_id = OBJECT_ID('$tbl') AND name = '$col') " +
                    "AND ic.key_ordinal = 1) THEN 1 ELSE 0 END AS HasIndex"
                $idxResult = Invoke-Sqlcmd @connParams -Database $db -Query $idxCheckQuery
                if ($idxResult.HasIndex -eq 0) {
                    $createIdxQuery = "CREATE NONCLUSTERED INDEX [$idxName] ON [$tbl] ([$col])"
                    Invoke-Sqlcmd @connParams -Database $db -Query $createIdxQuery -QueryTimeout 600
                    $tempIndexes += @{ IndexName = $idxName; TableName = $tbl }
                    Write-Log "    OK Created $idxName" "DarkCyan"
                } else {
                    Write-Log "    SKIP $tbl.$col already indexed" "DarkGray"
                }
            }
            catch {
                Write-Log "    WARN Could not create index on $tbl.$col : $($_.Exception.Message)" "Yellow"
            }
        }
    }

    # Pre-flight counts — fast estimated row counts + date ranges (indexes now available for MIN/MAX)
    Write-Log "" "White"
    Write-Log "  Pre-flight summary (estimated):" "Yellow"
    foreach ($row in $tableInfo) {
        $tbl = $row.TableName
        $col = $row.DateColumn
        try {
            $countQuery = "SELECT " +
                "(SELECT SUM(p.row_count) FROM sys.dm_db_partition_stats p WHERE p.object_id = OBJECT_ID('$tbl') AND p.index_id IN (0,1)) AS Total, " +
                "CONVERT(VARCHAR(20), MIN([$col]), 120) AS MinDate, " +
                "CONVERT(VARCHAR(20), MAX([$col]), 120) AS MaxDate " +
                "FROM [$tbl]"
            $result = Invoke-Sqlcmd @connParams -Database $db -Query $countQuery
            Write-Log "    $tbl : ~$($result.Total) rows ($col`: $($result.MinDate) to $($result.MaxDate))" "Yellow"
        }
        catch {
            Write-Log "    $tbl : error counting — $($_.Exception.Message)" "Red"
        }
    }

    # Pre-flight counts — FK-joined tables (estimated row counts only, no joins)
    foreach ($childTbl in @($fkJoinDeletes.Keys)) {
        $fk = $fkJoinDeletes[$childTbl]
        if (-not $fk.ParentDateCol) { continue }
        try {
            $existsQuery = "SELECT OBJECT_ID('$childTbl', 'U') AS ObjId"
            $existsResult = Invoke-Sqlcmd @connParams -Database $db -Query $existsQuery
            if ($null -eq $existsResult.ObjId) { continue }
        }
        catch { continue }

        try {
            $fkCountQuery = "SELECT SUM(p.row_count) AS Total " +
                "FROM sys.dm_db_partition_stats p " +
                "WHERE p.object_id = OBJECT_ID('$childTbl') AND p.index_id IN (0,1)"
            $fkResult = Invoke-Sqlcmd @connParams -Database $db -Query $fkCountQuery
            Write-Log "    $childTbl : ~$($fkResult.Total) rows (purge via $($fk.ParentTable).$($fk.ParentDateCol))" "Yellow"
        }
        catch {
            Write-Log "    $childTbl : error counting — $($_.Exception.Message)" "Red"
        }
    }

    if ($WhatIf) {
        Write-Log "" "White"
        Write-Log "  WhatIf mode — no data deleted for $db" "Cyan"
        continue
    }

    # ── Benchmark batch sizes on the largest table ──
    if ($BenchmarkBatchSize) {
        Write-Log "" "White"
        Write-Log "  =============================================" "Cyan"
        Write-Log "  BATCH SIZE BENCHMARK" "Cyan"
        Write-Log "  =============================================" "Cyan"

        # Find the largest LEAF table (no FK references pointing to it) to avoid constraint errors
        $benchBest = $null
        $leafQuery = "SELECT t.name AS TableName " +
            "FROM sys.tables t " +
            "WHERE NOT EXISTS (SELECT 1 FROM sys.foreign_keys fk WHERE fk.referenced_object_id = t.object_id) " +
            "ORDER BY t.name"
        $leafTables = @()
        try {
            $leafResults = Invoke-Sqlcmd @connParams -Database $db -Query $leafQuery
            $leafTables = $leafResults | ForEach-Object { $_.TableName }
        } catch { }

        foreach ($row in $tableInfo) {
            $tbl = $row.TableName; $col = $row.DateColumn
            if ($tbl -notin $leafTables) { continue }  # skip parent tables
            try {
                # Use estimated row count from partition stats (instant, no scan)
                $cntResult = Invoke-Sqlcmd @connParams -Database $db -Query "SELECT SUM(p.row_count) AS Cnt FROM sys.dm_db_partition_stats p WHERE p.object_id = OBJECT_ID('$tbl') AND p.index_id IN (0,1)"
                if (-not $benchBest -or $cntResult.Cnt -gt $benchBest.PurgeCount) {
                    $benchBest = @{ TableName = $tbl; DateColumn = $col; PurgeCount = $cntResult.Cnt }
                }
            } catch { }
        }

        if ($benchBest -and $benchBest.PurgeCount -ge 100000) {
            Write-Log "  Target: $($benchBest.TableName) ($($benchBest.PurgeCount) rows to purge by $($benchBest.DateColumn))" "Cyan"
            Write-Log "  Testing 2 trial batches per size (early exit)..." "White"
            Write-Log "  ------------------------------------------------" "White"

            $testSizes = @(10000, 50000, 100000, 250000, 500000)
            $bestRate = 0
            $bestSize = $BatchSize
            $benchRowsDeleted = 0  # track total deleted across all sizes
            $declines = 0          # consecutive throughput declines

            foreach ($testSize in $testSizes) {
                # Skip if we've already deleted most rows (estimate remaining)
                $estRemain = $benchBest.PurgeCount - $benchRowsDeleted
                if ($estRemain -lt $testSize) {
                    Write-Log ("  {0,7}: SKIP (~{1} rows remain)" -f $testSize, $estRemain) "DarkGray"
                    continue
                }

                $sw = [System.Diagnostics.Stopwatch]::StartNew()
                $trialTotal = 0
                # Two trial batches per size
                for ($trial = 0; $trial -lt 2; $trial++) {
                    try {
                        $delQuery = "SET NOCOUNT ON; DECLARE @d INT; DELETE TOP ($testSize) FROM [$($benchBest.TableName)] WHERE [$($benchBest.DateColumn)] < '$cutoffStr'; SET @d = @@ROWCOUNT; IF @d > 0 CHECKPOINT; SELECT @d AS Deleted;"
                        $delR = Invoke-Sqlcmd @connParams -Database $db -Query $delQuery
                        $trialTotal += $delR.Deleted
                        if ($delR.Deleted -eq 0) { break }
                    } catch { break }
                }
                $sw.Stop()
                $elapsedMs = $sw.ElapsedMilliseconds
                $rate = if ($elapsedMs -gt 0) { [math]::Round(($trialTotal * 1000) / $elapsedMs) } else { 0 }
                $benchRowsDeleted += $trialTotal

                $estRemaining = $benchBest.PurgeCount - $benchRowsDeleted
                $estStr = if ($rate -gt 0) {
                    $estSec = [math]::Round($estRemaining / $rate)
                    "{0}h {1}m" -f [math]::Floor($estSec / 3600), [math]::Floor(($estSec % 3600) / 60)
                } else { "?" }

                Write-Log ("  {0,7}: {1} rows in {2}ms = ~{3} rows/sec  (est. total: {4})" -f $testSize, $trialTotal, $elapsedMs, $rate, $estStr) "White"

                if ($rate -gt $bestRate) {
                    $bestRate = $rate
                    $bestSize = $testSize
                    $declines = 0
                }
                else {
                    $declines++
                    if ($declines -ge 2) {
                        Write-Log "  (early exit — throughput declining)" "DarkGray"
                        break
                    }
                }
            }

            Write-Log "  ------------------------------------------------" "White"
            Write-Log "  Winner: $bestSize rows/batch (~$bestRate rows/sec)" "Green"
            $BatchSize = $bestSize
            Write-Log "  Using BatchSize = $BatchSize for remaining cleanup." "Cyan"
            Write-Log "  =============================================" "Cyan"
        }
        else {
            $cnt = if ($benchBest) { $benchBest.PurgeCount } else { 0 }
            Write-Log "  Benchmark skipped: largest table has < 100K rows to purge ($cnt)" "Yellow"
            Write-Log "  Using default BatchSize = $BatchSize" "Yellow"
        }
    }

    # Delete in FK-safe order
    Write-Log "" "White"
    Write-Log "  Deleting..." "White"

    $cleanedTables = @()  # track tables that had rows purged

    foreach ($tbl in $deleteOrder) {
        # Determine delete strategy for this table
        $isFkJoin  = $fkJoinDeletes.ContainsKey($tbl)
        $hasDateCol = $dateColumns.ContainsKey($tbl)

        if (-not $isFkJoin -and -not $hasDateCol) {
            # Table not in this HDB version or not relevant
            continue
        }

        # Verify table exists
        try {
            $existsQuery = "SELECT OBJECT_ID('$tbl', 'U') AS ObjId"
            $existsResult = Invoke-Sqlcmd @connParams -Database $db -Query $existsQuery
            if ($null -eq $existsResult.ObjId) { continue }
        }
        catch { continue }

        try {
            $totalDeleted = 0

            if ($isFkJoin -and $fkJoinDeletes[$tbl].ParentDateCol) {
                # ── FK-join delete: materialize keys, index, delete by key — all in one session ──
                $fk = $fkJoinDeletes[$tbl]

                # Single multi-statement batch: collect keys → index → batched delete → cleanup
                # This avoids temp table scope issues across Invoke-Sqlcmd calls.
                $fkBatchQuery = "SET NOCOUNT ON; " +
                    "SELECT child.[$($fk.ChildFK)] AS KeyVal " +
                    "INTO #FKKeysToDelete " +
                    "FROM [$tbl] child " +
                    "INNER JOIN [$($fk.ParentTable)] parent " +
                    "ON child.[$($fk.ChildFK)] = parent.[$($fk.ParentPK)] " +
                    "WHERE parent.[$($fk.ParentDateCol)] < '$cutoffStr'; " +
                    "CREATE CLUSTERED INDEX IX_FKKeys ON #FKKeysToDelete (KeyVal); " +
                    "DECLARE @kc BIGINT = (SELECT COUNT(*) FROM #FKKeysToDelete); " +
                    "DECLARE @d INT = 1, @tot BIGINT = 0, @st DATETIME = GETDATE(); " +
                    "WHILE @d > 0 BEGIN " +
                    "  DELETE TOP ($BatchSize) t FROM [$tbl] t INNER JOIN #FKKeysToDelete k ON t.[$($fk.ChildFK)] = k.KeyVal; " +
                    "  SET @d = @@ROWCOUNT; SET @tot = @tot + @d; " +
                    "  IF @d > 0 CHECKPOINT; " +
                    "END; " +
                    "DROP TABLE #FKKeysToDelete; " +
                    "SELECT @kc AS KeyCount, @tot AS TotalDeleted, DATEDIFF(SECOND, @st, GETDATE()) AS ElapsedSec;"

                Write-Log "    $tbl : collecting keys via FK join (one-time)..." "DarkCyan"
                $sw = [System.Diagnostics.Stopwatch]::StartNew()
                $fkResult = Invoke-Sqlcmd @connParams -Database $db -Query $fkBatchQuery -QueryTimeout 0
                $sw.Stop()
                $keyCount = $fkResult.KeyCount
                $totalDeleted = $fkResult.TotalDeleted
                $elapsed = [math]::Round($sw.Elapsed.TotalSeconds, 1)
                $rate = if ($elapsed -gt 0) { [math]::Round($totalDeleted / $elapsed) } else { 0 }
                Write-Log "    $tbl : $totalDeleted/$keyCount keys deleted | ${elapsed}s | ~${rate} rows/sec" "DarkGray"
            }
            elseif ($hasDateCol) {
                # ── Direct date-column delete ──
                $col = $dateColumns[$tbl]
                $sw = [System.Diagnostics.Stopwatch]::StartNew()
                do {
                    $deleteQuery = "SET NOCOUNT ON; DECLARE @d INT; DELETE TOP ($BatchSize) FROM [$tbl] WHERE [$col] < '$cutoffStr'; SET @d = @@ROWCOUNT; IF @d > 0 CHECKPOINT; SELECT @d AS Deleted;"
                    $delResult = Invoke-Sqlcmd @connParams -Database $db -Query $deleteQuery
                    $deleted = $delResult.Deleted
                    $totalDeleted += $deleted
                    if ($deleted -gt 0) {
                        $elapsed = [math]::Round($sw.Elapsed.TotalSeconds, 1)
                        $rate = if ($elapsed -gt 0) { [math]::Round($totalDeleted / $elapsed) } else { 0 }
                        Write-Log "    $tbl : batch $deleted ($totalDeleted total) | ${elapsed}s | ~${rate} rows/sec" "DarkGray"
                        if ($BatchDelaySec -gt 0) { Start-Sleep -Seconds $BatchDelaySec }
                    }
                } while ($deleted -eq $BatchSize)
            }
            else {
                Write-Log "  SKIP $tbl : no date column and no FK-join rule" "Yellow"
                continue
            }

            if ($totalDeleted -gt 0) {
                Write-Log "  OK $tbl : $totalDeleted rows purged" "Green"
                $dbTotal += $totalDeleted
                $cleanedTables += $tbl
            }
            else {
                Write-Log "  OK $tbl : nothing to purge" "DarkGray"
            }
        }
        catch {
            Write-Log "  ERROR $tbl : $($_.Exception.Message)" "Red"
        }
    }

    # Post-cleanup: drop temp indexes
    if ($tempIndexes.Count -gt 0) {
        Write-Log "" "White"
        Write-Log "  Dropping temp indexes..." "Cyan"
        foreach ($idx in $tempIndexes) {
            try {
                Invoke-Sqlcmd @connParams -Database $db -Query "DROP INDEX [$($idx.IndexName)] ON [$($idx.TableName)]"
                Write-Log "    Dropped $($idx.IndexName)" "DarkGray"
            }
            catch {
                Write-Log "    WARN Could not drop $($idx.IndexName): $($_.Exception.Message)" "Yellow"
            }
        }
    }

    # Post-cleanup: targeted stats update only on cleaned tables (much faster than sp_updatestats)
    if ($cleanedTables.Count -gt 0) {
        Write-Log "" "White"
        Write-Log "  Updating statistics on $($cleanedTables.Count) cleaned tables..." "Cyan"
        foreach ($tbl in $cleanedTables) {
            try {
                Invoke-Sqlcmd @connParams -Database $db -Query "UPDATE STATISTICS [$tbl]" -QueryTimeout 300
                Write-Log "    OK $tbl" "DarkGray"
            }
            catch {
                Write-Log "    WARN $tbl : $($_.Exception.Message)" "Yellow"
            }
        }
        Write-Log "  OK Statistics updated" "Green"

        # Recommend index rebuild for heavily deleted tables
        Write-Log "" "White"
        Write-Log "  TIP: After mass deletes, indexes may be fragmented." "Yellow"
        Write-Log "  Consider running this during a maintenance window:" "Yellow"
        foreach ($tbl in $cleanedTables) {
            Write-Log "    ALTER INDEX ALL ON [$tbl] REBUILD;" "DarkCyan"
        }
    }

    Write-Log "" "White"
    Write-Log "  Database $db total: $dbTotal rows purged" "Cyan"
    $allDbsTotal += $dbTotal
}

Write-Log "" "White"
Write-Log "================================================" "Cyan"
Write-Log "All databases complete." "Cyan"
Write-Log "Grand total rows purged: $allDbsTotal" "Cyan"
Write-Log "Log saved to: $LogFile" "Cyan"
Write-Log "================================================" "Cyan"
