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

    [int]$BatchSize = 10000,

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

    # Pre-flight counts — tables with their own date column
    Write-Log "" "White"
    Write-Log "  Pre-flight summary:" "Yellow"
    foreach ($row in $tableInfo) {
        $tbl = $row.TableName
        $col = $row.DateColumn
        try {
            $countQuery = "SELECT COUNT(*) AS Total, ISNULL(SUM(CASE WHEN [$col] < '$cutoffStr' THEN 1 ELSE 0 END), 0) AS ToPurge FROM [$tbl]"
            $result = Invoke-Sqlcmd @connParams -Database $db -Query $countQuery
            Write-Log "    $tbl : $($result.Total) total, $($result.ToPurge) to purge (by $col)" "Yellow"
        }
        catch {
            Write-Log "    $tbl : error counting — $($_.Exception.Message)" "Red"
        }
    }

    # Pre-flight counts — FK-joined tables (no date column)
    foreach ($childTbl in @($fkJoinDeletes.Keys)) {
        $fk = $fkJoinDeletes[$childTbl]
        if (-not $fk.ParentDateCol) { continue }
        # Check if child table exists in this database
        try {
            $existsQuery = "SELECT OBJECT_ID('$childTbl', 'U') AS ObjId"
            $existsResult = Invoke-Sqlcmd @connParams -Database $db -Query $existsQuery
            if ($null -eq $existsResult.ObjId) { continue }
        }
        catch { continue }

        try {
            $fkCountQuery = "SELECT " +
                "(SELECT COUNT(*) FROM [$childTbl]) AS Total, " +
                "(SELECT COUNT(*) FROM [$childTbl] child " +
                "INNER JOIN [$($fk.ParentTable)] parent ON child.[$($fk.ChildFK)] = parent.[$($fk.ParentPK)] " +
                "WHERE parent.[$($fk.ParentDateCol)] < '$cutoffStr') AS ToPurge"
            $fkResult = Invoke-Sqlcmd @connParams -Database $db -Query $fkCountQuery
            Write-Log "    $childTbl : $($fkResult.Total) total, $($fkResult.ToPurge) to purge (via $($fk.ParentTable).$($fk.ParentDateCol))" "Yellow"
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

    # Delete in FK-safe order
    Write-Log "" "White"
    Write-Log "  Deleting..." "White"

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
                # ── FK-join delete (no date column on this table) ──
                $fk = $fkJoinDeletes[$tbl]
                do {
                    $deleteQuery = "SET NOCOUNT ON; " +
                        "DELETE TOP ($BatchSize) child FROM [$tbl] child " +
                        "INNER JOIN [$($fk.ParentTable)] parent " +
                        "ON child.[$($fk.ChildFK)] = parent.[$($fk.ParentPK)] " +
                        "WHERE parent.[$($fk.ParentDateCol)] < '$cutoffStr'; " +
                        "SELECT @@ROWCOUNT AS Deleted;"
                    $delResult = Invoke-Sqlcmd @connParams -Database $db -Query $deleteQuery
                    $deleted = $delResult.Deleted
                    $totalDeleted += $deleted
                    if ($deleted -gt 0) {
                        Write-Log "    $tbl : deleted batch of $deleted ($totalDeleted total) [FK join -> $($fk.ParentTable)]" "DarkGray"
                    }
                } while ($deleted -eq $BatchSize)
            }
            elseif ($hasDateCol) {
                # ── Direct date-column delete ──
                $col = $dateColumns[$tbl]
                do {
                    $deleteQuery = "SET NOCOUNT ON; DELETE TOP ($BatchSize) FROM [$tbl] WHERE [$col] < '$cutoffStr'; SELECT @@ROWCOUNT AS Deleted;"
                    $delResult = Invoke-Sqlcmd @connParams -Database $db -Query $deleteQuery
                    $deleted = $delResult.Deleted
                    $totalDeleted += $deleted
                    if ($deleted -gt 0) {
                        Write-Log "    $tbl : deleted batch of $deleted ($totalDeleted total)" "DarkGray"
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
            }
            else {
                Write-Log "  OK $tbl : nothing to purge" "DarkGray"
            }
        }
        catch {
            Write-Log "  ERROR $tbl : $($_.Exception.Message)" "Red"
        }
    }

    # Post-cleanup stats update
    if ($dbTotal -gt 0) {
        Write-Log "" "White"
        Write-Log "  Updating statistics for $db..." "Cyan"
        try {
            Invoke-Sqlcmd @connParams -Database $db -Query "EXEC sp_updatestats"
            Write-Log "  OK Statistics updated" "Green"
        }
        catch {
            Write-Log "  ERROR sp_updatestats: $($_.Exception.Message)" "Red"
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
