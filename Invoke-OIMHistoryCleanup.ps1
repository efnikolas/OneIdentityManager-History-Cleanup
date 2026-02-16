# ============================================================
# One Identity Manager — HDB Cleanup PowerShell Wrapper
# ============================================================
# Purges archived data older than N years from one or more
# OIM History Databases (TimeTrace databases).
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

# FK-safe delete order: children first, parents last
$deleteOrder = @(
    'RawWatchProperty',
    'RawWatchOperation',
    'RawProcessStep',
    'RawProcessSubstitute',
    'RawProcessChain',
    'RawProcess',
    'RawProcessGroup',
    'RawJobHistory',
    'WatchProperty',
    'WatchOperation',
    'ProcessStep',
    'ProcessSubstitute',
    'ProcessChain',
    'ProcessInfo',
    'ProcessGroup',
    'HistoryJob',
    'HistoryChain'
)

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

    # Discover date columns for all tables in this HDB
    $discoverQuery = "SELECT t.name AS TableName, c.name AS DateColumn " +
        "FROM sys.tables t " +
        "CROSS APPLY ( " +
        "SELECT TOP 1 c.name FROM sys.columns c " +
        "INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id " +
        "WHERE c.object_id = t.object_id " +
        "AND ty.name IN ('datetime','datetime2','smalldatetime','date') " +
        "ORDER BY CASE c.name " +
        "WHEN 'XDateInserted' THEN 1 WHEN 'XDateUpdated' THEN 2 " +
        "WHEN 'StartDate' THEN 3 WHEN 'EndDate' THEN 4 ELSE 5 END, " +
        "c.column_id) c " +
        "WHERE t.name NOT IN ('SourceColumn','SourceDatabase','SourceTable') " +
        "ORDER BY t.name"

    try {
        $tableInfo = Invoke-Sqlcmd @connParams -Database $db -Query $discoverQuery
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

    # Pre-flight counts
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

    if ($WhatIf) {
        Write-Log "" "White"
        Write-Log "  WhatIf mode — no data deleted for $db" "Cyan"
        continue
    }

    # Delete in FK-safe order
    Write-Log "" "White"
    Write-Log "  Deleting..." "White"

    foreach ($tbl in $deleteOrder) {
        if (-not $dateColumns.ContainsKey($tbl)) {
            # Table may not exist in this HDB version
            continue
        }

        $col = $dateColumns[$tbl]

        try {
            $totalDeleted = 0
            do {
                $deleteQuery = "SET NOCOUNT ON; DELETE TOP ($BatchSize) FROM [$tbl] WHERE [$col] < '$cutoffStr'; SELECT @@ROWCOUNT AS Deleted;"
                $delResult = Invoke-Sqlcmd @connParams -Database $db -Query $deleteQuery
                $deleted = $delResult.Deleted
                $totalDeleted += $deleted
                if ($deleted -gt 0) {
                    Write-Log "    $tbl : deleted batch of $deleted ($totalDeleted total)" "DarkGray"
                }
            } while ($deleted -eq $BatchSize)

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
