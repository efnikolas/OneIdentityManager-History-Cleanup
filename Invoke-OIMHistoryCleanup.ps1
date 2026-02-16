# ============================================================
# One Identity Manager — PowerShell History Cleanup Wrapper
# ============================================================
# Run against one or more OIM HISTORY DATABASES (HDBs).
# Schedule via Task Scheduler or One Identity Job Server.
#
# Usage:
#   # Single HDB — dry run (report only):
#   .\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIM_HDB" -WhatIf
#
#   # Single HDB — actual cleanup:
#   .\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIM_HDB"
#
#   # Multiple HDBs:
#   .\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIM_HDB1","OneIM_HDB2","OneIM_HDB3"
#
#   # Multiple HDBs — dry run:
#   .\Invoke-OIMHistoryCleanup.ps1 -SqlServer "myserver" -Database "OneIM_HDB1","OneIM_HDB2" -WhatIf
# ============================================================

param(
    [Parameter(Mandatory = $true)]
    [string]$SqlServer,

    [Parameter(Mandatory = $true)]
    [string[]]$Database,

    [int]$RetentionYears = 2,

    [int]$BatchSize = 10000,

    [switch]$WhatIf
)

$CutoffDate = (Get-Date).AddYears(-$RetentionYears)
$LogFile = Join-Path $PSScriptRoot "cleanup_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

function Write-Log {
    param([string]$Message, [string]$Color = "White")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logEntry = "[$timestamp] $Message"
    Write-Host $logEntry -ForegroundColor $Color
    Add-Content -Path $LogFile -Value $logEntry
}

Write-Log "=============================================" "Cyan"
Write-Log "One Identity Manager History DB Cleanup" "Cyan"
Write-Log "=============================================" "Cyan"
Write-Log "Server:      $SqlServer"
Write-Log "Databases:   $($Database -join ', ')"
Write-Log "Cutoff:      $CutoffDate (retain last $RetentionYears years)"
Write-Log "BatchSize:   $BatchSize"
Write-Log "WhatIf:      $WhatIf"
Write-Log "Log File:    $LogFile"
Write-Log "=============================================" "Cyan"

# Known history tables — ORDER MATTERS (children before parents)
$historyTables = @(
    @{ Name = "DialogHistory";       Column = "XDateInserted" },
    @{ Name = "DialogJournalDetail"; Column = "XDateInserted"; Parent = "DialogJournal"; ParentKey = "UID_DialogJournal" },
    @{ Name = "DialogJournal";       Column = "XDateInserted" },
    @{ Name = "JobHistory";          Column = "XDateInserted" },
    @{ Name = "PersonWantsOrg";      Column = "XDateUpdated"  },
    @{ Name = "QBMDBQueueHistory";   Column = "XDateInserted" },
    @{ Name = "QBMProcessHistory";   Column = "XDateInserted" },
    @{ Name = "QBMDBQueueSlotHistory"; Column = "XDateInserted" }
)

$excludeFromDynamic = $historyTables | ForEach-Object { $_.Name }

$cutoffStr = $CutoffDate.ToString("yyyy-MM-dd")
$allDbsTotal = 0

foreach ($db in $Database) {
    Write-Log "" "White"
    Write-Log "=============================================" "Magenta"
    Write-Log "Processing database: $db" "Magenta"
    Write-Log "=============================================" "Magenta"

    $dbTotal = 0

    foreach ($tbl in $historyTables) {
        $table = $tbl.Name
        $col = $tbl.Column

        # If this table has a parent dependency, delete via JOIN on parent's date
        if ($tbl.Parent) {
            $countQuery = @"
SELECT COUNT(*) AS Cnt
FROM [$table] c
INNER JOIN [$($tbl.Parent)] p ON c.$($tbl.ParentKey) = p.$($tbl.ParentKey)
WHERE p.XDateInserted < '$cutoffStr'
"@
        }
        else {
            $countQuery = "SELECT COUNT(*) AS Cnt FROM [$table] WHERE [$col] < '$cutoffStr'"
        }

        try {
            $result = Invoke-Sqlcmd -ServerInstance $SqlServer -Database $db -Query $countQuery -ErrorAction Stop
            $rowCount = $result.Cnt
            Write-Log "  $table : $rowCount rows older than $RetentionYears years" "Yellow"

            if ($WhatIf) {
                continue
            }

            if ($rowCount -gt 0) {
                $totalDeleted = 0

                if ($tbl.Parent) {
                    do {
                        $deleteQuery = @"
DELETE TOP ($BatchSize) c
FROM [$table] c
INNER JOIN [$($tbl.Parent)] p ON c.$($tbl.ParentKey) = p.$($tbl.ParentKey)
WHERE p.XDateInserted < '$cutoffStr';
SELECT @@ROWCOUNT AS Deleted;
"@
                        $delResult = Invoke-Sqlcmd -ServerInstance $SqlServer -Database $db -Query $deleteQuery -ErrorAction Stop
                        $deleted = $delResult.Deleted
                        $totalDeleted += $deleted
                        Write-Log "    Deleted batch of $deleted... ($totalDeleted total)" "DarkGray"
                    } while ($deleted -eq $BatchSize)
                }
                else {
                    do {
                        $deleteQuery = "DELETE TOP ($BatchSize) FROM [$table] WHERE [$col] < '$cutoffStr'; SELECT @@ROWCOUNT AS Deleted;"
                        $delResult = Invoke-Sqlcmd -ServerInstance $SqlServer -Database $db -Query $deleteQuery -ErrorAction Stop
                        $deleted = $delResult.Deleted
                        $totalDeleted += $deleted
                        Write-Log "    Deleted batch of $deleted... ($totalDeleted total)" "DarkGray"
                    } while ($deleted -eq $BatchSize)
                }

                Write-Log "  ✓ $table : $totalDeleted rows purged" "Green"
                $dbTotal += $totalDeleted
            }
        }
        catch {
            Write-Log "  ⚠ Skipped $table — $($_.Exception.Message)" "Red"
        }
    }

    # Orphan cleanup for DialogJournalDetail
    Write-Log "  Cleaning orphaned DialogJournalDetail..." "Yellow"
    try {
        if (-not $WhatIf) {
            $orphanTotal = 0
            do {
                $orphanQuery = @"
DELETE TOP ($BatchSize) djd
FROM DialogJournalDetail djd
LEFT JOIN DialogJournal dj ON djd.UID_DialogJournal = dj.UID_DialogJournal
WHERE dj.UID_DialogJournal IS NULL;
SELECT @@ROWCOUNT AS Deleted;
"@
                $delResult = Invoke-Sqlcmd -ServerInstance $SqlServer -Database $db -Query $orphanQuery -ErrorAction Stop
                $deleted = $delResult.Deleted
                $orphanTotal += $deleted
            } while ($deleted -eq $BatchSize)

            if ($orphanTotal -gt 0) {
                Write-Log "  ✓ DialogJournalDetail orphans: $orphanTotal rows purged" "Green"
                $dbTotal += $orphanTotal
            }
            else {
                Write-Log "  DialogJournalDetail: no orphans found" "DarkGray"
            }
        }
    }
    catch {
        Write-Log "  ⚠ Orphan cleanup error — $($_.Exception.Message)" "Red"
    }

    # Dynamic scan for additional history tables
    Write-Log "" "White"
    Write-Log "Scanning for additional history tables in $db..." "Cyan"

    $excludeList = ($excludeFromDynamic | ForEach-Object { "'$_'" }) -join ", "
    $dynamicQuery = @"
SELECT t.TABLE_NAME
FROM INFORMATION_SCHEMA.COLUMNS c
JOIN INFORMATION_SCHEMA.TABLES t
    ON c.TABLE_NAME = t.TABLE_NAME AND t.TABLE_TYPE = 'BASE TABLE'
WHERE c.COLUMN_NAME = 'XDateInserted'
  AND t.TABLE_NAME LIKE '%History%'
  AND t.TABLE_NAME NOT IN ($excludeList)
ORDER BY t.TABLE_NAME
"@

    try {
        $extraTables = Invoke-Sqlcmd -ServerInstance $SqlServer -Database $db -Query $dynamicQuery -ErrorAction Stop

        foreach ($row in $extraTables) {
            $table = $row.TABLE_NAME
            $countQuery = "SELECT COUNT(*) AS Cnt FROM [$table] WHERE XDateInserted < '$cutoffStr'"
            $result = Invoke-Sqlcmd -ServerInstance $SqlServer -Database $db -Query $countQuery -ErrorAction Stop
            $rowCount = $result.Cnt

            Write-Log "  [Dynamic] $table : $rowCount rows older than $RetentionYears years" "Yellow"

            if (-not $WhatIf -and $rowCount -gt 0) {
                $totalDeleted = 0
                do {
                    $deleteQuery = "DELETE TOP ($BatchSize) FROM [$table] WHERE XDateInserted < '$cutoffStr'; SELECT @@ROWCOUNT AS Deleted;"
                    $delResult = Invoke-Sqlcmd -ServerInstance $SqlServer -Database $db -Query $deleteQuery -ErrorAction Stop
                    $deleted = $delResult.Deleted
                    $totalDeleted += $deleted
                    Write-Log "    Deleted batch of $deleted... ($totalDeleted total)" "DarkGray"
                } while ($deleted -eq $BatchSize)

                Write-Log "  ✓ $table : $totalDeleted rows purged" "Green"
                $dbTotal += $totalDeleted
            }
        }
    }
    catch {
        Write-Log "  ⚠ Dynamic scan error — $($_.Exception.Message)" "Red"
    }

    # Post-cleanup maintenance for this database
    if (-not $WhatIf -and $dbTotal -gt 0) {
        Write-Log "" "White"
        Write-Log "Updating statistics for $db..." "Cyan"
        try {
            Invoke-Sqlcmd -ServerInstance $SqlServer -Database $db -Query "EXEC sp_updatestats" -ErrorAction Stop
            Write-Log "✓ Statistics updated for $db" "Green"
        }
        catch {
            Write-Log "⚠ Failed to update statistics — $($_.Exception.Message)" "Red"
        }
    }

    Write-Log "" "White"
    Write-Log "  Database $db total: $dbTotal rows purged" "Cyan"
    $allDbsTotal += $dbTotal
}

Write-Log "" "White"
Write-Log "=============================================" "Cyan"
Write-Log "All databases complete." "Cyan"
Write-Log "Grand total rows purged: $allDbsTotal" "Cyan"
Write-Log "Log saved to: $LogFile" "Cyan"
Write-Log "=============================================" "Cyan"
