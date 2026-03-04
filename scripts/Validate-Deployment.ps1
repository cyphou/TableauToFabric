<#
.SYNOPSIS
    Validates a Tableau-to-Fabric deployment by checking that all expected
    items exist in the target workspace.

.DESCRIPTION
    Reads the migration project directory, discovers expected artifacts,
    and verifies each one exists in the Fabric workspace.  Also checks
    SQL endpoint, lakehouse tables, dataflow connections, and folder
    organization.  Reports any missing or misconfigured items.

.EXAMPLE
    .\Validate-Deployment.ps1 -WorkspaceId "xxx" -ProjectDir "C:\output\Superstore"
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceId,

    [Parameter(Mandatory)]
    [string]$ProjectDir
)

$ErrorActionPreference = "Stop"

# ── Load shared module ──────────────────────────────────────────────────
Import-Module (Join-Path $PSScriptRoot 'TableauToFabric.psm1') -Force

Write-Banner "Tableau-to-Fabric: Deployment Validation"

# ── Auth ─────────────────────────────────────────────────────────────────
$ctx = Get-AzContext
if (-not $ctx) {
    Write-Info "No active Azure session - running Connect-AzAccount ..."
    Connect-AzAccount | Out-Null
}
$token = Get-FabricToken

# ── Verify workspace ────────────────────────────────────────────────────
Write-Step "1" "Checking workspace"
try {
    $ws = Invoke-RestMethod `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId" `
        -Headers @{ Authorization = "Bearer $token" }
    Write-Success "Workspace: $($ws.displayName)"
}
catch {
    Write-Err "Workspace $WorkspaceId not accessible."
    exit 1
}

# ── List all items in workspace ──────────────────────────────────────────
Write-Step "2" "Listing workspace items"
$allItems = @()
$continuationUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items"
while ($continuationUrl) {
    $resp = Invoke-RestMethod -Uri $continuationUrl `
        -Headers @{ Authorization = "Bearer $token" }
    $allItems += $resp.value
    $continuationUrl = $resp.continuationUri
}
Write-Info "Found $($allItems.Count) items in workspace"

# ── Per-type counts ──────────────────────────────────────────────────────
$typeCounts = $allItems | Group-Object -Property type |
    Sort-Object -Property Count -Descending
foreach ($tc in $typeCounts) {
    Write-Info "  $($tc.Name) : $($tc.Count)"
}

# ── Expected artifacts from project ─────────────────────────────────────
Write-Step "3" "Comparing expected vs actual"

$typeMap = @{
    "Lakehouse"     = "Lakehouse"
    "Notebook"      = "Notebook"
    "Dataflow"      = "DataflowGen2"
    "SemanticModel" = "SemanticModel"
    "Report"        = "Report"
    "Pipeline"      = "DataPipeline"
}

$artDirs = Get-ChildItem -Path $ProjectDir -Directory

$expected = @()
foreach ($dir in $artDirs) {
    foreach ($suffix in $typeMap.Keys) {
        if ($dir.Name -like "*.$suffix") {
            $displayName = $dir.Name -replace "\.$suffix$", ""
            $fabricType  = $typeMap[$suffix]
            # Lakehouses use sanitized names
            if ($suffix -eq "Lakehouse") {
                $displayName = $displayName -replace '[^a-zA-Z0-9_]', '_'
            }
            $expected += [PSCustomObject]@{
                DisplayName = $displayName
                FabricType  = $fabricType
                Suffix      = $suffix
            }
            break
        }
    }
}

$passed = 0
$failed = 0
$foundIds = @{}   # track found item IDs for deeper checks

foreach ($exp in $expected) {
    $found = $allItems |
        Where-Object { $_.displayName -eq $exp.DisplayName -and $_.type -eq $exp.FabricType } |
        Select-Object -First 1

    if ($found) {
        Write-Success ("{0,-15} '{1}' - {2}" -f $exp.Suffix, $exp.DisplayName, $found.id)
        $passed++
        $foundIds[$exp.Suffix] = $found.id
    }
    else {
        Write-Err ("{0,-15} '{1}' - NOT FOUND" -f $exp.Suffix, $exp.DisplayName)
        $failed++
    }
}

# ── Lakehouse SQL endpoint ──────────────────────────────────────────────
$lhItem = $allItems |
    Where-Object { $_.type -eq "Lakehouse" } |
    Select-Object -First 1

if ($lhItem) {
    Write-Step "4" "Checking Lakehouse SQL endpoint"
    try {
        $lh = Invoke-RestMethod `
            -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/lakehouses/$($lhItem.id)" `
            -Headers @{ Authorization = "Bearer $token" }
        $ep = $lh.properties.sqlEndpointProperties.connectionString
        if ($ep) {
            Write-Success "SQL endpoint: $ep"
            $passed++
        }
        else {
            Write-Warn "SQL endpoint not yet provisioned"
            $failed++
        }
    }
    catch { Write-Warn "Could not check SQL endpoint"; $failed++ }

    # ── Lakehouse tables ─────────────────────────────────────────────
    Write-Step "5" "Checking Lakehouse tables"
    try {
        $tables = Invoke-RestMethod `
            -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/lakehouses/$($lhItem.id)/tables" `
            -Headers @{ Authorization = "Bearer $token" }
        $tableList = $tables.data
        if ($tableList -and $tableList.Count -gt 0) {
            Write-Success "Lakehouse has $($tableList.Count) table(s):"
            foreach ($t in $tableList) {
                Write-Info "  - $($t.name) ($($t.type), $($t.format))"
            }
            $passed++
        }
        else {
            Write-Info "Lakehouse has no tables yet (tables will appear after ETL runs)"
        }
    }
    catch {
        Write-Info "Could not enumerate tables (API may not support this for all SKUs)"
    }
}

# ── Dataflow connections ─────────────────────────────────────────────────
$dfItems = $allItems | Where-Object { $_.type -like "*Dataflow*" }
if ($dfItems.Count -gt 0) {
    Write-Step "6" "Checking Dataflow connections"
    $token = Get-FabricToken

    foreach ($df in $dfItems) {
        try {
            $connections = Invoke-RestMethod `
                -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$($df.id)/connections" `
                -Headers @{ Authorization = "Bearer $token" }
            $connList = $connections.value
            if ($connList -and $connList.Count -gt 0) {
                $bound = $connList | Where-Object { $_.connectionDetails }
                Write-Success "Dataflow '$($df.displayName)': $($connList.Count) connection(s), $($bound.Count) bound"
                $passed++
            }
            else {
                Write-Info "Dataflow '$($df.displayName)': no connections found (may need manual config)"
            }
        }
        catch {
            Write-Info "Could not check connections for '$($df.displayName)': $($_.Exception.Message)"
        }
    }
}

# ── Workspace folders ────────────────────────────────────────────────────
Write-Step "7" "Checking workspace folders"
try {
    $folders = Invoke-RestMethod `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" `
        -Headers @{ Authorization = "Bearer $token" }
    $folderList = $folders.value
    if ($folderList -and $folderList.Count -gt 0) {
        Write-Success "Workspace has $($folderList.Count) folder(s):"
        foreach ($f in $folderList) {
            Write-Info "  - $($f.displayName) ($($f.id))"
        }
        $passed++
    }
    else {
        Write-Info "No workspace folders created yet"
    }
}
catch {
    Write-Info "Could not list workspace folders: $($_.Exception.Message)"
}

# ── Semantic model definition check ──────────────────────────────────────
$smItem = $allItems | Where-Object { $_.type -eq "SemanticModel" } | Select-Object -First 1
if ($smItem) {
    Write-Step "8" "Checking Semantic Model definition"
    try {
        $smDef = Invoke-RestMethod `
            -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$($smItem.id)/getDefinition" `
            -Method Post `
            -Headers @{ Authorization = "Bearer $token" }
        $defParts = $smDef.definition.parts
        if ($defParts -and $defParts.Count -gt 0) {
            Write-Success "Semantic model '$($smItem.displayName)' has $($defParts.Count) definition parts"
            $passed++
        }
        else {
            Write-Warn "Semantic model definition is empty"
            $failed++
        }
    }
    catch {
        Write-Info "Could not retrieve semantic model definition (LRO or permission issue)"
    }
}

# ── Summary ──────────────────────────────────────────────────────────────
Write-Host ""
Write-Banner "VALIDATION RESULTS"
Write-Host ""
Write-Host "  Expected artifacts : $($expected.Count)" -ForegroundColor White
Write-Host "  Checks passed      : $passed" -ForegroundColor Green
Write-Host "  Checks failed      : $failed" -ForegroundColor $(if ($failed -gt 0) { "Red" } else { "Green" })
Write-Host ""

# Per-type summary
Write-Host "  Item types in workspace:" -ForegroundColor White
foreach ($tc in $typeCounts) {
    Write-Host ("    {0,-20} {1}" -f $tc.Name, $tc.Count) -ForegroundColor Gray
}
Write-Host ""

if ($failed -eq 0) {
    Write-Host "  All validation checks passed!" -ForegroundColor Green
}
else {
    Write-Host "  Some checks failed. Review output above." -ForegroundColor Yellow
    Write-Host "  Re-run Deploy-TableauMigration.ps1 to fix missing items." -ForegroundColor Yellow
}
Write-Host ""
Write-Host "  Portal: https://app.fabric.microsoft.com/groups/$WorkspaceId" -ForegroundColor Cyan
Write-Host ""

exit $failed
