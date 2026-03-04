<#
.SYNOPSIS
    Creates (or reuses) a Fabric workspace for the Tableau-to-Fabric migration.

.DESCRIPTION
    Idempotent: if a workspace with the given name already exists it is reused.
    Optionally assigns the workspace to a Fabric capacity.

    Adapted from the HorizonBooks New-HorizonBooksWorkspace.ps1 pattern.

.EXAMPLE
    .\New-MigrationWorkspace.ps1 -WorkspaceName "Contoso Sales" -CapacityId "abc-123"

.EXAMPLE
    .\New-MigrationWorkspace.ps1 -WorkspaceName "Contoso Sales"
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceName,

    [Parameter()]
    [string]$CapacityId = "",

    [Parameter()]
    [string]$Description = "Workspace created by Tableau-to-Fabric migration"
)

$ErrorActionPreference = "Stop"

# ── Load shared module ──────────────────────────────────────────────────
Import-Module (Join-Path $PSScriptRoot 'TableauToFabric.psm1') -Force

$timings = [System.Collections.Generic.List[PSCustomObject]]::new()

Write-Banner "Tableau-to-Fabric: Workspace Setup"

# ============================================================================
# Step 0 - Authenticate
# ============================================================================
Write-Step "0" "Authenticating to Azure"

$ctx = Get-AzContext
if (-not $ctx) {
    Write-Info "No active Azure session - running Connect-AzAccount ..."
    Connect-AzAccount | Out-Null
    $ctx = Get-AzContext
}
Write-Success "Signed in as $($ctx.Account.Id) (tenant $($ctx.Tenant.Id))"

$token = Get-FabricToken
Write-Success "Fabric API token acquired"

# ============================================================================
# Step 1 - Create or reuse workspace
# ============================================================================
Write-Step "1" "Creating workspace '$WorkspaceName'"

$workspaceId = $null

Measure-Step -Name "Create Workspace" -Timings $timings -Block {
    $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
    $body = @{
        displayName = $WorkspaceName
        description = $Description
    } | ConvertTo-Json

    try {
        $resp = Invoke-WebRequest -Method Post `
            -Uri "https://api.fabric.microsoft.com/v1/workspaces" `
            -Headers $headers -Body $body -UseBasicParsing

        if ($resp.StatusCode -eq 201) {
            $script:workspaceId = ($resp.Content | ConvertFrom-Json).id
            Write-Success "Workspace created: $script:workspaceId"
        }
    }
    catch {
        $errBody = ""
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $errBody = $sr.ReadToEnd(); $sr.Close()
        } catch {}

        if ($errBody -like "*WorkspaceNameAlreadyExists*" -or
            $errBody -like "*already exists*" -or
            $errBody -like "*already in use*") {
            Write-Info "Workspace '$WorkspaceName' already exists - looking it up..."

            $existing = (Invoke-RestMethod `
                -Uri "https://api.fabric.microsoft.com/v1/workspaces" `
                -Headers @{ Authorization = "Bearer $token" }).value

            $found = $existing |
                Where-Object { $_.displayName -eq $WorkspaceName } |
                Select-Object -First 1

            if ($found) {
                $script:workspaceId = $found.id
                Write-Success "Reusing workspace: $script:workspaceId"
            }
            else {
                throw "Workspace '$WorkspaceName' exists but not found in list."
            }
        }
        else {
            throw "Failed to create workspace: $errBody"
        }
    }

    $workspaceId = $script:workspaceId
}

if (-not $workspaceId) { $workspaceId = $script:workspaceId }

# ============================================================================
# Step 2 - Assign to capacity (optional)
# ============================================================================
if ($CapacityId) {
    Write-Step "2" "Assigning workspace to capacity $CapacityId"

    Measure-Step -Name "Assign Capacity" -Timings $timings -Block {
        $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
        $body = @{ capacityId = $CapacityId } | ConvertTo-Json

        try {
            $resp = Invoke-WebRequest -Method Post `
                -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/assignToCapacity" `
                -Headers $headers -Body $body -UseBasicParsing

            if ($resp.StatusCode -eq 202) {
                Write-Info "Capacity assignment accepted (202) - waiting 10s..."
                Start-Sleep -Seconds 10
            }
            Write-Success "Capacity assigned"
        }
        catch {
            $errBody = ""
            try {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $errBody = $sr.ReadToEnd(); $sr.Close()
            } catch {}

            if ($errBody -like "*already assigned*" -or
                $errBody -like "*AssignedToCapacity*") {
                Write-Info "Workspace already assigned to a capacity"
            }
            else {
                Write-Warn "Capacity assignment failed: $errBody"
            }
        }
    }
}
else {
    Write-Info "No CapacityId specified - skipping capacity assignment."
    Write-Info "(Workspace must already be on a Fabric capacity, or assign one in the portal.)"
}

# ============================================================================
# Done
# ============================================================================
Show-TimingSummary $timings

$portalUrl = "https://app.fabric.microsoft.com/groups/$workspaceId"

Write-Banner "WORKSPACE READY" Green
Write-Host ""
Write-Host "  Workspace : $WorkspaceName" -ForegroundColor White
Write-Host "  ID        : $workspaceId" -ForegroundColor White
Write-Host "  Portal    : $portalUrl" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Next step:" -ForegroundColor Yellow
Write-Host "    .\Deploy-TableauMigration.ps1 -WorkspaceId $workspaceId -ProjectDir <path>" -ForegroundColor Gray
Write-Host ""

# Return object for pipeline usage
[PSCustomObject]@{
    WorkspaceId   = $workspaceId
    WorkspaceName = $WorkspaceName
    PortalUrl     = $portalUrl
}
