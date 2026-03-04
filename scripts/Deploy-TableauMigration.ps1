<#
.SYNOPSIS
    Deploys all Tableau-to-Fabric migration artifacts to a Fabric workspace.

.DESCRIPTION
    Auto-discovers generated artifacts (Lakehouse, Notebook, Dataflow,
    SemanticModel, Report, Pipeline) from a migration project directory
    and deploys them in dependency order via the Fabric REST API.

    Deployment order:
      1. Lakehouse (empty)           → wait for SQL endpoint
      2. Notebook  (+ bind lakehouse)
      3. Dataflow  (Gen2)
      4. Semantic Model (TMDL)       → SQL endpoint + lakehouse-name tokens
      5. Report    (PBIR)            → byConnection rewrite
      6. Pipeline                    → references above items

    Adapted from the HorizonBooks Deploy-Full.ps1 / Deploy-HorizonBooks.ps1
    battle-tested deployment patterns.

.EXAMPLE
    # Deploy all artifacts from a generated project directory
    .\Deploy-TableauMigration.ps1 `
        -WorkspaceId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -ProjectDir  "C:\output\Superstore"

.EXAMPLE
    # Dry-run: show what would be deployed without making API calls
    .\Deploy-TableauMigration.ps1 `
        -WorkspaceId "xxxxxxxx" `
        -ProjectDir  "C:\output\Superstore" `
        -DryRun

.EXAMPLE
    # Deploy and run the ETL notebook automatically
    .\Deploy-TableauMigration.ps1 `
        -WorkspaceId "xxxxxxxx" `
        -ProjectDir  "C:\output\Superstore" `
        -RunNotebooks

.EXAMPLE
    # Deploy and run the full pipeline
    .\Deploy-TableauMigration.ps1 `
        -WorkspaceId "xxxxxxxx" `
        -ProjectDir  "C:\output\Superstore" `
        -RunPipeline
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceId,

    [Parameter(Mandatory)]
    [string]$ProjectDir,

    [Parameter()]
    [switch]$DryRun,

    [Parameter()]
    [switch]$RunNotebooks,

    [Parameter()]
    [switch]$RunPipeline,

    [Parameter()]
    [switch]$SkipLakehouse,

    [Parameter()]
    [switch]$SkipNotebook,

    [Parameter()]
    [switch]$SkipDataflow,

    [Parameter()]
    [switch]$SkipSemanticModel,

    [Parameter()]
    [switch]$SkipReport,

    [Parameter()]
    [switch]$SkipPipeline,

    [Parameter()]
    [switch]$EnableSchemas
)

$ErrorActionPreference = "Stop"

# ── Load shared module ──────────────────────────────────────────────────
Import-Module (Join-Path $PSScriptRoot 'TableauToFabric.psm1') -Force

$timings = [System.Collections.Generic.List[PSCustomObject]]::new()

Write-Banner "Tableau-to-Fabric: Deploy Migration Artifacts"

# ============================================================================
# PRE-FLIGHT - Discover artifacts
# ============================================================================

Write-Step "PRE" "Discovering artifacts in $ProjectDir"

if (-not (Test-Path $ProjectDir)) {
    Write-Err "Project directory not found: $ProjectDir"
    exit 1
}

# ── Discover artifact directories by suffix ──────────────────────────────
$artDirs = Get-ChildItem -Path $ProjectDir -Directory

$lakehouseDirs    = $artDirs | Where-Object { $_.Name -like "*.Lakehouse" }
$notebookDirs     = $artDirs | Where-Object { $_.Name -like "*.Notebook" }
$dataflowDirs     = $artDirs | Where-Object { $_.Name -like "*.Dataflow" }
$semanticModelDirs = $artDirs | Where-Object { $_.Name -like "*.SemanticModel" }
$reportDirs       = $artDirs | Where-Object { $_.Name -like "*.Report" }
$pipelineDirs     = $artDirs | Where-Object { $_.Name -like "*.Pipeline" }

# ── Read migration metadata if available ─────────────────────────────────
$metadataFile = Join-Path $ProjectDir "migration_metadata.json"
$metadata = $null
if (Test-Path $metadataFile) {
    $metadata = Get-Content -Path $metadataFile -Raw -Encoding UTF8 |
        ConvertFrom-Json
    Write-Info "Migration metadata loaded (source: $($metadata.source_file))"
}

# ── Summary ──────────────────────────────────────────────────────────────
Write-Info "Found artifacts:"
Write-Info "  Lakehouses     : $($lakehouseDirs.Count)"
Write-Info "  Notebooks      : $($notebookDirs.Count)"
Write-Info "  Dataflows      : $($dataflowDirs.Count)"
Write-Info "  Semantic Models: $($semanticModelDirs.Count)"
Write-Info "  Reports        : $($reportDirs.Count)"
Write-Info "  Pipelines      : $($pipelineDirs.Count)"

if ($DryRun) {
    Write-Banner "DRY RUN MODE - No API calls will be made" Yellow
    foreach ($d in @($lakehouseDirs) + @($notebookDirs) + @($dataflowDirs) +
                    @($semanticModelDirs) + @($reportDirs) + @($pipelineDirs)) {
        if ($d) {
            $type = $d.Name.Split(".")[-1]
            $name = $d.Name.Substring(0, $d.Name.Length - $type.Length - 1)
            Write-Info "  Would deploy $type : $name"
        }
    }
    Write-Banner "Dry run complete" Green
    exit 0
}

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
Write-Success "Signed in as $($ctx.Account.Id)"

$token = Get-FabricToken
Write-Success "Fabric API token acquired"

# ── Verify workspace exists ──────────────────────────────────────────────
Write-Info "Verifying workspace $WorkspaceId ..."
try {
    $ws = Invoke-RestMethod `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId" `
        -Headers @{ Authorization = "Bearer $token" }
    Write-Success "Workspace: $($ws.displayName)"
}
catch {
    Write-Err "Workspace $WorkspaceId not found or not accessible."
    exit 1
}

# ── Name sanitization helper ─────────────────────────────────────────────
function Get-SafeFabricName {
    param([string]$Name, [string]$Type)
    # Lakehouses require valid database identifier names (no hyphens, no spaces)
    if ($Type -eq "Lakehouse") {
        return ($Name -replace '[^a-zA-Z0-9_]', '_')
    }
    # Other types: replace characters not allowed in Fabric display names
    return ($Name -replace '[^\w\s\-\.]', '_')
}

# ── State tracking ───────────────────────────────────────────────────────
$deployedItems = @{}    # "Type:Name" -> itemId
$lakehouseId   = $null
$lakehouseName = $null
$sqlEndpoint   = $null

# ============================================================================
# Step 1 - Deploy Lakehouses
# ============================================================================
if (-not $SkipLakehouse -and $lakehouseDirs.Count -gt 0) {
    Write-Step "1" "Deploying Lakehouse(s)"

    foreach ($lhDir in $lakehouseDirs) {
        $lhDisplayName = Get-SafeFabricName -Name ($lhDir.Name -replace '\.Lakehouse$', '') -Type "Lakehouse"

        Measure-Step -Name "Lakehouse: $lhDisplayName" -Timings $timings -Block {
            $lhCreationPayload = $null
            if ($EnableSchemas) {
                $lhCreationPayload = @{ enableSchemas = $true }
                Write-Info "Schema-enabled Lakehouse requested"
            }

            $id = New-OrGetFabricItem `
                -DisplayName $lhDisplayName `
                -Type "Lakehouse" `
                -Description "Lakehouse for $lhDisplayName" `
                -WsId $WorkspaceId `
                -Token $token `
                -CreationPayload $lhCreationPayload

            if ($id) {
                $script:lakehouseId   = $id
                $script:lakehouseName = $lhDisplayName
                $deployedItems["Lakehouse:$lhDisplayName"] = $id
                Write-Success "Lakehouse '$lhDisplayName' ready: $id"
            }
            else {
                Write-Warn "Could not create/find Lakehouse '$lhDisplayName'"
            }
        }
    }

    # ── Wait for SQL endpoint ────────────────────────────────────────────
    if ($lakehouseId) {
        Write-Step "1b" "Waiting for SQL analytics endpoint"

        Measure-Step -Name "SQL Endpoint Discovery" -Timings $timings -Block {
            $script:sqlEndpoint = Wait-ForSqlEndpoint `
                -WsId $WorkspaceId `
                -LakehouseId $lakehouseId `
                -Token $token
        }
    }
}
else {
    if ($SkipLakehouse) { Write-Info "Lakehouse deployment skipped - use -SkipLakehouse to control" }
    else { Write-Info "No Lakehouse directories found" }
}

# Update token (may have expired during Lakehouse wait)
$token = Get-FabricToken

# ============================================================================
# Step 2 - Deploy Notebooks
# ============================================================================
if (-not $SkipNotebook -and $notebookDirs.Count -gt 0) {
    Write-Step "2" "Deploying Notebook(s)"

    foreach ($nbDir in $notebookDirs) {
        $nbDisplayName = $nbDir.Name -replace '\.Notebook$', ''

        Measure-Step -Name "Notebook: $nbDisplayName" -Timings $timings -Block {
            # Create the notebook item first
            $nbId = New-OrGetFabricItem `
                -DisplayName $nbDisplayName `
                -Type "Notebook" `
                -Description "ETL notebook for $nbDisplayName" `
                -WsId $WorkspaceId `
                -Token $token

            if (-not $nbId) {
                Write-Warn "Could not create Notebook '$nbDisplayName'"
                return
            }

            $deployedItems["Notebook:$nbDisplayName"] = $nbId

            # Find .ipynb files in the notebook directory
            $notebooks = Get-ChildItem -Path $nbDir.FullName -Filter "*.ipynb"

            foreach ($ipynb in $notebooks) {
                Write-Info "Processing notebook file: $($ipynb.Name)"

                # Read raw content and auto-convert VSCode.Cell format if needed
                $rawContent = Get-Content -Path $ipynb.FullName -Raw -Encoding UTF8
                $nbContent = ConvertFrom-VSCodeCellNotebook $rawContent

                # Convert ipynb JSON to Fabric .py notebook format
                $nbContent = ConvertTo-FabricPyNotebook $nbContent

                # Inject lakehouse binding into the notebook source
                if ($lakehouseId -and $lakehouseName) {
                    # For .py format, we add lakehouse config as a metadata comment block
                    $lhConfig = @"
# METADATA {"trident":{"lakehouse":{"default_lakehouse":"$lakehouseId","default_lakehouse_name":"$lakehouseName","default_lakehouse_workspace_id":"$WorkspaceId","known_lakehouses":[{"id":"$lakehouseId"}]}}}
"@
                    # Insert after the prologue line
                    $nbContent = $nbContent -replace '(# Fabric notebook source)', "`$1`n$lhConfig"
                }

                # Normalize line endings (Fabric API prefers \n)
                $nbContent = $nbContent -replace "`r`n", "`n"

                $b64 = ConvertTo-Base64FromString $nbContent

                $definition = @{
                    definition = @{
                        parts = @(
                            @{
                                path        = "notebook-content.py"
                                payload     = $b64
                                payloadType = "InlineBase64"
                            }
                        )
                    }
                } | ConvertTo-Json -Depth 5

                $result = Update-FabricItemDefinition `
                    -ItemId $nbId `
                    -WsId $WorkspaceId `
                    -DefinitionJson $definition `
                    -Token $token

                if ($result) {
                    Write-Success "Notebook definition uploaded: $($ipynb.Name)"
                }
                else {
                    Write-Warn "Notebook definition upload may have failed: $($ipynb.Name)"
                }
            }
        }
    }
}
else {
    if ($SkipNotebook) { Write-Info "Notebook deployment skipped - use -SkipNotebook to control" }
    else { Write-Info "No Notebook directories found" }
}

# ============================================================================
# Step 3 - Deploy Dataflows
# ============================================================================
if (-not $SkipDataflow -and $dataflowDirs.Count -gt 0) {
    Write-Step "3" "Deploying Dataflow(s)"

    $token = Get-FabricToken

    foreach ($dfDir in $dataflowDirs) {
        $dfDisplayName = $dfDir.Name -replace '\.Dataflow$', ''

        try {
        Measure-Step -Name "Dataflow: $dfDisplayName" -Timings $timings -Block {
            $dfId = New-OrGetFabricItem `
                -DisplayName $dfDisplayName `
                -Type "Dataflow" `
                -Description "Dataflow for $dfDisplayName" `
                -WsId $WorkspaceId `
                -Token $token

            if (-not $dfId) {
                Write-Warn "Could not create Dataflow '$dfDisplayName'"
                return
            }

            $deployedItems["Dataflow:$dfDisplayName"] = $dfId

            # Get definition parts
            $parts = Get-DataflowDefinitionParts -DataflowDir $dfDir.FullName

            if ($parts.Count -gt 0) {
                # Build JSON manually to avoid single-element array unwrapping
                $partsJsonArr = @()
                foreach ($p in $parts) {
                    $partsJsonArr += '{"path":"' + $p.path + '","payload":"' + $p.payload + '","payloadType":"' + $p.payloadType + '"}'
                }
                $definition = '{"definition":{"parts":[' + ($partsJsonArr -join ',') + ']}}'

                # Dataflows require the dedicated /dataflows/ endpoint
                $result = Update-DataflowDefinition `
                    -DataflowId $dfId `
                    -WsId $WorkspaceId `
                    -DefinitionJson $definition `
                    -Token $token

                if ($result) { Write-Success "Dataflow definition uploaded" }
                else { Write-Warn "Dataflow definition upload may have failed" }
            }
            else {
                Write-Info "No mashup content found for dataflow '$dfDisplayName'"
            }
        }
        } catch {
            Write-Warn "Dataflow step failed: $($_.Exception.Message) - continuing..."
        }
    }
}
else {
    if ($SkipDataflow) { Write-Info "Dataflow deployment skipped - use -SkipDataflow to control" }
    else { Write-Info "No Dataflow directories found" }
}

# ============================================================================
# Step 4 - Deploy Semantic Models
# ============================================================================
if (-not $SkipSemanticModel -and $semanticModelDirs.Count -gt 0) {
    Write-Step "4" "Deploying Semantic Model(s)"

    $token = Get-FabricToken

    foreach ($smDir in $semanticModelDirs) {
        $smDisplayName = $smDir.Name -replace '\.SemanticModel$', ''

        Measure-Step -Name "SemanticModel: $smDisplayName" -Timings $timings -Block {
            # Build TMDL definition parts with token replacement FIRST
            $ep = if ($sqlEndpoint) { $sqlEndpoint } else { "" }
            $ln = if ($lakehouseName) { $lakehouseName } else { "" }

            $parts = Get-TmdlDefinitionParts `
                -SemanticModelDir $smDir.FullName `
                -SqlEndpoint $ep `
                -LakehouseName $ln

            if ($parts.Count -eq 0) {
                Write-Warn "No TMDL definition files found in $($smDir.FullName)"
                return
            }

            Write-Info "Prepared $($parts.Count) TMDL definition files"

            # Check if it already exists
            $smId = $null
            try {
                $existing = (Invoke-RestMethod `
                    -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items?type=SemanticModel" `
                    -Headers @{ Authorization = "Bearer $token" }).value
                $found = $existing |
                    Where-Object { $_.displayName -eq $smDisplayName } |
                    Select-Object -First 1
                if ($found) {
                    $smId = $found.id
                    Write-Info "'$smDisplayName' already exists - updating definition ($smId)"
                }
            } catch {}

            if ($smId) {
                # Update existing semantic model definition
                $defJson = @{
                    definition = @{ parts = $parts }
                } | ConvertTo-Json -Depth 10

                $result = Update-FabricItemDefinition `
                    -ItemId $smId `
                    -WsId $WorkspaceId `
                    -DefinitionJson $defJson `
                    -Token $token

                if ($result) { Write-Success "Semantic model definition updated - $($parts.Count) files" }
                else { Write-Warn "Semantic model definition update may have failed" }
            }
            else {
                # Create with definition included (SemanticModel requires it)
                $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
                $bodyHash = @{
                    displayName = $smDisplayName
                    type        = "SemanticModel"
                    description = "Semantic model for $smDisplayName"
                    definition  = @{ parts = $parts }
                }
                $bodyJson = $bodyHash | ConvertTo-Json -Depth 10

                try {
                    $resp = Invoke-WebRequest -Method Post `
                        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" `
                        -Headers $headers -Body $bodyJson -UseBasicParsing

                    if ($resp.StatusCode -eq 201) {
                        $smId = ($resp.Content | ConvertFrom-Json).id
                        Write-Success "Created SemanticModel '$smDisplayName': $smId"
                    }
                    elseif ($resp.StatusCode -eq 202) {
                        $opUrl = $resp.Headers["Location"]
                        if ($opUrl) {
                            Write-Info "Waiting for SemanticModel creation LRO..."
                            $opResult = Wait-FabricOperation -OperationUrl $opUrl -Token $token
                        }
                        Start-Sleep -Seconds 3
                        $items = (Invoke-RestMethod `
                            -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items?type=SemanticModel" `
                            -Headers @{ Authorization = "Bearer $token" }).value
                        $found = $items |
                            Where-Object { $_.displayName -eq $smDisplayName } |
                            Select-Object -First 1
                        if ($found) { $smId = $found.id; Write-Success "Created SemanticModel: $smId" }
                    }
                }
                catch {
                    $errBody = ""
                    try {
                        $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                        $errBody = $sr.ReadToEnd(); $sr.Close()
                    } catch {}
                    Write-Err "Failed to create SemanticModel: $errBody"
                }
            }

            if ($smId) {
                $deployedItems["SemanticModel:$smDisplayName"] = $smId
            }
        }
    }
}
else {
    if ($SkipSemanticModel) { Write-Info "Semantic model deployment skipped - use -SkipSemanticModel to control" }
    else { Write-Info "No Semantic Model directories found" }
}

# ============================================================================
# Step 5 - Deploy Reports
# ============================================================================
if (-not $SkipReport -and $reportDirs.Count -gt 0) {
    Write-Step "5" "Deploying Report(s)"

    $token = Get-FabricToken

    foreach ($rptDir in $reportDirs) {
        $rptDisplayName = $rptDir.Name -replace '\.Report$', ''

        Measure-Step -Name "Report: $rptDisplayName" -Timings $timings -Block {
            # Find the semantic model ID for this report
            $smId = $null
            $smKey = "SemanticModel:$rptDisplayName"
            if ($deployedItems.ContainsKey($smKey)) {
                $smId = $deployedItems[$smKey]
            }
            else {
                # Try to find any deployed semantic model
                $smEntry = $deployedItems.GetEnumerator() |
                    Where-Object { $_.Key -like "SemanticModel:*" } |
                    Select-Object -First 1
                if ($smEntry) { $smId = $smEntry.Value }
            }

            if (-not $smId) {
                Write-Warn "No Semantic Model found for report binding - deploying without byConnection rewrite"
            }

            $parts = Get-PbirDefinitionParts `
                -ReportDir $rptDir.FullName `
                -SemanticModelId $smId

            if ($parts.Count -eq 0) {
                Write-Warn "No report definition files found in $($rptDir.FullName)"
                return
            }

            # Check if report already exists
            $rptId = $null
            try {
                $existing = (Invoke-RestMethod `
                    -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items?type=Report" `
                    -Headers @{ Authorization = "Bearer $token" }).value
                $found = $existing |
                    Where-Object { $_.displayName -eq $rptDisplayName } |
                    Select-Object -First 1
                if ($found) {
                    $rptId = $found.id
                    Write-Info "'$rptDisplayName' already exists - updating definition ($rptId)"
                }
            } catch {}

            if ($rptId) {
                # Update existing report definition
                Write-Info "Uploading $($parts.Count) report definition files..."
                $defJson = @{
                    definition = @{ parts = $parts }
                } | ConvertTo-Json -Depth 10

                $result = Update-FabricItemDefinition `
                    -ItemId $rptId `
                    -WsId $WorkspaceId `
                    -DefinitionJson $defJson `
                    -Token $token

                if ($result) { Write-Success "Report definition uploaded - $($parts.Count) files" }
                else { Write-Warn "Report definition upload may have failed" }
            }
            else {
                # Create report with definition included (Reports require definition at creation)
                Write-Info "Creating report with $($parts.Count) definition files..."
                $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
                $bodyHash = @{
                    displayName = $rptDisplayName
                    type        = "Report"
                    description = "Report for $rptDisplayName"
                    definition  = @{ parts = $parts }
                }
                $bodyJson = $bodyHash | ConvertTo-Json -Depth 10

                try {
                    $resp = Invoke-WebRequest -Method Post `
                        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" `
                        -Headers $headers -Body $bodyJson -UseBasicParsing

                    if ($resp.StatusCode -eq 201) {
                        $rptId = ($resp.Content | ConvertFrom-Json).id
                        Write-Success "Created Report '$rptDisplayName': $rptId"
                    }
                    elseif ($resp.StatusCode -eq 202) {
                        $opUrl = $resp.Headers["Location"]
                        if ($opUrl) {
                            Write-Info "Waiting for Report creation LRO..."
                            for ($p = 1; $p -le 24; $p++) {
                                Start-Sleep -Seconds 5
                                $poll = Invoke-RestMethod -Uri $opUrl `
                                    -Headers @{ Authorization = "Bearer $token" }
                                Write-Info ("  Report LRO: {0} ({1}s)" -f $poll.status, ($p*5))
                                if ($poll.status -eq "Succeeded") { break }
                                if ($poll.status -eq "Failed") {
                                    Write-Warn "Report creation LRO failed"
                                    $errorDetail = $poll | ConvertTo-Json -Depth 5 -Compress
                                    Write-Warn "LRO detail: $errorDetail"
                                    break
                                }
                            }
                        }
                        Start-Sleep -Seconds 3
                        $items = (Invoke-RestMethod `
                            -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items?type=Report" `
                            -Headers @{ Authorization = "Bearer $token" }).value
                        $found = $items |
                            Where-Object { $_.displayName -eq $rptDisplayName } |
                            Select-Object -First 1
                        if ($found) {
                            $rptId = $found.id
                            Write-Success "Created Report: $rptId"
                        }
                    }
                }
                catch {
                    $errBody = ""
                    try {
                        $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                        $errBody = $sr.ReadToEnd(); $sr.Close()
                    } catch {}
                    Write-Warn "Failed to create Report: $($_.Exception.Message) $errBody"
                }
            }

            if ($rptId) {
                $deployedItems["Report:$rptDisplayName"] = $rptId
            }
        }
    }
}
else {
    if ($SkipReport) { Write-Info "Report deployment skipped - use -SkipReport to control" }
    else { Write-Info "No Report directories found" }
}

# ============================================================================
# Step 6 - Deploy Pipelines
# ============================================================================
if (-not $SkipPipeline -and $pipelineDirs.Count -gt 0) {
    Write-Step "6" "Deploying Pipeline(s)"

    $token = Get-FabricToken

    foreach ($plDir in $pipelineDirs) {
        $plDisplayName = $plDir.Name -replace '\.Pipeline$', ''

        Measure-Step -Name "Pipeline: $plDisplayName" -Timings $timings -Block {
            $plId = New-OrGetFabricItem `
                -DisplayName $plDisplayName `
                -Type "DataPipeline" `
                -Description "Pipeline for $plDisplayName" `
                -WsId $WorkspaceId `
                -Token $token

            if (-not $plId) {
                Write-Warn "Could not create Pipeline '$plDisplayName'"
                return
            }

            $deployedItems["Pipeline:$plDisplayName"] = $plId

            $parts = Get-PipelineDefinitionParts -PipelineDir $plDir.FullName -DeployedItems $deployedItems -WorkspaceId $WorkspaceId

            if ($parts.Count -gt 0) {
                # Build JSON string manually to avoid PowerShell single-element
                # array serialization issue (object vs array)
                $partsJsonArr = @()
                foreach ($p in $parts) {
                    $partsJsonArr += '{"path":"' + $p.path + '","payload":"' + $p.payload + '","payloadType":"' + $p.payloadType + '"}'
                }
                $definition = '{"definition":{"parts":[' + ($partsJsonArr -join ',') + ']}}'

                # Pipelines require the dedicated /dataPipelines/ endpoint
                $result = Update-PipelineDefinition `
                    -PipelineId $plId `
                    -WsId $WorkspaceId `
                    -DefinitionJson $definition `
                    -Token $token

                if ($result) { Write-Success "Pipeline definition uploaded" }
                else { Write-Warn "Pipeline definition upload may have failed" }
            }
            else {
                Write-Info "No pipeline definition found for '$plDisplayName'"
            }
        }
    }
}
else {
    if ($SkipPipeline) { Write-Info "Pipeline deployment skipped - use -SkipPipeline to control" }
    else { Write-Info "No Pipeline directories found" }
}

# ============================================================================
# Step 6b - Organize Workspace Folders
# ============================================================================
if ($deployedItems.Count -gt 0) {
    Write-Step "6b" "Organizing items into workspace folders"

    $token = Get-FabricToken

    Measure-Step -Name "Workspace Folders" -Timings $timings -Block {
        # Folder mapping: FolderName -> array of item type prefixes
        $folderMapping = [ordered]@{
            "01 - Data Storage"       = @("Lakehouse")
            "02 - Data Ingestion"     = @("Dataflow")
            "03 - Data Transformation" = @("Notebook")
            "04 - Orchestration"      = @("Pipeline")
            "05 - Analytics"          = @("SemanticModel", "Report")
        }

        foreach ($folderEntry in $folderMapping.GetEnumerator()) {
            $folderName = $folderEntry.Key
            $types      = $folderEntry.Value

            # Find deployed items of these types
            $itemsForFolder = $deployedItems.GetEnumerator() |
                Where-Object {
                    $itemType = $_.Key.Split(":")[0]
                    $types -contains $itemType
                }

            if (-not $itemsForFolder) { continue }

            $folderId = New-OrGetWorkspaceFolder `
                -FolderName $folderName `
                -WsId $WorkspaceId `
                -Token $token

            if ($folderId) {
                Write-Success "Folder '$folderName' ready ($folderId)"
                foreach ($item in $itemsForFolder) {
                    $itemName = $item.Key.Split(":")[1]
                    Move-ItemToFolder `
                        -ItemId $item.Value `
                        -ItemName $itemName `
                        -FolderId $folderId `
                        -FolderName $folderName `
                        -WsId $WorkspaceId `
                        -Token $token
                }
            }
            else {
                Write-Warn "Could not create folder '$folderName'"
            }
        }

        Write-Success "Workspace folders organized"
    }
}

# ============================================================================
# Step 7 - Run Notebooks (optional)
# ============================================================================
if ($RunNotebooks -and $notebookDirs.Count -gt 0) {
    Write-Step "7" "Running Notebooks"

    $token = Get-FabricToken

    foreach ($nbDir in $notebookDirs) {
        $nbDisplayName = $nbDir.Name -replace '\.Notebook$', ''
        $nbId = $deployedItems["Notebook:$nbDisplayName"]

        if ($nbId) {
            Measure-Step -Name "Run: $nbDisplayName" -Timings $timings -Block {
                $ok = Run-FabricNotebook `
                    -NotebookId $nbId `
                    -NotebookName $nbDisplayName `
                    -WsId $WorkspaceId `
                    -Token $token

                if (-not $ok) {
                    Write-Warn "Notebook run for '$nbDisplayName' did not complete successfully"
                }
            }
        }
    }
}

# ============================================================================
# Step 8 - Run Pipeline (optional)
# ============================================================================
if ($RunPipeline -and $pipelineDirs.Count -gt 0) {
    Write-Step "8" "Running Pipeline"

    $token = Get-FabricToken

    foreach ($plDir in $pipelineDirs) {
        $plDisplayName = $plDir.Name -replace '\.Pipeline$', ''
        $plId = $deployedItems["Pipeline:$plDisplayName"]

        if ($plId) {
            Measure-Step -Name "Run: $plDisplayName" -Timings $timings -Block {
                $ok = Run-FabricPipeline `
                    -PipelineId $plId `
                    -PipelineName $plDisplayName `
                    -WsId $WorkspaceId `
                    -Token $token

                if (-not $ok) {
                    Write-Warn "Pipeline run for '$plDisplayName' did not complete successfully"
                }
            }
        }
    }
}

# ============================================================================
# SUMMARY
# ============================================================================
Show-TimingSummary $timings

Write-Banner "DEPLOYMENT COMPLETE" Green
Write-Host ""
Write-Host "  Workspace   : $($ws.displayName)" -ForegroundColor White
Write-Host "  Workspace ID: $WorkspaceId" -ForegroundColor Gray
Write-Host "  Portal      : https://app.fabric.microsoft.com/groups/$WorkspaceId" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Deployed items:" -ForegroundColor White
foreach ($entry in $deployedItems.GetEnumerator()) {
    $typeName = $entry.Key.Split(":")[0]
    $dispName = $entry.Key.Split(":")[1]
    Write-Host ("    {0,-15} {1,-30} {2}" -f $typeName, $dispName, $entry.Value) -ForegroundColor Gray
}
Write-Host ""

if ($sqlEndpoint) {
    Write-Host "  SQL Endpoint: $sqlEndpoint" -ForegroundColor Cyan
    Write-Host ""
}

if (-not $RunNotebooks -and $notebookDirs.Count -gt 0) {
    Write-Host "  Tip: Run notebooks with -RunNotebooks flag" -ForegroundColor Yellow
}
if (-not $RunPipeline -and $pipelineDirs.Count -gt 0) {
    Write-Host "  Tip: Run the pipeline with -RunPipeline flag" -ForegroundColor Yellow
}
Write-Host ""
