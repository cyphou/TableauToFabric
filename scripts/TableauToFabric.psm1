<#
.SYNOPSIS
    Shared helper functions for TableauToFabric Fabric deployment scripts.

.DESCRIPTION
    This module centralises common utilities (authentication, Fabric API,
    OneLake, definition encoding, logging) used across all deploy scripts.

    Reuses proven patterns from the HorizonBooks Fabric deployment project.

    Usage:
        Import-Module (Join-Path $PSScriptRoot 'TableauToFabric.psm1') -Force
#>

# ── Module-scoped defaults ──────────────────────────────────────────────
$script:FabricApiBase = "https://api.fabric.microsoft.com/v1"
$script:OneLakeBase   = "https://onelake.dfs.fabric.microsoft.com"

# ============================================================================
# DISPLAY HELPERS
# ============================================================================

function Write-Banner {
    param([string]$Title, [ConsoleColor]$Color = "Yellow")
    Write-Host ""
    Write-Host ("=" * 70) -ForegroundColor $Color
    Write-Host "  $Title" -ForegroundColor $Color
    Write-Host ("=" * 70) -ForegroundColor $Color
}

function Write-Step {
    param([string]$StepNum, [string]$Message)
    Write-Host ""
    Write-Host "  [$StepNum] $Message" -ForegroundColor Cyan
    Write-Host ("  " + "-" * 60) -ForegroundColor DarkGray
}

function Write-Info    { param([string]$M) Write-Host "      [INFO] $M" -ForegroundColor Gray }
function Write-Success { param([string]$M) Write-Host "      [ OK ] $M" -ForegroundColor Green }
function Write-Warn    { param([string]$M) Write-Host "      [WARN] $M" -ForegroundColor Yellow }
function Write-Err     { param([string]$M) Write-Host "      [FAIL] $M" -ForegroundColor Red }

function Measure-Step {
    <#
    .SYNOPSIS
        Times a script block and records the result in a caller-provided list.
    #>
    param(
        [string]$Name,
        [scriptblock]$Block,
        [System.Collections.Generic.List[PSCustomObject]]$Timings
    )
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        & $Block
        $sw.Stop()
        $null = $Timings.Add([PSCustomObject]@{
            Step     = $Name
            Duration = $sw.Elapsed
            Status   = "OK"
        })
    }
    catch {
        $sw.Stop()
        $null = $Timings.Add([PSCustomObject]@{
            Step     = $Name
            Duration = $sw.Elapsed
            Status   = "FAILED"
        })
        throw
    }
}

function Show-TimingSummary {
    <#
    .SYNOPSIS
        Displays a table of step timings.
    #>
    param([System.Collections.Generic.List[PSCustomObject]]$Timings)

    Write-Host ""
    Write-Banner "TIMING SUMMARY" Green
    Write-Host ""
    Write-Host ("  {0,-35} {1,12} {2,8}" -f "Step", "Duration", "Status") -ForegroundColor White
    Write-Host ("  " + "-" * 57) -ForegroundColor DarkGray
    $total = [TimeSpan]::Zero
    foreach ($t in $Timings) {
        $dur = "{0:mm\:ss\.fff}" -f $t.Duration
        $color = if ($t.Status -eq "OK") { "Green" } else { "Red" }
        Write-Host ("  {0,-35} {1,12} " -f $t.Step, $dur) -NoNewline
        Write-Host ("{0,8}" -f $t.Status) -ForegroundColor $color
        $total = $total.Add($t.Duration)
    }
    Write-Host ("  " + "-" * 57) -ForegroundColor DarkGray
    Write-Host ("  {0,-35} {1,12}" -f "TOTAL", ("{0:mm\:ss\.fff}" -f $total)) -ForegroundColor Yellow
    Write-Host ""
}

# ============================================================================
# TOKEN HELPERS
# ============================================================================

function Get-FabricToken {
    <#
    .SYNOPSIS
        Returns a bearer token for the Fabric REST API.
        Requires prior Connect-AzAccount.
    #>
    try {
        $token = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Fabric API token. Run 'Connect-AzAccount' first."
        throw
    }
}

function Get-StorageToken {
    <#
    .SYNOPSIS
        Returns a bearer token for the OneLake / Azure Storage DFS API.
    #>
    try {
        $token = Get-AzAccessToken -ResourceTypeName Storage
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Storage token. Run 'Connect-AzAccount' first."
        throw
    }
}

# ============================================================================
# FABRIC API HELPERS
# ============================================================================

function Invoke-FabricApi {
    <#
    .SYNOPSIS
        Calls the Fabric REST API with automatic retry, 429 handling,
        and long-running-operation polling.
    #>
    param(
        [string]$Method,
        [string]$Uri,
        [object]$Body        = $null,
        [string]$BodyJson    = $null,
        [string]$Token,
        [int]$MaxRetries     = 10
    )

    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Type"  = "application/json"
    }

    if (-not $BodyJson -and $Body) {
        $BodyJson = $Body | ConvertTo-Json -Depth 10
    }

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $params = @{
                Method          = $Method
                Uri             = $Uri
                Headers         = $headers
                UseBasicParsing = $true
            }
            if ($BodyJson) { $params["Body"] = $BodyJson }

            $webResponse = Invoke-WebRequest @params
            $statusCode  = $webResponse.StatusCode

            # 202 Accepted - Long Running Operation
            if ($statusCode -eq 202) {
                $locationHeader = $webResponse.Headers["Location"]
                $opIdHeader     = $webResponse.Headers["x-ms-operation-id"]
                $operationUrl   = $null
                if ($locationHeader) { $operationUrl = $locationHeader }
                elseif ($opIdHeader) { $operationUrl = "$($script:FabricApiBase)/operations/$opIdHeader" }

                if ($operationUrl) {
                    Write-Info "Waiting for long-running operation..."
                    return Wait-FabricOperation -OperationUrl $operationUrl -Token $Token
                }
                return $null
            }

            if ($webResponse.Content) {
                try   { return $webResponse.Content | ConvertFrom-Json }
                catch { return $webResponse.Content }
            }
            return $null
        }
        catch {
            $ex = $_.Exception
            $statusCode = $null
            $errorBody  = ""
            if ($ex -and $ex.Response) {
                $statusCode = [int]$ex.Response.StatusCode
                try {
                    $sr = New-Object System.IO.StreamReader($ex.Response.GetResponseStream())
                    $errorBody = $sr.ReadToEnd(); $sr.Close()
                } catch {}
            }

            $isRetriable = $errorBody -like "*isRetriable*true*" -or
                           $errorBody -like "*NotAvailableYet*"

            if ($statusCode -eq 429 -or $isRetriable) {
                $retryAfter = if ($isRetriable) { 15 } else { 30 }
                try {
                    $ra = $ex.Response.Headers | Where-Object { $_.Key -eq "Retry-After" } |
                        Select-Object -ExpandProperty Value -First 1
                    if ($ra) { $retryAfter = [int]$ra }
                } catch {}
                $reason = if ($isRetriable) { "Retriable error" } else { "Rate limited (429)" }
                Write-Warn ("$reason - retrying in {0}s (attempt {1}/{2})" -f $retryAfter, $attempt, $MaxRetries)
                Start-Sleep -Seconds $retryAfter
            }
            else {
                if ($errorBody) { throw "Fabric API error (HTTP $statusCode): $errorBody" }
                throw
            }
        }
    }
    throw "Max retries exceeded for $Uri"
}

function Wait-FabricOperation {
    <#
    .SYNOPSIS
        Polls a Fabric long-running operation until it succeeds,
        fails, or times out.
    #>
    param(
        [string]$OperationUrl,
        [string]$Token,
        [int]$TimeoutSeconds      = 600,
        [int]$PollIntervalSeconds = 10
    )

    $headers = @{ "Authorization" = "Bearer $Token" }
    $elapsed = 0

    while ($elapsed -lt $TimeoutSeconds) {
        Start-Sleep -Seconds $PollIntervalSeconds
        $elapsed += $PollIntervalSeconds
        try {
            $status = Invoke-RestMethod -Method Get -Uri $OperationUrl -Headers $headers
            $state  = $status.status
            Write-Info ("  Operation: {0} ({1}s)" -f $state, $elapsed)

            if ($state -eq "Succeeded") { return $status }
            if ($state -eq "Failed") {
                Write-Err "Operation failed: $($status | ConvertTo-Json -Depth 5)"
                throw "Fabric operation failed"
            }
        }
        catch {
            if ($_.Exception.Response -and $_.Exception.Response.StatusCode -eq 429) {
                Write-Warn "Rate limited while polling - waiting 30s..."
                Start-Sleep -Seconds 30
            }
            else { throw }
        }
    }
    throw ("Operation timed out after {0}s" -f $TimeoutSeconds)
}

function New-OrGetFabricItem {
    <#
    .SYNOPSIS
        Returns the ID of an existing Fabric item with the given display-name
        and type, or creates a new one if none exists.  Idempotent.
    #>
    param(
        [string]$DisplayName,
        [string]$Type,
        [string]$Description = "",
        [string]$WsId,
        [string]$Token,
        [hashtable]$CreationPayload = $null
    )

    # ── Look for an existing item first ──────────────────────────────
    try {
        $existing = (Invoke-RestMethod `
            -Uri "$($script:FabricApiBase)/workspaces/$WsId/items?type=$Type" `
            -Headers @{ Authorization = "Bearer $Token" }).value
        $found = $existing |
            Where-Object { $_.displayName -eq $DisplayName } |
            Select-Object -First 1
        if ($found) {
            Write-Info "'$DisplayName' ($Type) already exists - reusing $($found.id)"
            return $found.id
        }
    }
    catch { Write-Warn "Could not list existing ${Type} items: $($_.Exception.Message)" }

    # ── Create new item ──────────────────────────────────────────────
    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
    $bodyHash = @{ displayName = $DisplayName; type = $Type }
    if ($Description) { $bodyHash["description"] = $Description }
    if ($CreationPayload) { $bodyHash["creationPayload"] = $CreationPayload }
    $body = $bodyHash | ConvertTo-Json -Depth 5

    try {
        $resp = Invoke-WebRequest -Method Post `
            -Uri "$($script:FabricApiBase)/workspaces/$WsId/items" `
            -Headers $headers -Body $body -UseBasicParsing

        if ($resp.StatusCode -eq 201) {
            $newId = ($resp.Content | ConvertFrom-Json).id
            Write-Info "Created $Type '$DisplayName': $newId"
            return $newId
        }
        elseif ($resp.StatusCode -eq 202) {
            $opUrl = $resp.Headers["Location"]
            if ($opUrl) {
                for ($p = 1; $p -le 24; $p++) {
                    Start-Sleep -Seconds 5
                    $poll = Invoke-RestMethod -Uri $opUrl `
                        -Headers @{ Authorization = "Bearer $Token" }
                    Write-Info ("  LRO: {0} ({1}s)" -f $poll.status, ($p*5))
                    if ($poll.status -eq "Succeeded") { break }
                    if ($poll.status -eq "Failed") { Write-Warn "LRO failed"; break }
                }
            }
            Start-Sleep -Seconds 3
            $items = (Invoke-RestMethod `
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/items?type=$Type" `
                -Headers @{ Authorization = "Bearer $Token" }).value
            $found = $items |
                Where-Object { $_.displayName -eq $DisplayName } |
                Select-Object -First 1
            if ($found) { return $found.id }
        }
    }
    catch {
        $errBody = ""
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $errBody = $sr.ReadToEnd(); $sr.Close()
        } catch {}
        $errMsg = "$($_.Exception.Message) $errBody"

        if ($errMsg -like "*ItemDisplayNameAlreadyInUse*" -or
            $errMsg -like "*already in use*") {
            Write-Info "'$DisplayName' already exists - reusing"
            $items = (Invoke-RestMethod `
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/items?type=$Type" `
                -Headers @{ Authorization = "Bearer $Token" }).value
            $found = $items |
                Where-Object { $_.displayName -eq $DisplayName } |
                Select-Object -First 1
            if ($found) { return $found.id }
        }
        else { throw "Failed to create $Type '${DisplayName}': $errMsg" }
    }
    return $null
}

# ============================================================================
# ONELAKE HELPERS
# ============================================================================

function Upload-FileToOneLake {
    <#
    .SYNOPSIS
        Uploads a local file to OneLake via the DFS API (create → append → flush).
    #>
    param(
        [string]$LocalFilePath,
        [string]$OneLakePath,
        [string]$Token
    )

    $fileBytes = [System.IO.File]::ReadAllBytes($LocalFilePath)
    $fileName  = [System.IO.Path]::GetFileName($LocalFilePath)

    # Create file
    Invoke-RestMethod -Method Put `
        -Uri "${OneLakePath}/${fileName}?resource=file" `
        -Headers @{ "Authorization" = "Bearer $Token"; "Content-Length" = "0" } | Out-Null

    # Append data
    Invoke-RestMethod -Method Patch `
        -Uri "${OneLakePath}/${fileName}?action=append&position=0" `
        -Headers @{ "Authorization" = "Bearer $Token"
                    "Content-Type" = "application/octet-stream"
                    "Content-Length" = $fileBytes.Length.ToString() } `
        -Body $fileBytes | Out-Null

    # Flush
    Invoke-RestMethod -Method Patch `
        -Uri "${OneLakePath}/${fileName}?action=flush&position=$($fileBytes.Length)" `
        -Headers @{ "Authorization" = "Bearer $Token"; "Content-Length" = "0" } | Out-Null
}

# ============================================================================
# WORKSPACE FOLDER HELPERS
# ============================================================================

function New-OrGetWorkspaceFolder {
    <#
    .SYNOPSIS
        Creates a workspace folder (or returns the existing one).
        Idempotent - if a folder with the same name already exists, its ID is returned.
    #>
    param(
        [string]$FolderName,
        [string]$WsId,
        [string]$Token,
        [string]$ParentFolderId = $null
    )

    $body = @{ displayName = $FolderName }
    if ($ParentFolderId) { $body["parentFolderId"] = $ParentFolderId }

    try {
        $resp = Invoke-WebRequest -Method Post `
            -Uri "$($script:FabricApiBase)/workspaces/$WsId/folders" `
            -Headers @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" } `
            -Body ($body | ConvertTo-Json -Depth 3) -UseBasicParsing
        if ($resp.StatusCode -in @(200, 201)) {
            return ($resp.Content | ConvertFrom-Json).id
        }
    }
    catch {
        $errBody = ""
        try {
            if ($_.Exception.Response) {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $errBody = $sr.ReadToEnd(); $sr.Close()
            }
        } catch {}
        if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $errBody = $_.ErrorDetails.Message }

        if ($errBody -like "*already*" -or $errBody -like "*AlreadyExists*" -or $errBody -like "*DisplayName*") {
            try {
                $folders = (Invoke-RestMethod -Uri "$($script:FabricApiBase)/workspaces/$WsId/folders" `
                    -Headers @{ Authorization = "Bearer $Token" }).value
                $f = $folders | Where-Object { $_.displayName -eq $FolderName } | Select-Object -First 1
                if ($f) { return $f.id }
            } catch {}
        }
        else { Write-Warn "Folder '$FolderName' error: $($_.Exception.Message) $errBody" }
    }
    return $null
}

function Move-ItemToFolder {
    <#
    .SYNOPSIS
        Moves a Fabric item into a workspace folder.
        Includes retry logic with exponential backoff for 429 throttling.
    #>
    param(
        [string]$ItemId,
        [string]$ItemName,
        [string]$FolderId,
        [string]$FolderName,
        [string]$WsId,
        [string]$Token
    )

    $maxRetries = 5
    $retryCount = 0
    $delay = 5

    while ($true) {
        try {
            Invoke-RestMethod -Method Post `
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/items/$ItemId/move" `
                -Headers @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" } `
                -Body (@{ targetFolderId = $FolderId } | ConvertTo-Json -Depth 3) | Out-Null
            Write-Info "  Moved $ItemName -> $FolderName/"
            Start-Sleep -Seconds 2   # proactive throttle spacing
            return
        }
        catch {
            $statusCode = $null
            if ($_.Exception.Response) {
                $statusCode = [int]$_.Exception.Response.StatusCode
            }
            if ($statusCode -eq 429 -and $retryCount -lt $maxRetries) {
                $retryCount++
                $retryAfter = $delay
                if ($_.Exception.Response.Headers) {
                    $raHeader = $_.Exception.Response.Headers |
                        Where-Object { $_.Key -eq 'Retry-After' } |
                        Select-Object -ExpandProperty Value -First 1
                    if ($raHeader) {
                        $parsedRa = 0
                        if ([int]::TryParse($raHeader, [ref]$parsedRa) -and $parsedRa -gt 0) {
                            $retryAfter = $parsedRa
                        }
                    }
                }
                Write-Info "  Throttled (429) moving $ItemName - retrying in ${retryAfter}s (attempt $retryCount/$maxRetries)"
                Start-Sleep -Seconds $retryAfter
                $delay = [Math]::Min($delay * 2, 60)
                $Token = Get-FabricToken
            }
            else {
                Write-Warn "  Could not move $ItemName to $FolderName/ : $($_.Exception.Message)"
                return
            }
        }
    }
}

# ============================================================================
# DEFINITION HELPERS
# ============================================================================

function Update-FabricItemDefinition {
    <#
    .SYNOPSIS
        Updates the definition of a Fabric item (notebook, semantic model,
        report, etc.) with retry logic and LRO handling.
    #>
    param(
        [string]$ItemId,
        [string]$WsId,
        [string]$DefinitionJson,
        [string]$Token
    )

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        if ($attempt -gt 1) {
            Write-Info "Definition update retry $attempt/3 - waiting 10s..."
            Start-Sleep -Seconds 10
            $Token   = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $resp = Invoke-WebRequest -Method Post `
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/items/$ItemId/updateDefinition" `
                -Headers $headers -Body $DefinitionJson -UseBasicParsing

            if ($resp.StatusCode -eq 200) { return $true }
            if ($resp.StatusCode -eq 202) {
                $opUrl = $resp.Headers["Location"]
                if ($opUrl) {
                    for ($p = 1; $p -le 24; $p++) {
                        Start-Sleep -Seconds 5
                        $poll = Invoke-RestMethod -Uri $opUrl `
                            -Headers @{ Authorization = "Bearer $Token" }
                        Write-Info ("  Definition LRO: {0} ({1}s)" -f $poll.status, ($p*5))
                        if ($poll.status -eq "Succeeded") { return $true }
                        if ($poll.status -eq "Failed") {
                            $errorDetail = $poll | ConvertTo-Json -Depth 5 -Compress
                            Write-Warn "Definition LRO failed: $errorDetail"
                            return $false
                        }
                    }
                }
            }
        }
        catch {
            Write-Warn "Definition update error (attempt $attempt): $($_.Exception.Message)"
        }
    }
    return $false
}

function Update-PipelineDefinition {
    <#
    .SYNOPSIS
        Updates a Data Pipeline definition using the dedicated
        /dataPipelines/{id}/updateDefinition endpoint (NOT the generic /items/ one).
    #>
    param(
        [string]$PipelineId,
        [string]$WsId,
        [string]$DefinitionJson,
        [string]$Token
    )

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        if ($attempt -gt 1) {
            Write-Info "Pipeline definition retry $attempt/3 - waiting 10s..."
            Start-Sleep -Seconds 10
            $Token   = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $resp = Invoke-WebRequest -Method Post `
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/dataPipelines/$PipelineId/updateDefinition" `
                -Headers $headers -Body $DefinitionJson -UseBasicParsing

            if ($resp.StatusCode -eq 200) { return $true }
            if ($resp.StatusCode -eq 202) {
                $opUrl = $resp.Headers["Location"]
                if ($opUrl) {
                    for ($p = 1; $p -le 24; $p++) {
                        Start-Sleep -Seconds 5
                        $poll = Invoke-RestMethod -Uri $opUrl `
                            -Headers @{ Authorization = "Bearer $Token" }
                        Write-Info ("  Pipeline LRO: {0} ({1}s)" -f $poll.status, ($p*5))
                        if ($poll.status -eq "Succeeded") { return $true }
                        if ($poll.status -eq "Failed") {
                            Write-Warn "Pipeline definition LRO failed"
                            return $false
                        }
                    }
                }
            }
        }
        catch {
            $errBody = ""
            try {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $errBody = $sr.ReadToEnd(); $sr.Close()
            } catch {}
            Write-Warn "Pipeline definition error (attempt $attempt): $errBody"
        }
    }
    return $false
}

function Update-DataflowDefinition {
    <#
    .SYNOPSIS
        Updates a Dataflow Gen2 definition using the dedicated
        /dataflows/{id}/updateDefinition endpoint (NOT the generic /items/ one).
    #>
    param(
        [string]$DataflowId,
        [string]$WsId,
        [string]$DefinitionJson,
        [string]$Token
    )

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        if ($attempt -gt 1) {
            Write-Info "Dataflow definition retry $attempt/3 - waiting 10s..."
            Start-Sleep -Seconds 10
            $Token   = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $resp = Invoke-WebRequest -Method Post `
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/dataflows/$DataflowId/updateDefinition" `
                -Headers $headers -Body $DefinitionJson -UseBasicParsing

            if ($resp.StatusCode -eq 200) { return $true }
            if ($resp.StatusCode -eq 202) {
                $opUrl = $resp.Headers["Location"]
                if ($opUrl) {
                    for ($p = 1; $p -le 24; $p++) {
                        Start-Sleep -Seconds 5
                        $poll = Invoke-RestMethod -Uri $opUrl `
                            -Headers @{ Authorization = "Bearer $Token" }
                        Write-Info ("  Dataflow LRO: {0} ({1}s)" -f $poll.status, ($p*5))
                        if ($poll.status -eq "Succeeded") { return $true }
                        if ($poll.status -eq "Failed") {
                            Write-Warn "Dataflow definition LRO failed"
                            return $false
                        }
                    }
                }
            }
        }
        catch {
            $errBody = ""
            try {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $errBody = $sr.ReadToEnd(); $sr.Close()
            } catch {}
            Write-Warn "Dataflow definition error (attempt $attempt): $errBody"
        }
    }
    return $false
}

function ConvertTo-Base64 {
    <#
    .SYNOPSIS
        Reads a file and returns its UTF-8 content as a Base64 string.
        Strips UTF-8 BOM (EF BB BF) if present.
    #>
    param([string]$FilePath)

    $bytes = [System.IO.File]::ReadAllBytes($FilePath)

    # Strip UTF-8 BOM
    if ($bytes.Length -ge 3 -and
        $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
        $bytes = $bytes[3..($bytes.Length - 1)]
    }

    return [Convert]::ToBase64String($bytes)
}

function ConvertTo-Base64FromString {
    <#
    .SYNOPSIS
        Converts a UTF-8 string to a Base64 string.
    #>
    param([string]$Text)
    return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text))
}

# ============================================================================
# NOTEBOOK HELPERS
# ============================================================================

function ConvertTo-FabricPyNotebook {
    <#
    .SYNOPSIS
        Converts Jupyter notebook JSON content to the Fabric .py notebook
        format required by the updateDefinition API.

    .DESCRIPTION
        Fabric notebooks uploaded via the REST API must use the .py format
        with a mandatory prologue: "# Fabric notebook source"

        Code cells are delimited by: # %%
        Markdown cells are delimited by: # %% [markdown]
        (markdown content is prefixed with # on each line)
    #>
    param([string]$IpynbJson)

    $trimmed = $IpynbJson.TrimStart()

    # If it doesn't look like JSON, it might already be .py format
    if (-not $trimmed.StartsWith("{")) {
        # Already .py — just ensure it has the prologue
        if (-not $trimmed.StartsWith("# Fabric notebook source")) {
            return "# Fabric notebook source`n$IpynbJson"
        }
        return $IpynbJson
    }

    # Parse the ipynb JSON
    $nb = $IpynbJson | ConvertFrom-Json
    $lines = @("# Fabric notebook source")

    foreach ($cell in $nb.cells) {
        $cellType = $cell.cell_type

        if ($cellType -eq "markdown") {
            $lines += ""
            $lines += "# %% [markdown]"
            foreach ($srcLine in $cell.source) {
                $clean = $srcLine -replace "`r?`n$", ""
                $lines += "# $clean"
            }
        }
        else {
            # code cell
            $lines += ""
            $lines += "# %%"
            foreach ($srcLine in $cell.source) {
                $clean = $srcLine -replace "`r?`n$", ""
                $lines += $clean
            }
        }
    }

    return ($lines -join "`n")
}

function ConvertFrom-VSCodeCellNotebook {
    <#
    .SYNOPSIS
        Detects and converts VSCode.Cell (XML-like) notebook format to
        standard Jupyter .ipynb JSON.  If content is already valid JSON,
        returns it unchanged.
    #>
    param([string]$Content)

    $trimmed = $Content.TrimStart()

    # Already valid JSON?
    if ($trimmed.StartsWith("{")) {
        return $Content
    }

    # Parse <VSCode.Cell ...> blocks
    $cells = @()
    $pattern = '(?s)<VSCode\.Cell\s+id="[^"]*"\s+language="(\w+)">\s*\n(.*?)\n</VSCode\.Cell>'
    $matches = [regex]::Matches($Content, $pattern)

    foreach ($m in $matches) {
        $lang   = $m.Groups[1].Value
        $source = $m.Groups[2].Value

        # Split source into lines for proper .ipynb format
        $sourceLines = @()
        foreach ($line in ($source -split "`n")) {
            $sourceLines += "$line`n"
        }
        # Remove trailing newline from last element
        if ($sourceLines.Count -gt 0) {
            $lastLine = $sourceLines[$sourceLines.Count - 1]
            $sourceLines[$sourceLines.Count - 1] = $lastLine.TrimEnd("`n")
        }

        if ($lang -eq "markdown") {
            $cells += @{
                cell_type = "markdown"
                metadata  = @{ nteract = @{ transient = @{ deleting = $false } } }
                source    = $sourceLines
            }
        }
        else {
            $cells += @{
                cell_type      = "code"
                metadata       = @{
                    jupyter   = @{ outputs_hidden = $false; source_hidden = $false }
                    nteract   = @{ transient = @{ deleting = $false } }
                    microsoft = @{ language = "python" }
                }
                source         = $sourceLines
                outputs        = @()
                execution_count = $null
            }
        }
    }

    if ($cells.Count -eq 0) {
        Write-Warn "No VSCode.Cell blocks found - returning content as-is"
        return $Content
    }

    $notebook = @{
        nbformat       = 4
        nbformat_minor = 5
        metadata       = @{
            kernel_info  = @{ name = "synapse_pyspark" }
            kernelspec   = @{
                name         = "synapse_pyspark"
                display_name = "Synapse PySpark"
                language     = "Python"
            }
            language_info = @{
                name        = "python"
                version     = "3.10"
                mimetype    = "text/x-python"
                file_extension = ".py"
            }
            microsoft    = @{ language = "python" }
        }
        cells          = $cells
    }

    return ($notebook | ConvertTo-Json -Depth 15 -Compress:$false)
}

function Set-NotebookLakehouseBinding {
    <#
    .SYNOPSIS
        Injects Lakehouse metadata into a Jupyter notebook JSON, setting
        the default_lakehouse, default_lakehouse_name, and
        default_lakehouse_workspace_id in the trident metadata block.
    #>
    param(
        [string]$NotebookPath,
        [string]$WorkspaceId,
        [string]$LakehouseId,
        [string]$LakehouseName
    )

    $json = Get-Content -Path $NotebookPath -Raw -Encoding UTF8 |
            ConvertFrom-Json

    # Create or update trident metadata
    if (-not $json.metadata) { $json.metadata = @{} }
    $json.metadata.trident = @{
        lakehouse = @{
            default_lakehouse       = $LakehouseId
            default_lakehouse_name  = $LakehouseName
            default_lakehouse_workspace_id = $WorkspaceId
            known_lakehouses        = @(
                @{
                    id   = $LakehouseId
                    name = $LakehouseName
                }
            )
        }
    }

    return ($json | ConvertTo-Json -Depth 20 -Compress:$false)
}

# ============================================================================
# SEMANTIC MODEL (TMDL) HELPERS
# ============================================================================

function Get-TmdlDefinitionParts {
    <#
    .SYNOPSIS
        Collects all TMDL definition files for a Semantic Model and returns
        them as an array of definition parts (path + base64 payload).
        Applies token replacements to expressions.tmdl.
        Generates definition.pbism manifest and prefixes all TMDL paths
        with 'definition/' as required by the Fabric API.
    #>
    param(
        [string]$SemanticModelDir,
        [string]$SqlEndpoint = "",
        [string]$LakehouseName = ""
    )

    $parts = @()
    $defDir = Join-Path $SemanticModelDir "definition"
    if (-not (Test-Path $defDir)) {
        # Try root level if no definition/ subfolder
        $defDir = $SemanticModelDir
    }

    # Handle double-nested definition/definition/ structure
    $innerDef = Join-Path $defDir "definition"
    if (Test-Path $innerDef) {
        $defDir = $innerDef
    }

    # ── Generate definition.pbism manifest (root-level, no prefix) ──
    $pbismContent = '{"version": "4.0", "settings": {}}'
    $parts += @{
        path        = "definition.pbism"
        payload     = (ConvertTo-Base64FromString $pbismContent)
        payloadType = "InlineBase64"
    }

    # Collect .tmdl files and other definition files
    # Skip cultures/ subdirectory (linguisticMetadata often has invalid XML format)
    $files = Get-ChildItem -Path $defDir -Recurse -File |
        Where-Object {
            $_.Name -ne ".platform" -and
            $_.Name -ne "semantic_model_metadata.json" -and
            $_.Name -ne "migration_metadata.json" -and
            $_.FullName -notlike "*\cultures\*"
        }

    foreach ($file in $files) {
        # Compute relative path from defDir, then prepend definition/
        $relFromDef = $file.FullName.Substring($defDir.Length + 1).Replace("\", "/")
        $relativePath = "definition/$relFromDef"

        if ($file.Extension -eq ".tmdl" -and $file.Name -eq "expressions.tmdl") {
            # Apply SQL endpoint and lakehouse name replacements
            $content = Get-Content -Path $file.FullName -Raw -Encoding UTF8
            if ($SqlEndpoint) {
                $content = $content.Replace("{{YOUR_LAKEHOUSE_SQL_ENDPOINT}}", $SqlEndpoint)
            }
            if ($LakehouseName) {
                $content = $content.Replace("{{YOUR_LAKEHOUSE_NAME}}", $LakehouseName)
            }
            $b64 = ConvertTo-Base64FromString $content
        }
        else {
            $b64 = ConvertTo-Base64 $file.FullName
        }

        $parts += @{
            path        = $relativePath
            payload     = $b64
            payloadType = "InlineBase64"
        }
    }

    return $parts
}

# ============================================================================
# REPORT (PBIR) HELPERS
# ============================================================================

function Get-PbirDefinitionParts {
    <#
    .SYNOPSIS
        Collects all PBIR report definition files and returns them as
        definition parts. Rewrites definition.pbir from byPath to
        byConnection using the deployed Semantic Model ID.
        Prefixes paths with 'definition/' as required by Fabric API.
    #>
    param(
        [string]$ReportDir,
        [string]$SemanticModelId
    )

    $parts = @()
    $defDir = Join-Path $ReportDir "definition"
    if (-not (Test-Path $defDir)) {
        $defDir = $ReportDir
    }

    # Include definition.pbir from the Report root (outside definition/ subdir)
    $pbirFile = Join-Path $ReportDir "definition.pbir"
    $rootPbirIncluded = $false

    $files = Get-ChildItem -Path $defDir -Recurse -File |
        Where-Object {
            $_.Name -ne ".platform" -and
            $_.Name -ne "migration_metadata.json"
        }

    # Also add definition.pbir from root if defDir != ReportDir
    if ($defDir -ne $ReportDir -and (Test-Path $pbirFile)) {
        $rootPbirIncluded = $true
    }

    foreach ($file in $files) {
        $relativePath = $file.FullName.Substring($defDir.Length + 1).Replace("\", "/")

        # Remap RegisteredResources/ → StaticResources/RegisteredResources/
        if ($relativePath -like "RegisteredResources/*") {
            $relativePath = "StaticResources/$relativePath"
        }
        elseif ($relativePath -eq "definition.pbir") {
            # definition.pbir stays at root level (no prefix)
        }
        else {
            # All other files need definition/ prefix
            $relativePath = "definition/$relativePath"
        }

        if ($file.Name -eq "definition.pbir" -and $SemanticModelId) {
            # Rewrite byPath → byConnection for API deployment
            # Use semanticmodelid= connection string format (matches HorizonBooks pattern)
            $pbirContent = @{
                version         = "4.0"
                datasetReference = @{
                    byPath = $null
                    byConnection = @{
                        connectionString            = "semanticmodelid=$SemanticModelId"
                        pbiServiceModelId           = $null
                        pbiModelVirtualServerName   = "sobe_wowvirtualserver"
                        pbiModelDatabaseName        = $SemanticModelId
                        name                        = "EntityDataSource"
                        connectionType              = "pbiServiceXmlaStyleLive"
                    }
                }
            } | ConvertTo-Json -Depth 5

            $b64 = ConvertTo-Base64FromString $pbirContent
        }
        else {
            $b64 = ConvertTo-Base64 $file.FullName
        }

        $parts += @{
            path        = $relativePath
            payload     = $b64
            payloadType = "InlineBase64"
        }
    }

    # Add definition.pbir from Report root (if it wasn't already included from defDir)
    if ($rootPbirIncluded) {
        if ($SemanticModelId) {
            $pbirContent = @{
                version          = "4.0"
                datasetReference = @{
                    byPath       = $null
                    byConnection = @{
                        connectionString          = "semanticmodelid=$SemanticModelId"
                        pbiServiceModelId         = $null
                        pbiModelVirtualServerName = "sobe_wowvirtualserver"
                        pbiModelDatabaseName      = $SemanticModelId
                        name                      = "EntityDataSource"
                        connectionType            = "pbiServiceXmlaStyleLive"
                    }
                }
            } | ConvertTo-Json -Depth 5
            $b64 = ConvertTo-Base64FromString $pbirContent
        }
        else {
            $b64 = ConvertTo-Base64 $pbirFile
        }
        $parts += @{
            path        = "definition.pbir"
            payload     = $b64
            payloadType = "InlineBase64"
        }
    }

    return $parts
}

# ============================================================================
# PIPELINE HELPERS
# ============================================================================

function Get-PipelineDefinitionParts {
    <#
    .SYNOPSIS
        Reads the pipeline definition JSON, extracts the 'properties' block
        (the only content Fabric API accepts), and returns it as a definition part.
        Resolves {{TOKEN}} placeholders and legacy activity types using actual
        deployed item IDs — first from $DeployedItems, then by querying the
        workspace API for existing items.
    #>
    param(
        [string]$PipelineDir,
        [hashtable]$DeployedItems = @{},
        [string]$WorkspaceId = ""
    )

    $defFile = Join-Path $PipelineDir "pipeline_definition.json"
    if (-not (Test-Path $defFile)) {
        Write-Warn "Pipeline definition not found: $defFile"
        return @()
    }

    # ── Build an ID lookup by querying workspace for existing items ──────
    # This handles the case where items were deployed in a previous run
    # and aren't in the $DeployedItems hashtable.
    $resolvedIds = @{
        "WORKSPACE_ID" = $WorkspaceId
    }

    # Seed from $DeployedItems first
    foreach ($key in $DeployedItems.Keys) {
        $parts = $key -split ':', 2
        if ($parts.Count -eq 2) {
            $itemType = $parts[0]
            switch ($itemType) {
                "Dataflow"      { $resolvedIds["DATAFLOW_ID"]       = $DeployedItems[$key] }
                "Notebook"      { $resolvedIds["NOTEBOOK_ID"]       = $DeployedItems[$key] }
                "SemanticModel" { $resolvedIds["SEMANTIC_MODEL_ID"] = $DeployedItems[$key] }
            }
        }
    }

    # Query workspace API for any missing IDs
    $typesToQuery = @{
        "DATAFLOW_ID"       = "Dataflow"
        "NOTEBOOK_ID"       = "Notebook"
        "SEMANTIC_MODEL_ID" = "SemanticModel"
    }

    $token = Get-FabricToken
    foreach ($tokenKey in $typesToQuery.Keys) {
        if (-not $resolvedIds.ContainsKey($tokenKey) -or -not $resolvedIds[$tokenKey]) {
            $fabricType = $typesToQuery[$tokenKey]
            try {
                $items = (Invoke-RestMethod `
                    -Uri "$($script:FabricApiBase)/workspaces/$WorkspaceId/items?type=$fabricType" `
                    -Headers @{ Authorization = "Bearer $token" }).value
                # Pick the first item whose displayName matches the pipeline/project name
                $pipelineName = (Split-Path $PipelineDir -Leaf) -replace '\.Pipeline$', ''
                $match = $items | Where-Object { $_.displayName -eq $pipelineName } | Select-Object -First 1
                if ($match) {
                    $resolvedIds[$tokenKey] = $match.id
                    Write-Info "  Resolved $fabricType '$pipelineName' -> $($match.id)"
                }
                elseif ($items.Count -eq 1) {
                    # If exactly one item of this type, use it
                    $resolvedIds[$tokenKey] = $items[0].id
                    Write-Info "  Resolved $fabricType '$($items[0].displayName)' -> $($items[0].id)"
                }
            }
            catch {
                Write-Warn "  Could not query workspace for $fabricType items"
            }
        }
    }

    # ── Read and process the definition ──────────────────────────────────
    $rawJson = Get-Content -Path $defFile -Raw -Encoding UTF8

    # Replace {{TOKEN}} placeholders with resolved IDs
    foreach ($tokenKey in $resolvedIds.Keys) {
        $rawJson = $rawJson -replace "\{\{${tokenKey}\}\}", $resolvedIds[$tokenKey]
    }

    $raw = $rawJson | ConvertFrom-Json

    $activities = @()
    if ($raw.properties -and $raw.properties.activities) {
        $activities = @($raw.properties.activities)
    }

    # ── Backward compat: rewrite legacy activity types ───────────────────
    foreach ($activity in $activities) {
        $tp = $activity.typeProperties
        if (-not $tp) { continue }

        switch ($activity.type) {
            "NotebookActivity" {
                $nbId = $resolvedIds["NOTEBOOK_ID"]
                if ($nbId) {
                    $activity.type = "TridentNotebook"
                    $activity.typeProperties = @{
                        notebookId  = $nbId
                        workspaceId = $WorkspaceId
                    }
                }
            }
            "DataflowRefresh" {
                $dfId = $resolvedIds["DATAFLOW_ID"]
                if ($dfId) {
                    $activity.type = "RefreshDataflow"
                    $activity.typeProperties = @{
                        dataflowId     = $dfId
                        workspaceId    = $WorkspaceId
                        notifyOption   = "NoNotification"
                        dataflowType   = "DataflowFabric"
                    }
                }
            }
            "SemanticModelRefresh" {
                $smId = $resolvedIds["SEMANTIC_MODEL_ID"]
                if ($smId) {
                    $activity.type = "TridentDatasetRefresh"
                    $activity.typeProperties = @{
                        datasetId   = $smId
                        workspaceId = $WorkspaceId
                    }
                }
            }
        }
    }

    # ── Strip fields the Fabric API rejects ──────────────────────────────
    $cleanContent = @{
        properties = @{
            activities = $activities
        }
    }
    # Carry over description if present
    if ($raw.properties -and $raw.properties.description) {
        $cleanContent.properties.description = $raw.properties.description
    }

    $contentJson = $cleanContent | ConvertTo-Json -Depth 15 -Compress:$false
    $b64 = ConvertTo-Base64FromString $contentJson

    return @(
        @{
            path        = "pipeline-content.json"
            payload     = $b64
            payloadType = "InlineBase64"
        }
    )
}

# ============================================================================
# DATAFLOW HELPERS
# ============================================================================

function Get-DataflowDefinitionParts {
    <#
    .SYNOPSIS
        Reads the dataflow definition and returns mashup.pq + queryMetadata.json
        as parts (both required by the Fabric Dataflow Gen2 API).
    #>
    param([string]$DataflowDir)

    $parts = @()
    $mashupContent = $null

    # Main definition
    $defFile = Join-Path $DataflowDir "dataflow_definition.json"
    if (Test-Path $defFile) {
        # Extract mashup document from the definition
        $dfDef = Get-Content -Path $defFile -Raw -Encoding UTF8 | ConvertFrom-Json

        if ($dfDef.mashupDocument) {
            $mashupContent = $dfDef.mashupDocument
        }
    }

    # Standalone mashup file (fallback)
    $mashupFile = Join-Path $DataflowDir "mashup.pq"
    if (-not $mashupContent -and (Test-Path $mashupFile)) {
        $mashupContent = Get-Content -Path $mashupFile -Raw -Encoding UTF8
    }

    # Also try queries/ subdir for individual .m files
    if (-not $mashupContent) {
        $queriesDir = Join-Path $DataflowDir "queries"
        if (Test-Path $queriesDir) {
            $mFiles = Get-ChildItem -Path $queriesDir -Filter "*.m" -File
            if ($mFiles.Count -gt 0) {
                # Combine all .m files into a section document
                $sections = @("section Section1;")
                foreach ($mf in $mFiles) {
                    $queryName = [System.IO.Path]::GetFileNameWithoutExtension($mf.Name)
                    $queryBody = Get-Content -Path $mf.FullName -Raw -Encoding UTF8
                    $sections += "shared $queryName = $queryBody;"
                }
                $mashupContent = $sections -join "`n`n"
            }
        }
    }

    if ($mashupContent) {
        $parts += @{
            path        = "mashup.pq"
            payload     = (ConvertTo-Base64FromString $mashupContent)
            payloadType = "InlineBase64"
        }

        # ── Generate queryMetadata.json (required by Fabric Dataflow API) ──
        $queryMetaFile = Join-Path $DataflowDir "queryMetadata.json"
        if (Test-Path $queryMetaFile) {
            # Use existing file if present
            $parts += @{
                path        = "queryMetadata.json"
                payload     = (ConvertTo-Base64 $queryMetaFile)
                payloadType = "InlineBase64"
            }
        }
        else {
            # Auto-generate minimal queryMetadata from the mashup content
            $queriesMeta = @{}
            # Parse query names from 'shared QueryName =' pattern
            $matches = [regex]::Matches($mashupContent, 'shared\s+(\w+)\s*=')
            foreach ($m in $matches) {
                $qName = $m.Groups[1].Value
                $queriesMeta[$qName] = @{
                    queryId     = [guid]::NewGuid().ToString()
                    queryName   = $qName
                    isHidden    = $false
                    loadEnabled = $true
                }
            }

            $queryMetaObj = @{
                name                   = "Section1"
                formatVersion          = "202502"
                computeEngineSettings  = @{ allowFastCopy = $true; maxConcurrency = 1 }
                queriesMetadata        = $queriesMeta
            }
            $queryMetaJson = $queryMetaObj | ConvertTo-Json -Depth 5
            $parts += @{
                path        = "queryMetadata.json"
                payload     = (ConvertTo-Base64FromString $queryMetaJson)
                payloadType = "InlineBase64"
            }
        }
    }

    return $parts
}

# ============================================================================
# SQL ENDPOINT DISCOVERY
# ============================================================================

function Wait-ForSqlEndpoint {
    <#
    .SYNOPSIS
        Polls the Lakehouse API until the SQL analytics endpoint is provisioned.
        Returns the connection string.
    #>
    param(
        [string]$WsId,
        [string]$LakehouseId,
        [string]$Token,
        [int]$TimeoutSeconds = 180
    )

    Write-Info "Waiting for SQL analytics endpoint (may take 1-3 minutes)..."

    $elapsed = 0
    while ($elapsed -lt $TimeoutSeconds) {
        try {
            $lh = Invoke-RestMethod `
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/lakehouses/$LakehouseId" `
                -Headers @{ Authorization = "Bearer $Token" }

            $sqlEp = $lh.properties.sqlEndpointProperties.connectionString
            if ($sqlEp) {
                Write-Success "SQL endpoint ready: $sqlEp"
                return $sqlEp
            }
        }
        catch {
            Write-Info "  Endpoint not ready yet ($elapsed s)..."
        }

        Start-Sleep -Seconds 15
        $elapsed += 15
    }

    Write-Warn "SQL endpoint not available after ${TimeoutSeconds}s. Continue anyway."
    return $null
}

# ============================================================================
# NOTEBOOK JOB RUNNER
# ============================================================================

function Run-FabricNotebook {
    <#
    .SYNOPSIS
        Triggers a Fabric Spark notebook job and waits for completion.
    #>
    param(
        [string]$NotebookId,
        [string]$NotebookName,
        [string]$WsId,
        [string]$Token,
        [int]$TimeoutMinutes = 15
    )

    Write-Info "Starting $NotebookName (Spark session may take a few minutes)..."

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
    $jobLoc  = $null

    for ($runAttempt = 1; $runAttempt -le 3; $runAttempt++) {
        if ($runAttempt -gt 1) {
            Write-Info "Run retry $runAttempt/3 - waiting 30s..."
            Start-Sleep -Seconds 30
            $Token   = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $runResp = Invoke-WebRequest -Method Post `
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/items/$NotebookId/jobs/instances?jobType=RunNotebook" `
                -Headers $headers -UseBasicParsing

            if ($runResp.StatusCode -eq 202) {
                $jobLoc = $runResp.Headers["Location"]
                break
            }
        }
        catch {
            Write-Warn "Run error (attempt $runAttempt): $($_.Exception.Message)"
        }
    }

    if (-not $jobLoc) {
        Write-Warn "${NotebookName}: Could not start notebook job"
        return $false
    }

    $maxSeconds = $TimeoutMinutes * 60
    $waited     = 0
    while ($waited -lt $maxSeconds) {
        Start-Sleep -Seconds 15
        $waited += 15
        try {
            $jobStat = Invoke-RestMethod -Uri $jobLoc `
                -Headers @{ Authorization = "Bearer $Token" }
            Write-Info ("  {0} status: {1} ({2}s)" -f $NotebookName, $jobStat.status, $waited)
            if ($jobStat.status -eq "Completed") {
                Write-Success "$NotebookName completed"
                return $true
            }
            if ($jobStat.status -eq "Failed" -or $jobStat.status -eq "Cancelled") {
                $reason = ""
                if ($jobStat.failureReason) { $reason = $jobStat.failureReason.message }
                Write-Err "$NotebookName $($jobStat.status): $reason"
                return $false
            }
        }
        catch {
            if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 404) {
                Write-Info ("  Job not ready yet ({0}s)" -f $waited)
            }
            else { Write-Warn "  Poll error: $($_.Exception.Message)" }
        }
    }

    Write-Warn "${NotebookName}: timed out after $TimeoutMinutes minutes"
    return $false
}

# ============================================================================
# PIPELINE JOB RUNNER
# ============================================================================

function Run-FabricPipeline {
    <#
    .SYNOPSIS
        Triggers a Fabric Data Pipeline run and waits for completion.
    #>
    param(
        [string]$PipelineId,
        [string]$PipelineName,
        [string]$WsId,
        [string]$Token,
        [int]$TimeoutMinutes = 30
    )

    Write-Info "Starting pipeline $PipelineName ..."

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
    $jobLoc  = $null

    for ($runAttempt = 1; $runAttempt -le 3; $runAttempt++) {
        if ($runAttempt -gt 1) {
            Write-Info "Pipeline run retry $runAttempt/3 - waiting 30s..."
            Start-Sleep -Seconds 30
            $Token   = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $runResp = Invoke-WebRequest -Method Post `
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/items/$PipelineId/jobs/instances?jobType=Pipeline" `
                -Headers $headers -UseBasicParsing

            if ($runResp.StatusCode -eq 202) {
                $jobLoc = $runResp.Headers["Location"]
                break
            }
        }
        catch {
            Write-Warn "Pipeline run error (attempt $runAttempt): $($_.Exception.Message)"
        }
    }

    if (-not $jobLoc) {
        Write-Warn "Could not start pipeline run"
        return $false
    }

    $maxSeconds = $TimeoutMinutes * 60
    $waited     = 0
    while ($waited -lt $maxSeconds) {
        Start-Sleep -Seconds 20
        $waited += 20
        try {
            $jobStat = Invoke-RestMethod -Uri $jobLoc `
                -Headers @{ Authorization = "Bearer $Token" }
            Write-Info ("  Pipeline status: {0} ({1}s)" -f $jobStat.status, $waited)
            if ($jobStat.status -eq "Completed") {
                Write-Success "Pipeline $PipelineName completed"
                return $true
            }
            if ($jobStat.status -eq "Failed" -or $jobStat.status -eq "Cancelled") {
                $reason = ""
                if ($jobStat.failureReason) { $reason = $jobStat.failureReason.message }
                Write-Err "Pipeline $($jobStat.status): $reason"
                return $false
            }
        }
        catch {
            if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 404) {
                Write-Info ("  Pipeline job not ready yet ({0}s)" -f $waited)
            }
            else { Write-Warn "  Poll error: $($_.Exception.Message)" }
        }
    }

    Write-Warn "Pipeline timed out after $TimeoutMinutes minutes"
    return $false
}

# ============================================================================
# EXPORTS
# ============================================================================

Export-ModuleMember -Function @(
    # Display
    'Write-Banner', 'Write-Step', 'Write-Info', 'Write-Success',
    'Write-Warn', 'Write-Err', 'Measure-Step', 'Show-TimingSummary',
    # Tokens
    'Get-FabricToken', 'Get-StorageToken',
    # Fabric API
    'Invoke-FabricApi', 'Wait-FabricOperation', 'New-OrGetFabricItem',
    # Workspace Folders
    'New-OrGetWorkspaceFolder', 'Move-ItemToFolder',
    # OneLake
    'Upload-FileToOneLake',
    # Definitions
    'Update-FabricItemDefinition', 'Update-PipelineDefinition',
    'Update-DataflowDefinition', 'ConvertTo-Base64', 'ConvertTo-Base64FromString',
    # Notebook
    'ConvertFrom-VSCodeCellNotebook', 'ConvertTo-FabricPyNotebook', 'Set-NotebookLakehouseBinding',
    # TMDL / Semantic Model
    'Get-TmdlDefinitionParts',
    # PBIR / Report
    'Get-PbirDefinitionParts',
    # Pipeline
    'Get-PipelineDefinitionParts',
    # Dataflow
    'Get-DataflowDefinitionParts',
    # SQL Endpoint
    'Wait-ForSqlEndpoint',
    # Jobs
    'Run-FabricNotebook', 'Run-FabricPipeline'
) -Variable @('FabricApiBase', 'OneLakeBase')
