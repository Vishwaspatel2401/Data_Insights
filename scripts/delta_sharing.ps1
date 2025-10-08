# Requires -Version 7.0
<#
.SYNOPSIS
Delta Sharing (curl-only) helper.

.DESCRIPTION
Enumerates all shares/schemas/tables exposed by a Delta Sharing endpoint,
queries signed file URLs for each table, and optionally downloads the first
file per table to an output directory. Designed to be mostly side-effect free:
no implicit Set-Location, only explicit downloads when -Save is set and
approved via -WhatIf/-Confirm.

.PARAMETER ConfigPath
Path to a JSON file with keys: endpoint, bearerToken. Defaults to
"../credentials/config.share" relative to this script's parent directory.

.PARAMETER OutputDir
Directory where downloaded files are stored. Defaults to
"../data/raw" relative to this script's parent directory.

.PARAMETER Save
When provided, downloads the first file per table to OutputDir. Without -Save,
the script performs a dry query and returns metadata only.

.EXAMPLE
pwsh ./scripts/delta_sharing.ps1 -Verbose -InformationAction Continue

.EXAMPLE
pwsh ./scripts/delta_sharing.ps1 -Save -WhatIf -InformationAction Continue
#>

[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Low')]
param(
  [Parameter(Mandatory = $false)]
  [ValidateNotNullOrEmpty()]
  [string]$ConfigPath = (Join-Path -Path (Split-Path -Path $PSScriptRoot -Parent) -ChildPath 'credentials/config.share'),

  [Parameter(Mandatory = $false)]
  [ValidateNotNullOrEmpty()]
  [string]$OutputDir = (Join-Path -Path (Split-Path -Path $PSScriptRoot -Parent) -ChildPath 'data/raw'),

  [Parameter(Mandatory = $false)]
  [switch]$Save
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Read-Credentials {
  [CmdletBinding()] param([Parameter(Mandatory)][string]$Path)
  Write-Verbose "Reading credentials from '$Path'"
  if (-not (Test-Path -LiteralPath $Path)) {
    throw "Credentials file not found: $Path"
  }
  try {
    $json = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
  } catch {
    throw "Failed to parse credentials JSON at ${Path}: $($_.Exception.Message)"
  }
  if (-not $json.endpoint -or -not $json.bearerToken) {
    throw "Credentials JSON must include 'endpoint' and 'bearerToken'"
  }
  Write-Information "Loaded endpoint: $($json.endpoint)" -InformationAction Continue
  return $json
}

function New-AuthHeaders {
  [CmdletBinding()] param([Parameter(Mandatory)][string]$Bearer)
  Write-Verbose "Building authorization headers"
  return @{
    Authorization = "Bearer $Bearer"
    'Content-Type' = 'application/json'
    Accept = 'application/json'
  }
}

function Get-AllTables {
  [CmdletBinding()] param([Parameter(Mandatory)][string]$BaseUrl, [Parameter(Mandatory)][hashtable]$Headers)
  Write-Information "Fetching shares from $BaseUrl/shares" -InformationAction Continue
  $tables = @()
  $sharesUrl = "$BaseUrl/shares"
  $shares = Invoke-RestMethod -Method Get -Uri $sharesUrl -Headers $Headers
  $shareCount = @($shares.items).Count
  Write-Information "Discovered $shareCount share(s)" -InformationAction Continue
  foreach ($s in $shares.items) {
    $schemasUrl = "$BaseUrl/shares/$($s.name)/schemas"
    Write-Verbose "Fetching schemas: $schemasUrl"
    $schemas = Invoke-RestMethod -Method Get -Uri $schemasUrl -Headers $Headers
    foreach ($sc in $schemas.items) {
      $tablesUrl = "$BaseUrl/shares/$($s.name)/schemas/$($sc.name)/tables"
      Write-Verbose "Fetching tables: $tablesUrl"
      $tbls = Invoke-RestMethod -Method Get -Uri $tablesUrl -Headers $Headers
      foreach ($t in $tbls.items) {
        $tables += [pscustomobject]@{
          share = $s.name
          schema = $sc.name
          name = $t.name
        }
      }
    }
  }
  Write-Information "Total tables discovered: $(@($tables).Count)" -InformationAction Continue
  return $tables
}

function Get-TableFiles {
  [CmdletBinding()] param(
    [Parameter(Mandatory)][string]$BaseUrl,
    [Parameter(Mandatory)][hashtable]$Headers,
    [Parameter(Mandatory)][string]$Share,
    [Parameter(Mandatory)][string]$Schema,
    [Parameter(Mandatory)][string]$Table
  )
  $queryUrl = "$BaseUrl/shares/$Share/schemas/$Schema/tables/$Table/query"
  Write-Verbose "Querying files for $Share.$Schema.$Table via $queryUrl"
  $body = @{ predicateHints = @() } | ConvertTo-Json
  $resp = Invoke-RestMethod -Method Post -Uri $queryUrl -Headers $Headers -Body $body
  return $resp.addFiles
}

function Start-FileDownload {
  [CmdletBinding()] param([Parameter(Mandatory)][string]$Url, [Parameter(Mandatory)][string]$Destination)
  $destDir = Split-Path -Parent $Destination
  if (-not (Test-Path -LiteralPath $destDir)) {
    Write-Verbose "Creating directory '$destDir'"
    New-Item -ItemType Directory -Path $destDir -Force | Out-Null
  }
  Write-Verbose "Downloading → $Destination"
  try {
    if ($IsWindows) {
      Start-BitsTransfer -Source $Url -Destination $Destination
    } else {
      Invoke-WebRequest -Uri $Url -OutFile $Destination
    }
  } catch {
    throw "Failed to download $Url -> ${Destination}: $($_.Exception.Message)"
  }
}

function Format-TableId {
  [CmdletBinding()] param([Parameter(Mandatory)][pscustomobject]$Table)
  return "$($Table.share).$($Table.schema).$($Table.name)"
}

function Invoke-DeltaSharingFetch {
  [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory)][string]$ConfigPath,
    [Parameter(Mandatory)][string]$OutputDirectory,
    [Parameter()][switch]$DoSave
  )

  $creds = Read-Credentials -Path $ConfigPath
  $baseUrl = $creds.endpoint.TrimEnd('/')
  $headers = New-AuthHeaders -Bearer $creds.bearerToken

  $targets = Get-AllTables -BaseUrl $baseUrl -Headers $headers
  $results = @()
  $successful = 0

  $i = 0
  foreach ($t in $targets) {
    $i++
    $full = Format-TableId -Table $t
    Write-Information "📥 $full" -InformationAction Continue
    try {
      $files = Get-TableFiles -BaseUrl $baseUrl -Headers $headers -Share $t.share -Schema $t.schema -Table $t.name
    } catch {
      Write-Warning "Failed to query files for ${full}: $($_.Exception.Message)"
      $results += [pscustomobject]@{ table = $full; status = 'query_failed'; url = $null; destination = $null }
      continue
    }
    if (-not $files -or $files.Count -eq 0) {
      Write-Warning "No files for $full"
      $results += [pscustomobject]@{ table = $full; status = 'no_files'; url = $null; destination = $null }
      continue
    }

    $fileUrl = $files[0].url
    Write-Information "URL: $fileUrl" -InformationAction Continue

    if ($DoSave) {
      $timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
      $short = $t.name
      $dest = Join-Path $OutputDirectory ("{0}_{1}.parquet" -f $short, $timestamp)
      if ($PSCmdlet.ShouldProcess($dest, "Download file")) {
        try {
          Start-FileDownload -Url $fileUrl -Destination $dest
          Write-Information "✅ Saved → $dest" -InformationAction Continue
          $successful++
          $results += [pscustomobject]@{ table = $full; status = 'saved'; url = $fileUrl; destination = $dest }
        } catch {
          Write-Warning "Download failed for ${full}: $($_.Exception.Message)"
          $results += [pscustomobject]@{ table = $full; status = 'download_failed'; url = $fileUrl; destination = $dest }
        }
      } else {
        $results += [pscustomobject]@{ table = $full; status = 'skipped_by_whatif'; url = $fileUrl; destination = $dest }
      }
    } else {
      $results += [pscustomobject]@{ table = $full; status = 'listed'; url = $fileUrl; destination = $null }

    }
  }

  $summary = [pscustomobject]@{
    baseUrl = $baseUrl
    totalTables = @($targets).Count
    successfulSaves = $successful
    saved = [bool]$DoSave
    outputDir = $OutputDirectory
    results = $results
  }

  if ($DoSave) {
    if ($successful -gt 0) {
      Write-Information ("🎉 Successfully saved {0} file(s)." -f $successful) -InformationAction Continue
    } else {
      Write-Information "😞 No data could be fetched." -InformationAction Continue
    }
  }

  return $summary
}

try {
  $summary = Invoke-DeltaSharingFetch -ConfigPath $ConfigPath -OutputDirectory $OutputDir -DoSave:$Save
  $summary
} catch {
  Write-Error $_
  throw
}


