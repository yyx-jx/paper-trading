[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$Message,
  [string[]]$IncludePath = @()
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Invoke-Git {
  param(
    [Parameter(Mandatory = $true)]
    [string[]]$Arguments
  )

  & git @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "git $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
  }
}

function Get-GitOutput {
  param(
    [Parameter(Mandatory = $true)]
    [string[]]$Arguments
  )

  $output = & git @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "git $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
  }
  return $output
}

function Get-GitSingleLine {
  param(
    [Parameter(Mandatory = $true)]
    [string[]]$Arguments
  )

  $output = @(Get-GitOutput -Arguments $Arguments)
  if ($output.Count -eq 0) {
    return ""
  }
  return [string]$output[0]
}

function Test-BlockedPath {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path
  )

  $normalized = $Path.Replace("\", "/")
  return (
    $normalized -like "deploy/*.json" -or
    $normalized -like "deploy/*.zip" -or
    $normalized -like "deploy/windows-test-*" -or
    $normalized -like "deploy/windows-test-*/*" -or
    $normalized -like "remote_few_shot_work/*" -or
    $normalized -eq "remote_few_shot_work"
  )
}

$repoRoot = (Get-GitSingleLine -Arguments @("rev-parse", "--show-toplevel")).Trim()
Set-Location $repoRoot

$branch = (Get-GitSingleLine -Arguments @("branch", "--show-current")).Trim()
if ($branch -ne "deploy") {
  throw "Current branch is '$branch'. This script only pushes from 'deploy'."
}

$originUrl = (Get-GitSingleLine -Arguments @("remote", "get-url", "origin")).Trim()
if ([string]::IsNullOrWhiteSpace($originUrl)) {
  throw "Origin remote is not configured."
}

$mirrorExists = $true
try {
  $mirrorUrl = (Get-GitSingleLine -Arguments @("remote", "get-url", "mirror")).Trim()
} catch {
  $mirrorExists = $false
}

if (-not $mirrorExists) {
  throw "Mirror remote is not configured. Run: git remote add mirror git@github.com:zephyrxu2024-hyper/hyper-terminal.git"
}

Write-Host "== git status --short =="
$statusLines = @(Get-GitOutput -Arguments @("status", "--short"))
$statusLines | ForEach-Object { Write-Host $_ }

$untrackedPaths = @()
foreach ($line in $statusLines) {
  if ($line.StartsWith("?? ")) {
    $untrackedPaths += $line.Substring(3).Trim()
  }
}

$blockedUntracked = @($untrackedPaths | Where-Object { Test-BlockedPath -Path $_ })
if ($blockedUntracked.Count -gt 0) {
  throw "Blocked untracked paths detected:`n - $($blockedUntracked -join "`n - ")"
}

$normalizedIncludes = @($IncludePath | ForEach-Object { $_.Replace("\", "/") } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
foreach ($path in $normalizedIncludes) {
  if (Test-BlockedPath -Path $path) {
    throw "Blocked path cannot be force-included: $path"
  }
  if (-not (Test-Path -LiteralPath (Join-Path $repoRoot $path))) {
    throw "Included path does not exist: $path"
  }
}

Invoke-Git -Arguments @("add", "-u")
foreach ($path in $normalizedIncludes) {
  Invoke-Git -Arguments @("add", "--", $path)
}

$stagedNames = @(Get-GitOutput -Arguments @("diff", "--cached", "--name-only"))
if ($stagedNames.Count -eq 0) {
  throw "No staged changes to commit."
}

Write-Host "== staged files =="
$stagedNames | ForEach-Object { Write-Host $_ }

Invoke-Git -Arguments @("commit", "-m", $Message)
Invoke-Git -Arguments @("push", "origin", "deploy")
Invoke-Git -Arguments @("fetch", "mirror", "main")
Invoke-Git -Arguments @("push", "--force-with-lease", "mirror", "deploy:main")

Write-Host "Push complete: origin/deploy + mirror/main"
