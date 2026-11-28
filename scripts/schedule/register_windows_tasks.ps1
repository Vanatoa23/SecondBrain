<#
.SYNOPSIS
    Registers local Windows Scheduled Tasks for Second Brain maintenance.

.DESCRIPTION
    Creates two user-level scheduled tasks (no admin privileges required):
      1. SecondBrain-NightlyCurator  (Daily at 03:00)  -> scripts/curator.py
      2. SecondBrain-NightlyWiki     (Daily at 03:15)  -> scripts/export_wiki.py

    Supports -WhatIf to preview without making any changes.
    Requires DATABASE_URL to be set in the user environment (never passed as CLI args).
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$PythonExe = "python",
    [string]$CuratorTime = "03:00",
    [string]$WikiTime = "03:15"
)

$ErrorActionPreference = "Stop"

$curatorScript = Join-Path $RepoRoot "scripts\curator.py"
$wikiScript    = Join-Path $RepoRoot "scripts\export_wiki.py"

if (-not (Test-Path $curatorScript)) {
    throw "Curator script not found at: $curatorScript"
}
if (-not (Test-Path $wikiScript)) {
    throw "Wiki export script not found at: $wikiScript"
}

$tasks = @(
    @{
        Name   = "SecondBrain-NightlyCurator"
        Time   = $CuratorTime
        Args   = "`"$curatorScript`""
        Desc   = "Second Brain nightly curator consolidation (staleness, contradictions, flagged issues)"
    },
    @{
        Name   = "SecondBrain-NightlyWiki"
        Time   = $WikiTime
        Args   = "`"$wikiScript`""
        Desc   = "Second Brain nightly Markdown wiki export"
    }
)

foreach ($t in $tasks) {
    if ($PSCmdlet.ShouldProcess($t.Name, "Register scheduled task at $($t.Time)")) {
        $action   = New-ScheduledTaskAction -Execute $PythonExe -Argument $t.Args -WorkingDirectory $RepoRoot
        $trigger  = New-ScheduledTaskTrigger -Daily -At $t.Time
        $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopIfGoingOnBatteries
        Register-ScheduledTask `
            -TaskName $t.Name `
            -Action $action `
            -Trigger $trigger `
            -Settings $settings `
            -Description $t.Desc `
            -Force | Out-Null
        Write-Host "Registered task: $($t.Name) (Daily at $($t.Time))"
    }
}
