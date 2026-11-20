#requires -Version 5.1
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
. "$PSScriptRoot/LoadTest.Helpers.ps1"

$passed = 0
function Assert-Equal($Actual, $Expected, [string]$Name) {
    if ($Actual -ne $Expected) { throw "$Name -- expected '$Expected', got '$Actual'" }
    $script:passed++
}
function Assert-Throws([scriptblock]$Action, [string]$Name) {
    $caught = $false
    try { & $Action | Out-Null } catch { $caught = $true }
    if (-not $caught) { throw "$Name -- expected an error" }
    $script:passed++
}

Assert-Equal (ConvertTo-LoadDuration '30s') 30 'Seconds'
Assert-Equal (ConvertTo-LoadDuration '1m') 60 'Minutes'
Assert-Equal (ConvertTo-LoadDuration '60m') 3600 'Maximum stage'
Assert-Throws { ConvertTo-LoadDuration '0s' } 'Reject zero duration'
Assert-Throws { ConvertTo-LoadDuration '61m' } 'Reject excessive duration'
Assert-Throws { ConvertTo-LoadDuration '1hour' } 'Reject invalid unit'
Assert-Throws { Stop-LoadContainer -DockerExecutable 'docker' -Name 'shadow-postgres-primary' } 'Protect non-test containers'
Assert-Equal (Get-LoadPercentile -SortedValues @() -Percentile 0.95) $null 'No samples is null'
Assert-Equal (Get-LoadPercentile -SortedValues @(1.25) -Percentile 0.99) 1.25 'Single sample'

$report = ConvertFrom-PgBenchReport -Text (Get-Content "$PSScriptRoot/fixtures/success.txt" -Raw) `
    -TransactionLogs @("$PSScriptRoot/fixtures/transactions.txt")
Assert-Equal $report.ReportedClients 10 'Client count'
Assert-Equal $report.SuccessfulQueries 100 'Throughput uses complete report, not sampled logs'
Assert-Equal $report.ReportedFailedTransactions 2 'Failure count uses complete report'
Assert-Equal $report.QueriesPerSecond 33.333333 'Reported throughput'
Assert-Equal $report.AverageLatencyMs 3.125 'Mean latency'
Assert-Equal $report.LatencyStddevMs 0.75 'Latency standard deviation'
Assert-Equal $report.InitialConnectionMs 42.25 'Connection setup time'
Assert-Equal $report.SuccessfulLatencySamples 5 'Successful samples only'
Assert-Equal $report.P50LatencyMs 3 'p50 microseconds-to-milliseconds conversion'
Assert-Equal $report.P95LatencyMs 10 'p95 nearest rank'
Assert-Equal $report.P99LatencyMs 10 'p99 nearest rank'
Assert-Equal $report.FailedOrSkippedLogSamples 1 'Failure samples are excluded from latency'
Assert-Equal $report.InvalidLogLines 1 'Malformed records are counted'
Assert-Equal $report.HasClientErrors $false 'Healthy output'

$failed = ConvertFrom-PgBenchReport -Text 'pgbench: error: connection failed'
Assert-Equal $failed.QueriesPerSecond $null 'Startup failure is not zero QPS'
Assert-Equal $failed.P95LatencyMs $null 'Missing latency is not zero latency'
Assert-Equal $failed.HasClientErrors $true 'Connection errors detected'
$empty = ConvertFrom-PgBenchReport -Text ''
Assert-Equal $empty.SuccessfulQueries $null 'Empty report does not invent success'

& "$PSScriptRoot/run-load-test.ps1" -Users 1,10 -Durations '30s','1m' -PlanOnly

# Verify output capture and bounded process termination without requiring Docker.
$checkDirectory = Join-Path $PSScriptRoot ('../../target/load-report-checks/' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $checkDirectory -Force | Out-Null
$checkDirectory = (Resolve-Path $checkDirectory).Path
$shellName = if ($PSVersionTable.PSEdition -eq 'Desktop') { 'powershell.exe' }
    elseif ([Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT) { 'pwsh.exe' }
    else { 'pwsh' }
$shellExecutable = Join-Path $PSHOME $shellName
$processResult = Invoke-LoadDockerProcess -Executable $shellExecutable -Arguments @(
    '-NoProfile', '-Command', '[Console]::WriteLine("fixture output"); [Console]::Error.WriteLine("fixture error"); exit 7'
) -OutputPrefix "$checkDirectory/process check" -TimeoutSeconds 15
Assert-Equal $processResult.ExitCode 7 'Preserves failed process exit code'
Assert-Equal $processResult.TimedOut $false 'Completed process is not a timeout'
Assert-Equal ((Get-Content "$checkDirectory/process check.stdout.txt" -Raw).Trim()) 'fixture output' 'Captures stdout'
Assert-Equal ((Get-Content "$checkDirectory/process check.stderr.txt" -Raw).Trim()) 'fixture error' 'Captures stderr'
$timeoutResult = Invoke-LoadDockerProcess -Executable $shellExecutable -Arguments @(
    '-NoProfile', '-Command', 'Start-Sleep -Seconds 30'
) -OutputPrefix "$checkDirectory/timeout" -TimeoutSeconds 1
Assert-Equal $timeoutResult.TimedOut $true 'Terminates stalled generator process'

# Docker CLI --version needs no daemon and catches executable-discovery regressions.
if (Get-Command docker -CommandType Application -ErrorAction SilentlyContinue) {
    $dockerResult = Invoke-LoadDockerProcess -Arguments @('--version') `
        -OutputPrefix "$checkDirectory/docker-version" -TimeoutSeconds 15
    Assert-Equal $dockerResult.ExitCode 0 'Launches exactly one resolved Docker executable'
    Assert-Equal ($dockerResult.Text -match 'Docker version') $true 'Reads real Docker CLI output'
}
Write-Host "Load report checks passed: $passed"
