#requires -Version 5.1
[CmdletBinding()]
param(
    [ValidateNotNullOrEmpty()][ValidateRange(1, 2000)][int[]]$Users = @(1, 10, 50, 100),
    [ValidateNotNullOrEmpty()][string[]]$Durations = @('30s'),
    [ValidateSet('hot', 'miss')][string]$Workload = 'hot',
    [string]$Server = 'host.docker.internal',
    [ValidateRange(1, 65535)][int]$Port = 5432,
    [string]$Database = 'shadowdb',
    [string]$Username = 'shadowuser',
    [ValidateRange(0, 60)][int]$WarmupSeconds = 5,
    [ValidateRange(0, 60)][int]$CooldownSeconds = 5,
    [ValidateRange(1, 64)][int]$Threads = 4,
    [ValidateRange(0.001, 1.0)][double]$SampleRate = 0.1,
    [ValidateRange(10, 300)][int]$GraceSeconds = 30,
    [string]$Image = 'postgres:16-alpine',
    [switch]$PlanOnly
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
. "$PSScriptRoot/LoadTest.Helpers.ps1"

$secondsList = @($Durations | ForEach-Object { ConvertTo-LoadDuration $_ })
$jobs = @(foreach ($seconds in $secondsList) {
    foreach ($clients in $Users) {
        [pscustomobject]@{ Users = $clients; DurationSeconds = $seconds; Threads = [Math]::Min($clients, $Threads) }
    }
})
Write-Host "Target: ${Server}:${Port}/${Database}; workload=$Workload; simple-query protocol"
Write-Host "Each user keeps one connection and issues one SELECT at a time (no think time)."
$jobs | Format-Table | Out-Host
if ($PlanOnly) { return }

$dockerExecutable = Resolve-LoadDockerExecutable
$dockerStatus = & $dockerExecutable info --format '{{.ServerVersion}}' 2>&1
if ($LASTEXITCODE -ne 0) { throw "Docker is unavailable. Start Docker Desktop first. $dockerStatus" }

$runId = [DateTimeOffset]::UtcNow.ToString('yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
$outputDirectory = Join-Path $PSScriptRoot "results/$runId"
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
$outputDirectory = (Resolve-Path $outputDirectory).Path
$workloadDirectory = (Resolve-Path "$PSScriptRoot/workloads").Path
$password = if ($env:PGPASSWORD) { $env:PGPASSWORD } else { 'shadowpass' }
$sampleText = $SampleRate.ToString([Globalization.CultureInfo]::InvariantCulture)
$results = [Collections.Generic.List[object]]::new()

# Verify pgbench before measuring; image pulls and setup are not benchmark time.
$preflight = Invoke-LoadDockerProcess -Arguments @('run', '--rm', $Image, 'pgbench', '--version') `
    -OutputPrefix "$outputDirectory/preflight" -TimeoutSeconds 300
if ($preflight.ExitCode -ne 0 -or $preflight.TimedOut) {
    throw "Could not start pgbench. Inspect $outputDirectory/preflight.stderr.txt"
}
Write-Host $preflight.Text.Trim()

# Check the existing fixture before generating load; do not benchmark empty/missing data silently.
$seedName = 'shadow-load-' + [guid]::NewGuid().ToString('N')
$seedCheck = Invoke-LoadDockerProcess -Arguments @(
    'run', '--rm', '--name', $seedName,
    '--env', 'PGPASSWORD', '--env', 'PGCONNECT_TIMEOUT=5', '--env', 'PGSSLMODE=disable',
    '--env', 'PGGSSENCMODE=disable', '--env', 'LC_ALL=C',
    $Image, 'psql', '-X', '-A', '-t', '--set=ON_ERROR_STOP=1',
    '-h', $Server, '-p', "$Port", '-U', $Username, '-d', $Database,
    '-c', 'SELECT COUNT(*) FROM public.users WHERE id = 1;'
) -OutputPrefix "$outputDirectory/seed-check" -TimeoutSeconds $GraceSeconds `
    -ContainerName $seedName -DatabasePassword $password
if ($seedCheck.ExitCode -ne 0 -or $seedCheck.TimedOut -or
    (Get-Content "$outputDirectory/seed-check.stdout.txt" -Raw).Trim() -ne '1') {
    throw "Seed check failed: the endpoint must return the existing user with ID 1. Inspect $outputDirectory/seed-check.*.txt"
}

function Invoke-Stage([int]$Clients, [int]$Seconds, [int]$Workers, [string]$Directory, [bool]$Measured) {
    $name = 'shadow-load-' + [guid]::NewGuid().ToString('N')
    $arguments = @('run', '--rm', '--name', $name,
        '--env', 'PGPASSWORD', '--env', 'PGCONNECT_TIMEOUT=5', '--env', 'PGSSLMODE=disable',
        '--env', 'PGGSSENCMODE=disable', '--env', 'LC_ALL=C',
        '--mount', "type=bind,source=$workloadDirectory,target=/work,readonly",
        '--mount', "type=bind,source=$Directory,target=/results",
        '--workdir', '/results', $Image, 'pgbench', '-n', '-M', 'simple',
        '-h', $Server, '-p', "$Port", '-U', $Username,
        '-c', "$Clients", '-j', "$Workers", '-T', "$Seconds", '-P', '5', '-r',
        '-f', "/work/$Workload.sql")
    if ($Measured) { $arguments += @('-l', "--sampling-rate=$sampleText", '--log-prefix=/results/transactions') }
    $arguments += $Database
    Invoke-LoadDockerProcess -Arguments $arguments -OutputPrefix "$Directory/pgbench" `
        -TimeoutSeconds ($Seconds + $GraceSeconds) -ContainerName $name -DatabasePassword $password
}

for ($index = 0; $index -lt $jobs.Count; $index++) {
    $job = $jobs[$index]
    $stage = '{0:D2}-{1}-{2}users-{3}s' -f ($index + 1), $Workload, $job.Users, $job.DurationSeconds
    $stageDirectory = Join-Path $outputDirectory $stage
    New-Item -ItemType Directory -Path $stageDirectory -Force | Out-Null
    Write-Host "`n[$($index + 1)/$($jobs.Count)] $stage"
    $warmupFailed = $false
    if ($WarmupSeconds -gt 0) {
        $warmupDirectory = Join-Path $stageDirectory 'warmup'
        New-Item -ItemType Directory -Path $warmupDirectory -Force | Out-Null
        Write-Host "Warming up for ${WarmupSeconds}s (excluded from measurements)..."
        $warmup = Invoke-Stage $job.Users $WarmupSeconds $job.Threads $warmupDirectory $false
        $warmupFailed = $warmup.ExitCode -ne 0 -or $warmup.TimedOut
    }
    if ($warmupFailed) {
        $execution = $warmup
        $status = 'warmup_failed'
        $report = ConvertFrom-PgBenchReport -Text ''
    } else {
        Write-Host "Measuring $($job.Users) users for $($job.DurationSeconds)s..."
        $execution = Invoke-Stage $job.Users $job.DurationSeconds $job.Threads $stageDirectory $true
        $logs = @(Get-ChildItem -LiteralPath $stageDirectory -Filter 'transactions.*' -File | Select-Object -ExpandProperty FullName)
        $report = ConvertFrom-PgBenchReport -Text $execution.Text -TransactionLogs $logs
        $status = if ($execution.TimedOut) { 'timed_out' }
            elseif ($execution.ExitCode -ne 0 -or $report.HasClientErrors -or
                    $null -eq $report.QueriesPerSecond -or $report.SuccessfulQueries -le 0 -or
                    $report.ReportedClients -ne $job.Users -or $report.ReportedFailedTransactions -gt 0) { 'failed' }
            else { 'completed' }
    }
    $row = [ordered]@{
        Stage = $stage; Status = $status; Users = $job.Users; DurationSeconds = $job.DurationSeconds
        Workload = $Workload; Server = $Server; Port = $Port; Database = $Database
        Threads = $job.Threads; WarmupSeconds = $WarmupSeconds; SampleRate = $SampleRate
        StartedAtUtc = $execution.StartedAtUtc; EndedAtUtc = $execution.EndedAtUtc
        WallSeconds = $execution.WallSeconds; ExitCode = $execution.ExitCode; TimedOut = $execution.TimedOut
    }
    foreach ($property in $report.PSObject.Properties) { $row[$property.Name] = $property.Value }
    $results.Add([pscustomobject]$row)
    $results | Export-Csv "$outputDirectory/summary.csv" -NoTypeInformation -Encoding utf8
    ConvertTo-Json -InputObject @($results.ToArray()) -Depth 5 | Set-Content "$outputDirectory/summary.json" -Encoding utf8
    Write-Host "Status: $status; qps=$($report.QueriesPerSecond); avg=$($report.AverageLatencyMs)ms; sampled p95=$($report.P95LatencyMs)ms"
    if ($status -ne 'completed') {
        Write-Warning "Stage incomplete. Stopping the matrix; inspect its pgbench stderr. Results: $outputDirectory"
        break
    }
    if ($report.SuccessfulLatencySamples -lt 1000) { Write-Warning 'Fewer than 1000 latency samples; tail percentiles are rough estimates.' }
    if ($index -lt $jobs.Count - 1 -and $CooldownSeconds -gt 0) { Start-Sleep -Seconds $CooldownSeconds }
}
Write-Host "`nResults: $outputDirectory"
$results | Format-Table Users, DurationSeconds, Status, QueriesPerSecond, AverageLatencyMs, P95LatencyMs, P99LatencyMs | Out-Host
if (@($results | Where-Object Status -ne 'completed').Count -gt 0) { exit 2 }
