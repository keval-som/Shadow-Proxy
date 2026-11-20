#requires -Version 5.1
Set-StrictMode -Version Latest

function Resolve-LoadDockerExecutable {
    # Get-Command can return both docker.exe and an extensionless Docker shim.
    # ProcessStartInfo.FileName must contain exactly one executable path.
    $name = if ([Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT) { 'docker.exe' } else { 'docker' }
    $command = Get-Command $name -CommandType Application -ErrorAction Stop | Select-Object -First 1
    return $command.Path
}

function Set-LoadProcessArguments {
    param([Parameter(Mandatory)][Diagnostics.ProcessStartInfo]$Info,
          [Parameter(Mandatory)][string[]]$ArgumentList)
    if ($Info.PSObject.Properties['ArgumentList']) {
        foreach ($argument in $ArgumentList) { $Info.ArgumentList.Add($argument) }
    } else {
        # .NET Framework (Windows PowerShell 5.1) has no ArgumentList property.
        # Quote for Windows argv parsing, not for a shell: UseShellExecute is false.
        $quoted = foreach ($argument in $ArgumentList) {
            '"' + ($argument -replace '(\\*)"', '$1$1\"' -replace '(\\+)$', '$1$1') + '"'
        }
        $Info.Arguments = $quoted -join ' '
    }
}

function Stop-LoadProcess {
    param([Parameter(Mandatory)][Diagnostics.Process]$Process)
    if ($PSVersionTable.PSEdition -eq 'Core') { $Process.Kill($true) }
    else { $Process.Kill() }
}

function ConvertTo-LoadDuration {
    param([Parameter(Mandatory)][string]$Value)
    if ($Value -notmatch '^([1-9][0-9]*)(s|m)$') {
        throw "Invalid duration '$Value'. Quote durations in PowerShell, for example -Durations '30s','1m'."
    }
    $seconds = [long]$Matches[1] * $(if ($Matches[2] -eq 'm') { 60 } else { 1 })
    if ($seconds -gt 3600) { throw 'Each measured stage must be at most 1 hour.' }
    return [int]$seconds
}

function Get-LoadPercentile {
    param([Parameter(Mandatory)][AllowEmptyCollection()][double[]]$SortedValues,
          [Parameter(Mandatory)][ValidateRange(0.0, 1.0)][double]$Percentile)
    if ($SortedValues.Count -eq 0) { return $null }
    # Nearest-rank percentile. Input is milliseconds and sorted ascending.
    $index = [Math]::Max(0, [int][Math]::Ceiling($Percentile * $SortedValues.Count) - 1)
    return [Math]::Round($SortedValues[$index], 3)
}

function ConvertFrom-PgBenchReport {
    param([Parameter(Mandatory)][AllowEmptyString()][string]$Text,
          [string[]]$TransactionLogs = @())
    $culture = [Globalization.CultureInfo]::InvariantCulture
    function Read-Number([string]$Pattern) {
        $match = [regex]::Match($Text, $Pattern, [Text.RegularExpressions.RegexOptions]::Multiline)
        if (-not $match.Success) { return $null }
        return [double]::Parse($match.Groups[1].Value, $culture)
    }
    $latencies = [Collections.Generic.List[double]]::new()
    $failureSamples = 0
    $invalidLines = 0
    foreach ($path in $TransactionLogs) {
        foreach ($line in [IO.File]::ReadLines($path)) {
            if ([string]::IsNullOrWhiteSpace($line)) { continue }
            $parts = $line.Trim() -split '\s+'
            if ($parts.Count -lt 6 -or $parts[0] -notmatch '^\d+$' -or $parts[1] -notmatch '^\d+$') {
                $invalidLines++
                continue
            }
            if ($parts[2] -in @('failed', 'serialization', 'deadlock', 'skipped')) {
                $failureSamples++
                continue
            }
            [long]$microseconds = 0
            if ([long]::TryParse($parts[2], [ref]$microseconds) -and $microseconds -ge 0) {
                $latencies.Add($microseconds / 1000.0)
            } else { $invalidLines++ }
        }
    }
    $latencies.Sort()
    return [pscustomobject]@{
        ReportedClients = Read-Number '^number of clients:\s+(\d+)'
        SuccessfulQueries = Read-Number '^number of transactions actually processed:\s+(\d+)'
        ReportedFailedTransactions = Read-Number '^number of failed transactions:\s+(\d+)'
        QueriesPerSecond = Read-Number '^tps\s*=\s*([0-9.]+)'
        AverageLatencyMs = Read-Number '^latency average\s*=\s*([0-9.]+)\s+ms'
        LatencyStddevMs = Read-Number '^latency stddev\s*=\s*([0-9.]+)\s+ms'
        InitialConnectionMs = Read-Number '^initial connection time\s*=\s*([0-9.]+)\s+ms'
        P50LatencyMs = Get-LoadPercentile -SortedValues $latencies.ToArray() -Percentile 0.50
        P95LatencyMs = Get-LoadPercentile -SortedValues $latencies.ToArray() -Percentile 0.95
        P99LatencyMs = Get-LoadPercentile -SortedValues $latencies.ToArray() -Percentile 0.99
        SuccessfulLatencySamples = $latencies.Count
        FailedOrSkippedLogSamples = $failureSamples
        InvalidLogLines = $invalidLines
        HasClientErrors = $Text -match '(?im)(pgbench:\s*(error|fatal):|client\s+\d+.*aborted)'
    }
}

function Stop-LoadContainer {
    param([string]$DockerExecutable, [string]$Name)
    if ($Name -notmatch '^shadow-load-[0-9a-f]{32}$') { throw 'Refusing to remove an unrecognized container name.' }
    $info = [Diagnostics.ProcessStartInfo]::new()
    $info.FileName = $DockerExecutable
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    Set-LoadProcessArguments -Info $info -ArgumentList @('rm', '--force', $Name)
    $cleanup = [Diagnostics.Process]::Start($info)
    try {
        $stdout = $cleanup.StandardOutput.ReadToEndAsync()
        $stderr = $cleanup.StandardError.ReadToEndAsync()
        if (-not $cleanup.WaitForExit(5000)) {
            Stop-LoadProcess -Process $cleanup
            $cleanup.WaitForExit()
            Write-Warning "Docker cleanup timed out. Check container $Name manually."
        } elseif ($cleanup.ExitCode -ne 0) {
            Write-Warning "Could not confirm cleanup for $Name. Check Docker Desktop."
        }
        [void]$stdout.GetAwaiter().GetResult()
        [void]$stderr.GetAwaiter().GetResult()
    } finally { $cleanup.Dispose() }
}

function Invoke-LoadDockerProcess {
    param([Parameter(Mandatory)][string[]]$Arguments,
          [Parameter(Mandatory)][string]$OutputPrefix,
          [Parameter(Mandatory)][int]$TimeoutSeconds,
          [string]$ContainerName = '',
          [string]$DatabasePassword = '',
          [string]$Executable = '')
    $info = [Diagnostics.ProcessStartInfo]::new()
    $info.FileName = if ($Executable) { $Executable } else {
        Resolve-LoadDockerExecutable
    }
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    Set-LoadProcessArguments -Info $info -ArgumentList $Arguments
    # Docker copies this variable into the container. Never place credentials in command arguments.
    $info.EnvironmentVariables['PGPASSWORD'] = $DatabasePassword
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $info
    $startedAt = [DateTimeOffset]::UtcNow
    $watch = [Diagnostics.Stopwatch]::StartNew()
    $timedOut = $false
    $started = $false
    try {
        [void]$process.Start()
        $started = $true
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        while (-not $process.WaitForExit(1000)) {
            if ($watch.Elapsed.TotalSeconds -ge $TimeoutSeconds) {
                $timedOut = $true
                break
            }
            Write-Progress -Activity 'Running PostgreSQL load test' -Status (
                'Elapsed {0:N0}s (watchdog: {1}s)' -f $watch.Elapsed.TotalSeconds, $TimeoutSeconds)
        }
        if ($timedOut) {
            # Only remove the uniquely named container created by this invocation.
            if ($ContainerName) { Stop-LoadContainer -DockerExecutable $info.FileName -Name $ContainerName }
            if (-not $process.WaitForExit(5000)) { Stop-LoadProcess -Process $process; $process.WaitForExit() }
        }
        $out = $stdout.GetAwaiter().GetResult()
        $err = $stderr.GetAwaiter().GetResult()
        [IO.File]::WriteAllText("$OutputPrefix.stdout.txt", $out)
        [IO.File]::WriteAllText("$OutputPrefix.stderr.txt", $err)
        return [pscustomobject]@{
            ExitCode = $process.ExitCode
            TimedOut = $timedOut
            StartedAtUtc = $startedAt.ToString('o')
            EndedAtUtc = [DateTimeOffset]::UtcNow.ToString('o')
            WallSeconds = [Math]::Round($watch.Elapsed.TotalSeconds, 3)
            Text = "$out`n$err"
        }
    } finally {
        Write-Progress -Activity 'Running PostgreSQL load test' -Completed
        # Ctrl+C/exception must not leave a load generator running in the background.
        if ($started -and -not $process.HasExited) {
            if ($ContainerName) { Stop-LoadContainer -DockerExecutable $info.FileName -Name $ContainerName }
            if (-not $process.HasExited) { Stop-LoadProcess -Process $process }
        }
        $process.Dispose()
    }
}
