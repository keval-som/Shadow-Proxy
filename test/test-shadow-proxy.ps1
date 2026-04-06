# =============================================================================
# Shadow Proxy -- Integration Test Suite
# =============================================================================
# Prerequisites:
#   Infra running : docker compose up -d postgres-primary postgres-replica redis rabbitmq
#   Proxy running : java --enable-preview -jar target\shadow-proxy-1.0.0-SNAPSHOT.jar | Tee-Object -FilePath test\proxy.log
#
# Usage:
#   .\test\test-shadow-proxy.ps1
#   .\test\test-shadow-proxy.ps1 -LogFile ".\test\proxy.log"
# =============================================================================

param(
    [string]$ProxyHost        = "host.docker.internal",
    [int]   $ProxyPort        = 5432,
    [string]$DbUser           = "shadowuser",
    [string]$DbPass           = "shadowpass",
    [string]$DbName           = "shadowdb",
    [string]$LogFile          = "",
    [string]$PsqlImage        = "registry.hub.docker.com/bitnami/postgresql:latest",
    [string]$RedisContainer   = "shadow-redis",
    [string]$ReplicaContainer = "shadow-postgres-replica"
)

# -- Counters and helpers -----------------------------------------------------
$global:pass = 0
$global:fail = 0
$global:warn = 0

function Write-Pass { param([string]$msg) Write-Host "  [PASS] $msg" -ForegroundColor Green;  $global:pass++ }
function Write-Fail { param([string]$msg) Write-Host "  [FAIL] $msg" -ForegroundColor Red;    $global:fail++ }
function Write-Warn { param([string]$msg) Write-Host "  [WARN] $msg" -ForegroundColor Yellow; $global:warn++ }
function Write-Head { param([string]$msg) Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Write-Info { param([string]$msg) Write-Host "       $msg"   -ForegroundColor DarkGray }

$connStr     = "postgresql://${DbUser}:${DbPass}@${ProxyHost}:${ProxyPort}/${DbName}?sslmode=disable"
# Resolve log file to absolute path once so all functions see the same value
$resolvedLog = if ($LogFile) {
    $rp = Resolve-Path $LogFile -ErrorAction SilentlyContinue
    if ($rp) { $rp.Path } else { "" }
} else { "" }

# -- Run a single SQL statement, stripping Docker/Bitnami startup noise -------
function Invoke-Sql {
    param([string]$Sql)
    $raw = docker run --rm `
        -e PGPASSWORD=$DbPass `
        $PsqlImage `
        psql -At $connStr -c $Sql 2>&1
    # Bitnami images print startup lines like "postgresql HH:MM:SS INFO  ==>"
    # Filter them out so callers only see actual psql output
    $clean = $raw | Where-Object { $_ -notmatch 'INFO\s+==>' -and $_ -notmatch '^\s*$' }
    return $clean
}

# -- Safely convert Invoke-Sql result to a single string ----------------------
function Get-SqlScalar {
    param([object]$Result)
    # Result may be a string or an array -- grab the first meaningful line
    if ($Result -is [array]) {
        $line = $Result | Where-Object { $_ -notmatch 'INFO|WARN|==>' } | Select-Object -First 1
        return "$line".Trim()
    }
    return "$Result".Trim()
}

# -- Assert a pattern exists in the proxy log tail ----------------------------
function Assert-LogContains {
    param([string]$Pattern, [string]$Description)
    $lf = $script:resolvedLog
    if (-not $lf -or -not (Test-Path $lf)) {
        Write-Warn "$Description  [no -LogFile provided or file missing]"
        return
    }
    $tail    = Get-Content $lf -Tail 120
    $matched = $tail | Select-String -Pattern $Pattern -Quiet
    if ($matched) { Write-Pass $Description }
    else          { Write-Fail "$Description  (pattern not found: $Pattern)" }
}

# =============================================================================
# PRE-FLIGHT
# =============================================================================
Write-Head "PRE-FLIGHT"

# Check log file status upfront so warnings are explained once
if ($resolvedLog -and (Test-Path $resolvedLog)) {
    Write-Pass "Log file found: $resolvedLog"
} elseif ($LogFile) {
    Write-Warn "Log file not found at '$LogFile' -- log assertions will be skipped"
    Write-Warn "  Start the proxy with: java --enable-preview -jar target\shadow-proxy-1.0.0-SNAPSHOT.jar | Tee-Object -FilePath test\proxy.log"
} else {
    Write-Info "No -LogFile provided -- log assertions will be skipped"
}

$verRaw = Invoke-Sql 'SELECT version();'
$ver    = Get-SqlScalar $verRaw
if ($ver -match "PostgreSQL") {
    Write-Pass "Proxy is reachable"
    Write-Info $ver
} else {
    Write-Fail "Cannot connect to proxy at ${ProxyHost}:${ProxyPort} -- aborting (got: $ver)"
    exit 1
}

Write-Head "SEED -- reset test data"
Invoke-Sql "DELETE FROM orders WHERE product LIKE 'TEST_%';" | Out-Null
Invoke-Sql "DELETE FROM users  WHERE email   LIKE '%@shadow-test.com';" | Out-Null
Write-Info "Stale test rows removed"

# =============================================================================
# GROUP 1 -- Read / Write Routing
# =============================================================================
Write-Head "1. READ-WRITE ROUTING"

Invoke-Sql 'SELECT * FROM users;' | Out-Null
Start-Sleep -Milliseconds 300
Assert-LogContains "node=replica-1" "1a -- SELECT routed to replica"

Invoke-Sql "INSERT INTO users (username, email) VALUES ('test_routing', 'routing@shadow-test.com');" | Out-Null
Start-Sleep -Milliseconds 300
Assert-LogContains "node=primary" "1b -- INSERT routed to primary"

Invoke-Sql "UPDATE users SET email = 'routing2@shadow-test.com' WHERE username = 'test_routing';" | Out-Null
Start-Sleep -Milliseconds 300
Assert-LogContains "node=primary" "1c -- UPDATE routed to primary"

Invoke-Sql "DELETE FROM users WHERE username = 'test_routing';" | Out-Null
Start-Sleep -Milliseconds 300
Assert-LogContains "node=primary" "1d -- DELETE routed to primary"

$count = Get-SqlScalar (Invoke-Sql 'SELECT COUNT(*) FROM users;')
if ([int]$count -ge 3) {
    Write-Pass "1e -- Row count sane after write cycle ($count rows)"
} else {
    Write-Fail "1e -- Unexpected row count: $count"
}

# =============================================================================
# GROUP 2 -- Two-Level Caching
# =============================================================================
Write-Head "2. TWO-LEVEL CACHE (L1 Caffeine + L2 Redis)"

# DDL wipes both L1 (Caffeine) and L2 (Redis) via CacheHandler.invalidateOnWrite
# FLUSHALL alone only clears L2; L1 would still serve from memory and never re-populate L2
Invoke-Sql 'ALTER TABLE users ADD COLUMN IF NOT EXISTS __cold_test INT;' | Out-Null
Invoke-Sql 'ALTER TABLE users DROP COLUMN IF EXISTS __cold_test;' | Out-Null
Start-Sleep -Milliseconds 500
Write-Info "Both L1 and L2 caches cleared via DDL"

Invoke-Sql 'SELECT * FROM users;' | Out-Null
Start-Sleep -Milliseconds 600
Assert-LogContains "Cache MISS" "2a -- First SELECT is a Cache MISS"
Assert-LogContains "L1 cache PUT" "2b -- L1 (Caffeine) populated"
Assert-LogContains "L2 cache PUT" "2c -- L2 (Redis) populated"

$redisKeys = docker exec $RedisContainer redis-cli KEYS '*'
if ($redisKeys) {
    Write-Pass "2d -- Redis holds $($redisKeys.Count) key(s)"
} else {
    Write-Fail "2d -- Redis is empty after first SELECT (L1 may have served from memory)"
}

Invoke-Sql 'SELECT * FROM users;' | Out-Null
Start-Sleep -Milliseconds 300
Assert-LogContains "Cache L1 HIT" "2e -- Second SELECT served from L1"

Invoke-Sql 'SELECT COUNT(*) FROM users;' | Out-Null
Start-Sleep -Milliseconds 300
Assert-LogContains "Cache MISS" "2f -- Different SQL gets its own cache key"

Invoke-Sql 'SELECT u.username, o.product FROM users u JOIN orders o ON u.id = o.user_id;' | Out-Null
Start-Sleep -Milliseconds 300
Invoke-Sql 'SELECT u.username, o.product FROM users u JOIN orders o ON u.id = o.user_id;' | Out-Null
Start-Sleep -Milliseconds 300
Assert-LogContains "Cache L1 HIT" "2g -- JOIN query cached and served from L1 on repeat"

# =============================================================================
# GROUP 3 -- Cache Invalidation
# =============================================================================
Write-Head "3. CACHE INVALIDATION"

Invoke-Sql 'SELECT * FROM users;' | Out-Null
Invoke-Sql 'SELECT * FROM orders;' | Out-Null
Start-Sleep -Milliseconds 400

Invoke-Sql 'ALTER TABLE users ADD COLUMN IF NOT EXISTS __test_col INT;' | Out-Null
Start-Sleep -Milliseconds 400
Assert-LogContains "invalidateAll" "3a -- DDL triggers full cache invalidation"

Invoke-Sql 'SELECT * FROM users;' | Out-Null
Start-Sleep -Milliseconds 300
Assert-LogContains "Cache MISS" "3b -- SELECT after DDL is a Cache MISS"

# Verify parse failure does NOT wipe cache (the bug we fixed)
Invoke-Sql 'SELECT * FROM users;' | Out-Null
Invoke-Sql 'SELECT * FROM users;' | Out-Null
Start-Sleep -Milliseconds 300
$keysBefore = docker exec $RedisContainer redis-cli KEYS '*'
docker run --rm -e PGPASSWORD=$DbPass $PsqlImage psql -At $connStr -c 'INVALID JUNK 99999' 2>&1 | Out-Null
Start-Sleep -Milliseconds 400
$keysAfter = docker exec $RedisContainer redis-cli KEYS '*'
if ($keysBefore.Count -le $keysAfter.Count) {
    Write-Pass "3c -- Parse failure did NOT wipe cache ($($keysBefore.Count) keys before, $($keysAfter.Count) after)"
} else {
    Write-Fail "3c -- Cache incorrectly wiped on parse failure (before=$($keysBefore.Count) after=$($keysAfter.Count))"
}
Assert-LogContains "treating as CONTROL" "3d -- Unrecognised SQL logged as CONTROL"

Invoke-Sql 'ALTER TABLE users DROP COLUMN IF EXISTS __test_col;' | Out-Null

# =============================================================================
# GROUP 4 -- Data Integrity
# =============================================================================
Write-Head "4. DATA INTEGRITY"

$ts = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
Invoke-Sql "INSERT INTO users (username, email) VALUES ('integ_$ts', 'integ${ts}@shadow-test.com');" | Out-Null
Start-Sleep -Milliseconds 300
docker exec $RedisContainer redis-cli FLUSHALL | Out-Null
$found = Get-SqlScalar (Invoke-Sql "SELECT COUNT(*) FROM users WHERE email = 'integ${ts}@shadow-test.com';")
if ([int]$found -eq 1) {
    Write-Pass "4a -- Inserted row readable through proxy"
} else {
    Write-Fail "4a -- Row not found after INSERT (count=$found)"
}

Invoke-Sql "UPDATE users SET email = 'updated${ts}@shadow-test.com' WHERE username = 'integ_$ts';" | Out-Null
Start-Sleep -Milliseconds 300
docker exec $RedisContainer redis-cli FLUSHALL | Out-Null
$updated = Get-SqlScalar (Invoke-Sql "SELECT COUNT(*) FROM users WHERE email = 'updated${ts}@shadow-test.com';")
if ([int]$updated -eq 1) {
    Write-Pass "4b -- UPDATE propagated correctly"
} else {
    Write-Fail "4b -- Updated row not found (count=$updated)"
}

$sum = Get-SqlScalar (Invoke-Sql 'SELECT SUM(total) FROM orders;')
if ($sum -match '^\d') {
    Write-Pass "4c -- SUM(total) returns numeric result: $sum"
} else {
    Write-Fail "4c -- SUM(total) returned unexpected: $sum"
}

Invoke-Sql "DELETE FROM users WHERE email LIKE '%@shadow-test.com';" | Out-Null
Write-Info "Test rows cleaned up"

# =============================================================================
# GROUP 5 -- Audit Trail (RabbitMQ)
# =============================================================================
Write-Head "5. AUDIT TRAIL (RabbitMQ)"

try {
    $bytes   = [System.Text.Encoding]::ASCII.GetBytes("shadowmq:shadowmqpass")
    $b64     = [Convert]::ToBase64String($bytes)
    $headers = @{ Authorization = "Basic $b64" }
    # Vhost is named "shadow" -- encode as plain "shadow" (only "/" needs %2F encoding)
    $queues  = Invoke-RestMethod "http://localhost:15672/api/queues/shadow" -Headers $headers -ErrorAction Stop
    $auditQ  = $queues | Where-Object { $_.name -like "*audit*" }
    if ($auditQ) {
        Write-Pass "5a -- RabbitMQ audit queue exists: $($auditQ.name)"
        Write-Pass "5b -- Audit queue message count: $($auditQ.messages_ready)"
    } else {
        Write-Warn "5a -- Audit queue not found (events may still be in flight)"
    }
} catch {
    Write-Warn "5   -- RabbitMQ management API unreachable: $_"
}

Assert-LogContains "Audit published eventId=" "5c -- Audit events published to RabbitMQ"

# =============================================================================
# GROUP 6 -- Session Management (Redis)
# =============================================================================
Write-Head "6. SESSION MANAGEMENT (Redis)"

Assert-LogContains "Session saved id=" "6a -- Session persisted to Redis on connect"

# =============================================================================
# GROUP 7 -- Circuit Breaker
# =============================================================================
Write-Head "7. CIRCUIT BREAKER"

Write-Info "Stopping replica ($ReplicaContainer)..."
docker stop $ReplicaContainer | Out-Null
Start-Sleep -Seconds 3

for ($i = 0; $i -lt 6; $i++) {
    Invoke-Sql 'SELECT * FROM users;' 2>&1 | Out-Null
    Start-Sleep -Milliseconds 300
}

Assert-LogContains "OPEN|circuit|CircuitBreaker" "7a -- Circuit breaker opened after replica failures"

Write-Info "Restarting replica..."
docker start $ReplicaContainer | Out-Null
Start-Sleep -Seconds 8

$recovery = Get-SqlScalar (Invoke-Sql 'SELECT COUNT(*) FROM users;')
if ($recovery -match '^\d') {
    Write-Pass "7b -- Proxy recovered after replica restart (rows=$recovery)"
} else {
    Write-Fail "7b -- Proxy did not recover (result=$recovery)"
}

# =============================================================================
# SUMMARY
# =============================================================================
Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host " Shadow Proxy -- Test Results" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  PASSED : $($global:pass)" -ForegroundColor Green
if ($global:fail -gt 0) {
    Write-Host "  FAILED : $($global:fail)" -ForegroundColor Red
} else {
    Write-Host "  FAILED : $($global:fail)" -ForegroundColor Green
}
if ($global:warn -gt 0) {
    Write-Host "  WARNED : $($global:warn)" -ForegroundColor Yellow
} else {
    Write-Host "  WARNED : $($global:warn)" -ForegroundColor DarkGray
}
Write-Host "================================================" -ForegroundColor Cyan

if ($global:fail -gt 0) { exit 1 } else { exit 0 }
