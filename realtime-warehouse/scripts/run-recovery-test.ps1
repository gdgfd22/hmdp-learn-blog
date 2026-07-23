param(
    [ValidateRange(1000, 1000000)]
    [int]$Count = 30000,
    [ValidateRange(10, 100000)]
    [int]$RatePerSecond = 500,
    [ValidateRange(1, 30)]
    [int]$TimeoutMinutes = 10,
    [string]$ResultDirectory = ""
)

$ErrorActionPreference = "Stop"
$composeFile = (Resolve-Path (Join-Path $PSScriptRoot "..\docker-compose.yml")).Path
$generatorScript = (Resolve-Path (Join-Path $PSScriptRoot "generate-events.ps1")).Path
$flinkPort = if ($env:FLINK_UI_PORT) { $env:FLINK_UI_PORT } else { "18081" }

if ([string]::IsNullOrWhiteSpace($ResultDirectory)) {
    $ResultDirectory = Join-Path $PSScriptRoot "..\benchmark-results"
}
$ResultDirectory = [System.IO.Path]::GetFullPath($ResultDirectory)
New-Item -ItemType Directory -Path $ResultDirectory -Force | Out-Null

function Get-RunningJobs {
    $overview = Invoke-RestMethod -Uri "http://localhost:$flinkPort/jobs/overview" -TimeoutSec 10
    return @($overview.jobs)
}

function Get-DwdCompletedCheckpointCount {
    $job = Get-RunningJobs |
        Where-Object { $_.name -eq "hmdp-dwd-clean-and-cdc" -and $_.state -eq "RUNNING" } |
        Sort-Object -Property "start-time" -Descending |
        Select-Object -First 1
    if ($null -eq $job) {
        return 0
    }
    $checkpoints = Invoke-RestMethod -Uri "http://localhost:$flinkPort/jobs/$($job.jid)/checkpoints" -TimeoutSec 10
    return [int]$checkpoints.counts.completed
}

function Get-KafkaLag {
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = docker compose -f $composeFile exec -T kafka `
        /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server kafka:29092 `
        --group hmdp-dwd-behavior `
        --describe 2>$null
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousPreference
    if ($exitCode -ne 0) {
        return $null
    }
    $total = 0L
    $matched = $false
    foreach ($line in $output) {
        $parts = $line.Trim() -split "\s+"
        if ($parts.Count -ge 6 -and $parts[0] -eq "hmdp-dwd-behavior" -and $parts[1] -eq "ods_behavior_event") {
            $value = 0L
            if ([long]::TryParse($parts[5], [ref]$value)) {
                $total += $value
                $matched = $true
            }
        }
    }
    if ($matched) { return $total }
    return $null
}

function Get-DorisCount([string]$Prefix) {
    $sql = "SELECT COUNT(*) FROM hmdp_analytics.dwd_user_behavior_detail WHERE event_id LIKE '$Prefix%';"
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = docker compose -f $composeFile exec -T mysql `
        mysql -hdoris-fe -P9030 -uroot -N -B -e $sql 2>$null
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousPreference
    if ($exitCode -ne 0) {
        return 0L
    }
    return [long]($output | Select-Object -Last 1)
}

$initialJobs = Get-RunningJobs
$expectedJobNames = @($initialJobs | Where-Object { $_.state -eq "RUNNING" } | ForEach-Object { $_.name })
if ($expectedJobNames.Count -eq 0) {
    throw "No running Flink jobs found"
}
$initialTaskManagers = Invoke-RestMethod -Uri "http://localhost:$flinkPort/taskmanagers" -TimeoutSec 10
$initialTaskManagerIds = @($initialTaskManagers.taskmanagers | ForEach-Object { $_.id })
if ($initialTaskManagerIds.Count -eq 0) {
    throw "No registered Flink TaskManager found"
}

$runId = "recovery-{0}-{1}" -f (Get-Date -Format "yyyyMMddHHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 6))
$prefix = "bench-$runId-"
$metadataPath = Join-Path $ResultDirectory "$runId-generator.json"
$stdoutPath = Join-Path $ResultDirectory "$runId-generator.out.log"
$stderrPath = Join-Path $ResultDirectory "$runId-generator.err.log"
$resultPath = Join-Path $ResultDirectory "$runId-result.json"

$arguments = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$generatorScript`"",
    "-Count", $Count,
    "-UserCount", 5000,
    "-ShopCount", 14,
    "-RatePerSecond", $RatePerSecond,
    "-RunId", $runId,
    "-OutputMetadataPath", "`"$metadataPath`""
)
$generator = Start-Process -FilePath "powershell.exe" `
    -ArgumentList $arguments `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath `
    -WindowStyle Hidden `
    -PassThru

$checkpointBefore = Get-DwdCompletedCheckpointCount
$checkpointDeadline = (Get-Date).AddSeconds(75)
Write-Host "Waiting for a new completed Checkpoint while load is running..."
while ((Get-Date) -lt $checkpointDeadline) {
    if ((Get-DwdCompletedCheckpointCount) -gt $checkpointBefore) {
        break
    }
    Start-Sleep -Seconds 2
}
if ((Get-DwdCompletedCheckpointCount) -le $checkpointBefore) {
    throw "No new completed Checkpoint was observed before recovery test"
}

$killEpochMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
Write-Host "Killing Flink TaskManager at $killEpochMs..."
$previousPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$killOutput = docker compose -f $composeFile kill flink-taskmanager 2>&1
$killExitCode = $LASTEXITCODE
$ErrorActionPreference = $previousPreference
$killOutput | ForEach-Object { Write-Host $_ }
if ($killExitCode -ne 0) {
    throw "Failed to kill Flink TaskManager"
}

$previousPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$restartOutput = docker compose -f $composeFile up -d flink-taskmanager 2>&1
$restartExitCode = $LASTEXITCODE
$ErrorActionPreference = $previousPreference
$restartOutput | ForEach-Object { Write-Host $_ }
if ($restartExitCode -ne 0) {
    throw "Failed to restart Flink TaskManager"
}

$deadline = (Get-Date).AddMinutes($TimeoutMinutes)
$jobsRecoveredEpochMs = 0L
$lagRecoveredEpochMs = 0L
$checkpointStableEpochMs = 0L
$maxLag = 0L

while ((Get-Date) -lt $deadline) {
    $nowEpochMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    try {
        $jobs = Get-RunningJobs
        $runningNames = @($jobs | Where-Object { $_.state -eq "RUNNING" } | ForEach-Object { $_.name })
        $allExpectedRunning = @($expectedJobNames | Where-Object { $_ -notin $runningNames }).Count -eq 0
        if ($allExpectedRunning -and $jobsRecoveredEpochMs -eq 0) {
            $taskmanagers = Invoke-RestMethod -Uri "http://localhost:$flinkPort/taskmanagers" -TimeoutSec 10
            $currentTaskManagerIds = @($taskmanagers.taskmanagers | ForEach-Object { $_.id })
            $newTaskManagerRegistered = @($currentTaskManagerIds | Where-Object { $_ -notin $initialTaskManagerIds }).Count -gt 0
            if ($newTaskManagerRegistered) {
                $jobsRecoveredEpochMs = $nowEpochMs
            }
        }

        if ($jobsRecoveredEpochMs -gt 0 -and $checkpointStableEpochMs -eq 0) {
            $jobsWithPostRecoveryCheckpoint = 0
            foreach ($expectedName in $expectedJobNames) {
                $runningJob = $jobs |
                    Where-Object { $_.name -eq $expectedName -and $_.state -eq "RUNNING" } |
                    Sort-Object -Property "start-time" -Descending |
                    Select-Object -First 1
                if ($null -ne $runningJob) {
                    $checkpointInfo = Invoke-RestMethod `
                        -Uri "http://localhost:$flinkPort/jobs/$($runningJob.jid)/checkpoints" `
                        -TimeoutSec 10
                    $hasPostRecoveryCheckpoint = @(
                        $checkpointInfo.history |
                            Where-Object {
                                $_.status -eq "COMPLETED" -and
                                $_.trigger_timestamp -gt $killEpochMs
                            }
                    ).Count -gt 0
                    if ($hasPostRecoveryCheckpoint) {
                        $jobsWithPostRecoveryCheckpoint++
                    }
                }
            }
            if ($jobsWithPostRecoveryCheckpoint -eq $expectedJobNames.Count) {
                $checkpointStableEpochMs = $nowEpochMs
            }
        }
    }
    catch {
        # JobManager can briefly reject requests while tasks reconnect.
    }

    $lag = Get-KafkaLag
    if ($null -ne $lag) {
        $maxLag = [Math]::Max($maxLag, [long]$lag)
        if ($jobsRecoveredEpochMs -gt 0 -and $lag -eq 0 -and $lagRecoveredEpochMs -eq 0) {
            $lagRecoveredEpochMs = $nowEpochMs
        }
    }

    if ($jobsRecoveredEpochMs -gt 0 -and
        $lagRecoveredEpochMs -gt 0 -and
        $checkpointStableEpochMs -gt 0 -and
        $generator.HasExited) {
        $visible = Get-DorisCount $prefix
        if ($visible -ge $Count) {
            break
        }
    }
    Start-Sleep -Seconds 2
}

if (-not $generator.HasExited) {
    Stop-Process -Id $generator.Id -Force -ErrorAction SilentlyContinue
    throw "Generator did not finish during recovery test"
}
$generator.WaitForExit()
if (-not (Test-Path $metadataPath)) {
    $errorText = if (Test-Path $stderrPath) { Get-Content -Raw $stderrPath } else { "" }
    throw "Generator failed during recovery test: $errorText"
}

$finalVisibleCount = Get-DorisCount $prefix
$finalLag = Get-KafkaLag
$finalJobs = Get-RunningJobs | Where-Object { $_.state -eq "RUNNING" }
$restoredCheckpointJobs = 0
foreach ($job in $finalJobs) {
    try {
        $checkpointInfo = Invoke-RestMethod -Uri "http://localhost:$flinkPort/jobs/$($job.jid)/checkpoints" -TimeoutSec 10
        if ([int]$checkpointInfo.counts.restored -gt 0) {
            $restoredCheckpointJobs++
        }
    }
    catch {
        # Recovery completion is decided by job state, lag and row count.
    }
}
$result = [ordered]@{
    run_id = $runId
    input_count = $Count
    requested_rate_per_second = $RatePerSecond
    expected_running_jobs = $expectedJobNames.Count
    kill_epoch_ms = $killEpochMs
    jobs_recovered_epoch_ms = $jobsRecoveredEpochMs
    job_recovery_duration_ms = if ($jobsRecoveredEpochMs -gt 0) { $jobsRecoveredEpochMs - $killEpochMs } else { $null }
    lag_recovered_epoch_ms = $lagRecoveredEpochMs
    lag_recovery_duration_ms = if ($lagRecoveredEpochMs -gt 0) { $lagRecoveredEpochMs - $killEpochMs } else { $null }
    checkpoint_stable_epoch_ms = $checkpointStableEpochMs
    checkpoint_stable_duration_ms = if ($checkpointStableEpochMs -gt 0) { $checkpointStableEpochMs - $killEpochMs } else { $null }
    maximum_kafka_lag = $maxLag
    final_kafka_lag = $finalLag
    doris_visible_rows = $finalVisibleCount
    data_loss_count = [Math]::Max(0, $Count - $finalVisibleCount)
    jobs_with_restored_checkpoint = $restoredCheckpointJobs
    completed = ($jobsRecoveredEpochMs -gt 0 -and
                 $lagRecoveredEpochMs -gt 0 -and
                 $checkpointStableEpochMs -gt 0 -and
                 $finalVisibleCount -eq $Count)
}
$result | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 -Path $resultPath
Write-Host ($result | ConvertTo-Json -Depth 6)

if (-not $result.completed) {
    throw "Recovery test did not meet completion criteria. Inspect $resultPath"
}
