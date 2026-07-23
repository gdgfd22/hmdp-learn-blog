param(
    [ValidateRange(1, 10)]
    [int]$Rounds = 3,
    [ValidateRange(100, 1000000)]
    [int]$CountPerRound = 30000,
    [ValidateRange(1, 100000)]
    [int]$RatePerSecond = 1000,
    [ValidateRange(250, 10000)]
    [int]$PollIntervalMs = 1000,
    [ValidateRange(1, 60)]
    [int]$DrainTimeoutMinutes = 10,
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

function Get-Percentile([double[]]$Values, [double]$Percentile) {
    if ($null -eq $Values -or $Values.Count -eq 0) {
        return $null
    }
    $sorted = @($Values | Sort-Object)
    $index = [Math]::Ceiling($Percentile * $sorted.Count) - 1
    $index = [Math]::Max(0, [Math]::Min($index, $sorted.Count - 1))
    return [Math]::Round($sorted[$index], 2)
}

function Get-DorisRunStats([string]$Prefix) {
    $safePrefix = $Prefix.Replace("'", "''")
    $sql = @"
SELECT COUNT(*),
       COALESCE(UNIX_TIMESTAMP(MAX(event_time)) * 1000, 0)
FROM hmdp_analytics.dwd_user_behavior_detail
WHERE event_id LIKE '$safePrefix%';
"@
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = docker compose -f $composeFile exec -T mysql `
        mysql -hdoris-fe -P9030 -uroot -N -B -e $sql 2>$null
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousPreference
    if ($exitCode -ne 0) {
        throw "Failed to query Doris benchmark rows"
    }
    $parts = ($output | Select-Object -Last 1) -split "\s+"
    return [pscustomobject]@{
        Count = [long]$parts[0]
        MaxEventEpochMs = [long][decimal]$parts[1]
    }
}

function Get-DorisRunEvents([string]$Prefix) {
    $safePrefix = $Prefix.Replace("'", "''")
    $sql = @"
SELECT event_id, UNIX_TIMESTAMP(event_time) * 1000
FROM hmdp_analytics.dwd_user_behavior_detail
WHERE event_id LIKE '$safePrefix%';
"@
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = docker compose -f $composeFile exec -T mysql `
        mysql -hdoris-fe -P9030 -uroot -N -B -e $sql 2>$null
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousPreference
    if ($exitCode -ne 0) {
        throw "Failed to query Doris benchmark event timestamps"
    }

    $rows = New-Object System.Collections.Generic.List[object]
    foreach ($line in $output) {
        $parts = $line -split "\t"
        if ($parts.Count -ge 2) {
            $rows.Add([pscustomobject]@{
                EventId = $parts[0]
                EventEpochMs = [long][decimal]$parts[1]
            })
        }
    }
    return $rows
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

    $lag = 0L
    $matched = $false
    foreach ($line in $output) {
        $parts = $line.Trim() -split "\s+"
        if ($parts.Count -ge 6 -and $parts[0] -eq "hmdp-dwd-behavior" -and $parts[1] -eq "ods_behavior_event") {
            $parsedLag = 0L
            if ([long]::TryParse($parts[5], [ref]$parsedLag)) {
                $lag += $parsedLag
                $matched = $true
            }
        }
    }
    if (-not $matched) {
        return $null
    }
    return $lag
}

function Get-FlinkJob([string]$Name) {
    $overview = Invoke-RestMethod -Uri "http://localhost:$flinkPort/jobs/overview" -TimeoutSec 10
    return $overview.jobs |
        Where-Object { $_.name -eq $Name -and $_.state -eq "RUNNING" } |
        Sort-Object -Property "start-time" -Descending |
        Select-Object -First 1
}

function Get-FlinkSourceRecordsOut([string]$JobId) {
    if ([string]::IsNullOrWhiteSpace($JobId)) {
        return $null
    }
    try {
        $job = Invoke-RestMethod -Uri "http://localhost:$flinkPort/jobs/$JobId" -TimeoutSec 10
        $source = $job.vertices |
            Where-Object { $_.name -match "ods_behavior_event" -and $_.name -match "Source" } |
            Select-Object -First 1
        if ($null -eq $source) {
            return $null
        }
        $availableMetrics = Invoke-RestMethod `
            -Uri "http://localhost:$flinkPort/jobs/$JobId/vertices/$($source.id)/subtasks/0/metrics" `
            -TimeoutSec 10
        $sourceMetric = $availableMetrics |
            Where-Object { $_.id -match "^Source.*ods_behavior_event.*\.numRecordsOut$" } |
            Select-Object -First 1
        if ($null -eq $sourceMetric) {
            return $null
        }
        $encodedMetric = [uri]::EscapeDataString($sourceMetric.id)
        $metrics = Invoke-RestMethod `
            -Uri "http://localhost:$flinkPort/jobs/$JobId/vertices/$($source.id)/subtasks/metrics?get=$encodedMetric&agg=sum" `
            -TimeoutSec 10
        $metric = $metrics | Where-Object { $_.id -eq $sourceMetric.id } | Select-Object -First 1
        if ($null -eq $metric) {
            return $null
        }
        return [long][double]$metric.sum
    }
    catch {
        return $null
    }
}

function Get-CheckpointStats([string]$JobId, [long]$FromEpochMs, [long]$ToEpochMs) {
    if ([string]::IsNullOrWhiteSpace($JobId)) {
        return [pscustomobject]@{ Count = 0; Failed = 0; AverageDurationMs = $null; P95DurationMs = $null; AverageSizeBytes = $null }
    }
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:$flinkPort/jobs/$JobId/checkpoints" -TimeoutSec 10
        $history = @($response.history | Where-Object {
            $_.trigger_timestamp -ge $FromEpochMs -and
            $_.trigger_timestamp -le $ToEpochMs
        })
        $completed = @($history | Where-Object { $_.status -eq "COMPLETED" })
        $failed = @($history | Where-Object { $_.status -eq "FAILED" })
        $durations = [double[]]@($completed | ForEach-Object { [double]$_.end_to_end_duration })
        $sizes = [double[]]@($completed | ForEach-Object { [double]$_.state_size })
        return [pscustomobject]@{
            Count = $completed.Count
            Failed = $failed.Count
            AverageDurationMs = if ($durations.Count -gt 0) { [Math]::Round(($durations | Measure-Object -Average).Average, 2) } else { $null }
            P95DurationMs = Get-Percentile $durations 0.95
            AverageSizeBytes = if ($sizes.Count -gt 0) { [Math]::Round(($sizes | Measure-Object -Average).Average, 2) } else { $null }
        }
    }
    catch {
        return [pscustomobject]@{ Count = 0; Failed = 0; AverageDurationMs = $null; P95DurationMs = $null; AverageSizeBytes = $null }
    }
}

function Start-Generator([string]$RunId, [string]$MetadataPath, [string]$StdoutPath, [string]$StderrPath) {
    $arguments = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", "`"$generatorScript`"",
        "-Count", $CountPerRound,
        "-UserCount", 5000,
        "-ShopCount", 14,
        "-RatePerSecond", $RatePerSecond,
        "-RunId", $RunId,
        "-OutputMetadataPath", "`"$MetadataPath`""
    )
    return Start-Process -FilePath "powershell.exe" `
        -ArgumentList $arguments `
        -RedirectStandardOutput $StdoutPath `
        -RedirectStandardError $StderrPath `
        -WindowStyle Hidden `
        -PassThru
}

$dwdJob = Get-FlinkJob "hmdp-dwd-clean-and-cdc"
if ($null -eq $dwdJob -or $dwdJob.state -ne "RUNNING") {
    throw "DWD Flink job is not running. Start the stack and submit jobs before benchmarking."
}

$allResults = New-Object System.Collections.Generic.List[object]
$testStartedAt = Get-Date

for ($round = 1; $round -le $Rounds; $round++) {
    $runId = "{0}-r{1}-{2}" -f (Get-Date -Format "yyyyMMddHHmmss"), $round, ([guid]::NewGuid().ToString("N").Substring(0, 6))
    $prefix = "bench-$runId-"
    $metadataPath = Join-Path $ResultDirectory "$runId-generator.json"
    $stdoutPath = Join-Path $ResultDirectory "$runId-generator.out.log"
    $stderrPath = Join-Path $ResultDirectory "$runId-generator.err.log"
    $samplesPath = Join-Path $ResultDirectory "$runId-samples.csv"
    $samples = New-Object System.Collections.Generic.List[object]
    $observedEventIds = New-Object 'System.Collections.Generic.HashSet[string]'
    $endToEndLatencies = New-Object System.Collections.Generic.List[double]

    Write-Host "`nRound $round/$Rounds - runId=$runId, count=$CountPerRound, requestedRate=$RatePerSecond events/s"
    $roundStartEpochMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $sourceRecordsBefore = Get-FlinkSourceRecordsOut $dwdJob.jid
    Write-Host "Flink source counter before round: $sourceRecordsBefore"
    $generator = Start-Generator $runId $metadataPath $stdoutPath $stderrPath
    $deadline = (Get-Date).AddMinutes($DrainTimeoutMinutes)
    $lastVisibleCount = 0L
    $firstVisibleEpochMs = 0L
    $completionEpochMs = 0L
    $maxLag = 0L

    while ((Get-Date) -lt $deadline) {
        $nowEpochMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        $doris = Get-DorisRunStats $prefix
        $lag = Get-KafkaLag
        if ($null -ne $lag) {
            $maxLag = [Math]::Max($maxLag, [long]$lag)
        }

        $visibilityStalenessMs = $null
        if ($doris.Count -gt 0 -and $doris.Count -ne $lastVisibleCount) {
            if ($firstVisibleEpochMs -eq 0) {
                $firstVisibleEpochMs = $nowEpochMs
            }
            if ($doris.MaxEventEpochMs -gt 0) {
                $visibilityStalenessMs = [Math]::Max(0, $nowEpochMs - $doris.MaxEventEpochMs)
            }
            $visibleEvents = Get-DorisRunEvents $prefix
            $observedEpochMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
            foreach ($visibleEvent in $visibleEvents) {
                if ($observedEventIds.Add($visibleEvent.EventId)) {
                    $endToEndLatencies.Add([Math]::Max(0, $observedEpochMs - $visibleEvent.EventEpochMs))
                }
            }
        }

        $samples.Add([pscustomobject]@{
            timestamp_epoch_ms = $nowEpochMs
            visible_count = $doris.Count
            kafka_lag = $lag
            visibility_staleness_ms = $visibilityStalenessMs
        })
        $lastVisibleCount = $doris.Count

        if ($generator.HasExited) {
            $generator.WaitForExit()
            if (-not (Test-Path $metadataPath) -and ($nowEpochMs - $roundStartEpochMs) -gt 10000) {
                $errorText = if (Test-Path $stderrPath) { Get-Content -Raw $stderrPath } else { "" }
                throw "Generator exited without producing metadata: $errorText"
            }
        }

        if ($generator.HasExited -and $doris.Count -ge $CountPerRound) {
            $completionEpochMs = $nowEpochMs
            break
        }
        Start-Sleep -Milliseconds $PollIntervalMs
    }

    if (-not $generator.HasExited) {
        Stop-Process -Id $generator.Id -Force -ErrorAction SilentlyContinue
        throw "Generator did not finish before timeout"
    }
    $generator.WaitForExit()
    if ($completionEpochMs -eq 0) {
        throw "Doris did not expose all $CountPerRound rows for run $runId before timeout"
    }
    if (-not (Test-Path $metadataPath)) {
        throw "Generator metadata was not created for run $runId"
    }

    $metadata = Get-Content -Raw -Encoding UTF8 $metadataPath | ConvertFrom-Json
    $samples | Export-Csv -Encoding UTF8 -NoTypeInformation -Path $samplesPath
    $sourceRecordsAfter = Get-FlinkSourceRecordsOut $dwdJob.jid
    if ($null -ne $sourceRecordsBefore) {
        $metricDeadline = (Get-Date).AddSeconds(15)
        while ($null -ne $sourceRecordsAfter -and
               $sourceRecordsAfter -lt ($sourceRecordsBefore + $CountPerRound) -and
               (Get-Date) -lt $metricDeadline) {
            Start-Sleep -Seconds 1
            $sourceRecordsAfter = Get-FlinkSourceRecordsOut $dwdJob.jid
        }
    }
    Write-Host "Flink source counter after round: $sourceRecordsAfter"
    $checkpointStats = Get-CheckpointStats $dwdJob.jid $roundStartEpochMs $completionEpochMs

    $visibilitySamples = [double[]]@(
        $samples |
            Where-Object { $null -ne $_.visibility_staleness_ms } |
            ForEach-Object { [double]$_.visibility_staleness_ms }
    )
    $endToEndLatencyValues = [double[]]@($endToEndLatencies | ForEach-Object { $_ })
    $positiveIntervals = New-Object System.Collections.Generic.List[double]
    for ($i = 1; $i -lt $samples.Count; $i++) {
        $rowDelta = [long]$samples[$i].visible_count - [long]$samples[$i - 1].visible_count
        $timeDeltaMs = [long]$samples[$i].timestamp_epoch_ms - [long]$samples[$i - 1].timestamp_epoch_ms
        if ($rowDelta -gt 0 -and $timeDeltaMs -gt 0) {
            $positiveIntervals.Add($rowDelta * 1000.0 / $timeDeltaMs)
        }
    }

    $flinkRecords = if ($null -ne $sourceRecordsBefore -and $null -ne $sourceRecordsAfter) {
        [Math]::Max(0, $sourceRecordsAfter - $sourceRecordsBefore)
    } else {
        $null
    }
    $roundElapsedMs = [Math]::Max(1, $completionEpochMs - $roundStartEpochMs)
    $result = [ordered]@{
        round = $round
        run_id = $runId
        input_count = $CountPerRound
        requested_rate_per_second = $RatePerSecond
        actual_producer_rate_per_second = [double]$metadata.actual_rate_per_second
        flink_source_records_before = $sourceRecordsBefore
        flink_source_records_after = $sourceRecordsAfter
        flink_source_records = $flinkRecords
        flink_source_throughput_per_second = if ($null -ne $flinkRecords) { [Math]::Round($flinkRecords * 1000.0 / [Math]::Max(1, [long]$metadata.producer_elapsed_ms), 2) } else { $null }
        doris_rows_visible = $lastVisibleCount
        doris_effective_throughput_per_second = [Math]::Round($lastVisibleCount * 1000.0 / $roundElapsedMs, 2)
        doris_peak_observed_throughput_per_second = if ($positiveIntervals.Count -gt 0) { [Math]::Round(($positiveIntervals | Measure-Object -Maximum).Maximum, 2) } else { $null }
        freshest_visible_staleness_p50_ms = Get-Percentile $visibilitySamples 0.50
        freshest_visible_staleness_p95_ms = Get-Percentile $visibilitySamples 0.95
        end_to_end_visibility_observations = $endToEndLatencyValues.Count
        end_to_end_visibility_p50_ms = Get-Percentile $endToEndLatencyValues 0.50
        end_to_end_visibility_p95_ms = Get-Percentile $endToEndLatencyValues 0.95
        end_to_end_visibility_max_ms = if ($endToEndLatencyValues.Count -gt 0) { [Math]::Round(($endToEndLatencyValues | Measure-Object -Maximum).Maximum, 2) } else { $null }
        final_drain_latency_ms = [Math]::Max(0, $completionEpochMs - [long]$metadata.producer_end_epoch_ms)
        max_kafka_lag = $maxLag
        checkpoint_completed = $checkpointStats.Count
        checkpoint_failed = $checkpointStats.Failed
        checkpoint_average_duration_ms = $checkpointStats.AverageDurationMs
        checkpoint_p95_duration_ms = $checkpointStats.P95DurationMs
        checkpoint_average_size_bytes = $checkpointStats.AverageSizeBytes
        round_elapsed_ms = $roundElapsedMs
        started_at = [DateTimeOffset]::FromUnixTimeMilliseconds($roundStartEpochMs).ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss.fff zzz")
        completed_at = [DateTimeOffset]::FromUnixTimeMilliseconds($completionEpochMs).ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss.fff zzz")
    }
    $allResults.Add([pscustomobject]$result)
    Write-Host ("Round {0} completed: producer={1} events/s, Flink source={2} events/s, Doris visible={3}, lagMax={4}, end-to-end P95 upper bound={5} ms" -f `
        $round, $result.actual_producer_rate_per_second, $result.flink_source_throughput_per_second, `
        $result.doris_rows_visible, $result.max_kafka_lag, $result.end_to_end_visibility_p95_ms)
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$jsonPath = Join-Path $ResultDirectory "benchmark-$timestamp.json"
$csvPath = Join-Path $ResultDirectory "benchmark-$timestamp.csv"
$allResults | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $jsonPath
$allResults | Export-Csv -Encoding UTF8 -NoTypeInformation -Path $csvPath

$numericResults = @($allResults | ForEach-Object { $_ })
$averageProducer = [Math]::Round(($numericResults.actual_producer_rate_per_second | Measure-Object -Average).Average, 2)
$averageFlink = if (@($numericResults.flink_source_throughput_per_second | Where-Object { $null -ne $_ }).Count -gt 0) {
    [Math]::Round(($numericResults.flink_source_throughput_per_second | Where-Object { $null -ne $_ } | Measure-Object -Average).Average, 2)
} else { $null }
$averageDoris = [Math]::Round(($numericResults.doris_effective_throughput_per_second | Measure-Object -Average).Average, 2)
$p95Visibility = Get-Percentile ([double[]]@($numericResults.end_to_end_visibility_p95_ms)) 0.95
$averageCheckpoint = [Math]::Round(($numericResults.checkpoint_average_duration_ms | Where-Object { $null -ne $_ } | Measure-Object -Average).Average, 2)
$maxLagOverall = ($numericResults.max_kafka_lag | Measure-Object -Maximum).Maximum
$averageDrain = [Math]::Round(($numericResults.final_drain_latency_ms | Measure-Object -Average).Average, 2)

$summary = [ordered]@{
    test_started_at = $testStartedAt.ToString("yyyy-MM-dd HH:mm:ss zzz")
    test_finished_at = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss zzz")
    rounds = $Rounds
    count_per_round = $CountPerRound
    total_events = $Rounds * $CountPerRound
    requested_rate_per_second = $RatePerSecond
    average_actual_producer_rate_per_second = $averageProducer
    average_flink_source_throughput_per_second = $averageFlink
    average_doris_effective_throughput_per_second = $averageDoris
    end_to_end_visibility_p95_upper_bound_across_rounds_ms = $p95Visibility
    average_final_drain_latency_ms = $averageDrain
    maximum_kafka_lag = $maxLagOverall
    average_checkpoint_duration_ms = $averageCheckpoint
    result_json = $jsonPath
    result_csv = $csvPath
}
$summaryPath = Join-Path $ResultDirectory "benchmark-$timestamp-summary.json"
$summary | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $summaryPath

Write-Host "`nBenchmark completed."
Write-Host ($summary | ConvertTo-Json -Depth 8)
