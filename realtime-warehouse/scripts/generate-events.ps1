param(
    [ValidateRange(1, 1000000)]
    [int]$Count = 1000,
    [ValidateRange(1, 100000)]
    [int]$UserCount = 100,
    [ValidateRange(1, 1000)]
    [int]$ShopCount = 14,
    [ValidateRange(0, 100000)]
    [int]$RatePerSecond = 0,
    [ValidatePattern('^[A-Za-z0-9_-]+$')]
    [string]$RunId = "",
    [string]$OutputMetadataPath = ""
)

$ErrorActionPreference = "Stop"
$composeFile = (Resolve-Path (Join-Path $PSScriptRoot "..\docker-compose.yml")).Path

if ([string]::IsNullOrWhiteSpace($RunId)) {
    $RunId = "{0}-{1}" -f (Get-Date -Format "yyyyMMddHHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 8))
}

function Escape-JsonString([string]$value) {
    if ($null -eq $value) {
        return ""
    }
    return $value.Replace('\', '\\').Replace('"', '\"')
}

$dockerProcess = New-Object System.Diagnostics.Process
$dockerProcess.StartInfo = New-Object System.Diagnostics.ProcessStartInfo
$dockerProcess.StartInfo.FileName = "docker"
$dockerProcess.StartInfo.Arguments = "compose -f `"$composeFile`" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic ods_behavior_event"
$dockerProcess.StartInfo.UseShellExecute = $false
$dockerProcess.StartInfo.CreateNoWindow = $true
$dockerProcess.StartInfo.RedirectStandardInput = $true
$dockerProcess.StartInfo.RedirectStandardOutput = $true
$dockerProcess.StartInfo.RedirectStandardError = $true

$wallClock = [System.Diagnostics.Stopwatch]::StartNew()
$started = $dockerProcess.Start()
if (-not $started) {
    throw "Failed to start Kafka console producer"
}

$firstEventTime = 0L
$lastEventTime = 0L
$producerStartTime = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()

try {
    for ($i = 0; $i -lt $Count; $i++) {
        $userId = 1 + ($i % $UserCount)
        $shopId = 1 + ($i % $ShopCount)
        $blogId = 4 + ($i % 4)
        $voucherId = 1 + ($i % 4)
        $eventType = switch ($i % 6) {
            0 { "SHOP_VIEW" }
            1 { "BLOG_VIEW" }
            2 { "BLOG_LIKE" }
            3 { "VOUCHER_EXPOSURE" }
            4 { "SECKILL_REQUEST" }
            default { "FOLLOW" }
        }

        $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        if ($firstEventTime -eq 0) {
            $firstEventTime = $now
        }
        $lastEventTime = $now
        $eventId = "bench-$RunId-$i"
        $blogJson = if ($eventType -like "BLOG*") { [string]$blogId } else { "null" }
        $voucherJson = if ($eventType -in @("VOUCHER_EXPOSURE", "SECKILL_REQUEST")) { [string]$voucherId } else { "null" }
        $resultJson = if ($eventType -eq "SECKILL_REQUEST") { '"ACCEPTED"' } else { "null" }

        $json = '{"event_id":"' + (Escape-JsonString $eventId) +
            '","event_type":"' + $eventType +
            '","user_id":' + $userId +
            ',"device_id":"load-device-' + $userId +
            '","shop_id":' + $shopId +
            ',"blog_id":' + $blogJson +
            ',"voucher_id":' + $voucherJson +
            ',"order_id":null,"result":' + $resultJson +
            ',"event_time":' + $now +
            ',"ingest_time":' + $now +
            ',"properties":{"benchmark_run_id":"' + (Escape-JsonString $RunId) +
            '","benchmark_sequence":"' + $i + '"}}'

        $dockerProcess.StandardInput.WriteLine($json)
        if (($i + 1) % 200 -eq 0) {
            $dockerProcess.StandardInput.Flush()
        }

        if ($RatePerSecond -gt 0) {
            $expectedElapsedMs = (($i + 1) * 1000.0) / $RatePerSecond
            $sleepMs = [int][Math]::Floor($expectedElapsedMs - $wallClock.Elapsed.TotalMilliseconds)
            if ($sleepMs -gt 0) {
                Start-Sleep -Milliseconds $sleepMs
            }
        }
    }
}
finally {
    $dockerProcess.StandardInput.Flush()
    $dockerProcess.StandardInput.Close()
}

$dockerProcess.WaitForExit()
$producerEndTime = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$stderr = $dockerProcess.StandardError.ReadToEnd()
$stdout = $dockerProcess.StandardOutput.ReadToEnd()
if ($dockerProcess.ExitCode -ne 0) {
    throw "Kafka producer failed with exit code $($dockerProcess.ExitCode): $stderr $stdout"
}

$elapsedMs = [Math]::Max(1, $producerEndTime - $producerStartTime)
$metadata = [ordered]@{
    run_id = $RunId
    event_id_prefix = "bench-$RunId-"
    count = $Count
    requested_rate_per_second = $RatePerSecond
    actual_rate_per_second = [Math]::Round($Count * 1000.0 / $elapsedMs, 2)
    producer_start_epoch_ms = $producerStartTime
    first_event_epoch_ms = $firstEventTime
    last_event_epoch_ms = $lastEventTime
    producer_end_epoch_ms = $producerEndTime
    producer_elapsed_ms = $elapsedMs
}

$metadataJson = $metadata | ConvertTo-Json -Depth 4
if (-not [string]::IsNullOrWhiteSpace($OutputMetadataPath)) {
    $metadataDirectory = Split-Path -Parent $OutputMetadataPath
    if (-not [string]::IsNullOrWhiteSpace($metadataDirectory)) {
        New-Item -ItemType Directory -Path $metadataDirectory -Force | Out-Null
    }
    Set-Content -Encoding UTF8 -Path $OutputMetadataPath -Value $metadataJson
}

Write-Output ($metadata | ConvertTo-Json -Depth 4 -Compress)
