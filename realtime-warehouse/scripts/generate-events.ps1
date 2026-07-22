param(
    [ValidateRange(1, 1000000)]
    [int]$Count = 1000,
    [ValidateRange(1, 10000)]
    [int]$UserCount = 100,
    [ValidateRange(1, 1000)]
    [int]$ShopCount = 14
)

$ErrorActionPreference = "Stop"
$composeFile = Join-Path $PSScriptRoot "..\docker-compose.yml"
$start = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$events = New-Object System.Collections.Generic.List[string]

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
    $event = [ordered]@{
        eventId = [guid]::NewGuid().ToString()
        eventType = $eventType
        userId = $userId
        deviceId = "load-device-$userId"
        shopId = $shopId
        blogId = if ($eventType -like "BLOG*") { $blogId } else { $null }
        voucherId = if ($eventType -in @("VOUCHER_EXPOSURE", "SECKILL_REQUEST")) { $voucherId } else { $null }
        result = if ($eventType -eq "SECKILL_REQUEST") { "ACCEPTED" } else { $null }
        eventTime = $now
        ingestTime = $now
        properties = @{}
    }
    $events.Add(($event | ConvertTo-Json -Compress))
}

$events | docker compose -f $composeFile exec -T kafka /opt/kafka/bin/kafka-console-producer.sh `
    --bootstrap-server kafka:29092 --topic ods_behavior_event
if ($LASTEXITCODE -ne 0) { throw "Kafka event generation failed" }

$elapsed = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() - $start
Write-Host "Produced $Count events in ${elapsed}ms. This is generator time, not end-to-end latency."
