$ErrorActionPreference = "Stop"
$composeFile = Join-Path $PSScriptRoot "..\docker-compose.yml"
$flinkPort = if ($env:FLINK_UI_PORT) { $env:FLINK_UI_PORT } else { "18081" }

docker compose -f $composeFile ps

Write-Host "`nKafka consumer groups:"
docker compose -f $composeFile exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server kafka:29092 --all-groups --describe

Write-Host "`nFlink jobs:"
$jobs = Invoke-RestMethod -Uri "http://localhost:$flinkPort/jobs/overview"
$jobs.jobs | Select-Object jid, name, state, start-time, duration | Format-Table

Write-Host "`nDoris backends:"
docker compose -f $composeFile exec -T mysql mysql -hdoris-fe -P9030 -uroot `
    -e "SELECT BackendId, Host, Alive, TabletNum FROM backends();"
