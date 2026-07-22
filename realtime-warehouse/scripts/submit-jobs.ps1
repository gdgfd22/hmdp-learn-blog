$ErrorActionPreference = "Stop"

$composeFile = Join-Path $PSScriptRoot "..\docker-compose.yml"

function Submit-FlinkSql([string]$file, [string]$name) {
    Write-Host "Submitting $name..."
    $output = docker compose -f $composeFile exec -T flink-jobmanager ./bin/sql-client.sh -f "/opt/flink/sql/$file" 2>&1
    $output | ForEach-Object { Write-Host $_ }
    if ($LASTEXITCODE -ne 0 -or ($output -join "`n") -match "\[ERROR\]") {
        throw "$name submission failed"
    }
}

Submit-FlinkSql "10-dwd.sql" "DWD cleaning and CDC job"
Submit-FlinkSql "11-quality-behavior.sql" "invalid behavior quality job"
Submit-FlinkSql "12-quality-order.sql" "order consistency quality job"
Submit-FlinkSql "13-quality-voucher.sql" "voucher consistency quality job"
Submit-FlinkSql "14-quality-duplicate.sql" "duplicate event quality job"
Submit-FlinkSql "20-dws.sql" "DWS aggregation job"

Write-Host "Jobs submitted. Inspect them at http://localhost:8081"
