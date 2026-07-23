param(
    [ValidatePattern('^[A-Za-z0-9_-]+$')]
    [string]$DeploymentId = ""
)

$ErrorActionPreference = "Stop"

$composeFile = Join-Path $PSScriptRoot "..\docker-compose.yml"
if ([string]::IsNullOrWhiteSpace($DeploymentId)) {
    $DeploymentId = "{0}_{1}" -f (Get-Date -Format "yyyyMMddHHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 8))
}

function Submit-FlinkSql([string]$file, [string]$name) {
    $sourcePath = Join-Path $PSScriptRoot "..\flink-sql\$file"
    $remoteFile = "/tmp/hmdp-$DeploymentId-$file"
    $tempFile = Join-Path ([System.IO.Path]::GetTempPath()) "hmdp-$DeploymentId-$file"

    Write-Host "Submitting $name with deployment ID $DeploymentId..."
    try {
        $sql = Get-Content -Raw -Encoding UTF8 $sourcePath
        $labelPattern = "('sink\.label-prefix'\s*=\s*')([^']+)(')"
        $sql = [regex]::Replace($sql, $labelPattern, {
            param($match)
            return $match.Groups[1].Value +
                $match.Groups[2].Value + "_" + $DeploymentId +
                $match.Groups[3].Value
        })
        [System.IO.File]::WriteAllText($tempFile, $sql, (New-Object System.Text.UTF8Encoding($false)))

        docker compose -f $composeFile cp $tempFile "flink-jobmanager:$remoteFile"
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to copy rendered SQL for $name"
        }

        $previousPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        $output = docker compose -f $composeFile exec -T flink-jobmanager ./bin/sql-client.sh -f $remoteFile 2>&1
        $exitCode = $LASTEXITCODE
        $ErrorActionPreference = $previousPreference
        $output | ForEach-Object { Write-Host $_ }
        if ($exitCode -ne 0 -or ($output -join "`n") -match "\[ERROR\]") {
            throw "$name submission failed"
        }
    }
    finally {
        docker compose -f $composeFile exec -T flink-jobmanager rm -f $remoteFile 2>$null
        Remove-Item -LiteralPath $tempFile -Force -ErrorAction SilentlyContinue
    }
}

Submit-FlinkSql "10-dwd.sql" "DWD cleaning and CDC job"
Submit-FlinkSql "11-quality-behavior.sql" "invalid behavior quality job"
Submit-FlinkSql "12-quality-order.sql" "order consistency quality job"
Submit-FlinkSql "13-quality-voucher.sql" "voucher consistency quality job"
Submit-FlinkSql "14-quality-duplicate.sql" "duplicate event quality job"
Submit-FlinkSql "20-dws.sql" "DWS aggregation job"

$flinkPort = if ($env:FLINK_UI_PORT) { $env:FLINK_UI_PORT } else { "18081" }
Write-Host "Jobs submitted with deployment ID $DeploymentId. Inspect them at http://localhost:$flinkPort"
