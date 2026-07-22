param(
    [datetime]$Date = (Get-Date).Date
)

$ErrorActionPreference = "Stop"
$composeFile = Join-Path $PSScriptRoot "..\docker-compose.yml"
$metricDate = $Date.ToString("yyyy-MM-dd")
$mysqlPassword = if ($env:MYSQL_ROOT_PASSWORD) { $env:MYSQL_ROOT_PASSWORD } else { "1234" }

$businessSql = @"
SELECT COUNT(*) AS order_count,
       SUM(CASE WHEN pay_time IS NOT NULL THEN 1 ELSE 0 END) AS paid_order_count,
       COALESCE(SUM(CASE WHEN pay_time IS NOT NULL THEN pay_amount ELSE 0 END), 0) AS gmv,
       COALESCE(SUM(refund_amount), 0) AS refund_amount
FROM hmdp.tb_voucher_order
WHERE create_time >= '$metricDate 00:00:00'
  AND create_time < DATE_ADD('$metricDate 00:00:00', INTERVAL 1 DAY);
"@

$analyticsSql = @"
SELECT COALESCE(SUM(order_count), 0),
       COALESCE(SUM(paid_order_count), 0),
       COALESCE(SUM(gmv), 0),
       COALESCE(SUM(refund_amount), 0)
FROM hmdp_analytics.dws_order_day
WHERE metric_date = '$metricDate';
"@

$business = docker compose -f $composeFile exec -T mysql mysql -uroot "-p$mysqlPassword" -N -e $businessSql
if ($LASTEXITCODE -ne 0) { throw "MySQL reconciliation query failed" }
$analytics = docker compose -f $composeFile exec -T mysql mysql -hdoris-fe -P9030 -uroot -N -e $analyticsSql
if ($LASTEXITCODE -ne 0) { throw "Doris reconciliation query failed" }

Write-Host "Metric date: $metricDate"
Write-Host "MySQL (orders, paid, GMV cents, refunds cents): $business"
Write-Host "Doris (orders, paid, GMV cents, refunds cents): $analytics"
if (($business -replace '\s+', ',') -eq ($analytics -replace '\s+', ',')) {
    Write-Host "Reconciliation passed."
    exit 0
}

Write-Warning "Reconciliation mismatch. Check CDC lag, running checkpoints and invalid-order quality records."
exit 1
