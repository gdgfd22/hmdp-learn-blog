#!/usr/bin/env bash
set -euo pipefail

until mysql -h doris-fe -P 9030 -u root -e "SELECT 1" >/dev/null 2>&1; do
  sleep 5
done

mysql -h doris-fe -P 9030 -u root < /opt/hmdp/01-schema.sql
