#!/usr/bin/env bash
set -euo pipefail

until mysql -h doris-fe -P 9030 -u root -e "SELECT 1" >/dev/null 2>&1; do
  sleep 5
done

until mysql -h doris-fe -P 9030 -u root -N -B -e "SHOW BACKENDS" 2>/dev/null \
  | awk -F '\t' '$10 == "true" { found = 1 } END { exit !found }'; do
  sleep 5
done

mysql -h doris-fe -P 9030 -u root < /opt/hmdp/01-schema.sql
