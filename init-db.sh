#!/bin/bash
set -e  

exec > /var/log/clickhouse-server/init.log 2>&1
echo "Starting init-db.sh at $(date)"

ls -l /init-sql-scripts/ || { echo "Error: /init-sql-scripts not mounted!"; exit 1; }
if [ -z "$(ls /init-sql-scripts/*.sql 2>/dev/null)" ]; then
    echo "Error: No *.sql files in /init-sql-scripts!"
    exit 1
fi

timeout=0
while ! clickhouse-client -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT 1" &> /dev/null; do
    echo "Waiting for Clickhouse... ($timeout/120)"
    sleep 1
    timeout=$((timeout + 1))
    if [ $timeout -ge 120 ]; then
        echo "Error: Timeout waiting for Clickhouse!"
        exit 1
    fi
done

echo "Clickhouse is ready."

clickhouse-client -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DB" || { echo "Error creating database!"; exit 1; }

for script in /init-sql-scripts/*.sql; do
    echo "Executing $script..."
    clickhouse-client --database "$CLICKHOUSE_DB" -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery < "$script" || { echo "Error executing $script!"; exit 1; }
done

echo "All scripts executed successfully."
