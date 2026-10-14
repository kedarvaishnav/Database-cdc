#!/bin/bash
set -euo pipefail

WAREHOUSE_BASE="${HOME}/warehouse"
HIVE_METASTORE="${WAREHOUSE_BASE}/hive-metastore"
SPARK_WAREHOUSE="${WAREHOUSE_BASE}/spark-warehouse"

echo "Setting up Hive and Spark warehouse directories..."
mkdir -p "${HIVE_METASTORE}"
mkdir -p "${SPARK_WAREHOUSE}"
chmod -R 777 "${WAREHOUSE_BASE}"

echo "  Hive Metastore: ${HIVE_METASTORE}"
echo "  Spark Warehouse: ${SPARK_WAREHOUSE}"
echo ""
echo "Setup complete."
echo "You can now run:"
echo "  spark-sql -f src/create_hive_tables.hql"
echo "  spark-sql -f src/validate_cdc.hql"
