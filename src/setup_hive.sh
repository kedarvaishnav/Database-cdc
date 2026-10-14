#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# Hive Warehouse Setup Script
#
# Sets up the local Hive/Spark warehouse directories and environment variables
# needed before running create_hive_tables.sql.
#
# Usage:
#     bash src/setup_hive.sh
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

echo "========================================"
echo "  Hive Warehouse Setup"
echo "========================================"

# ── 1. Create warehouse directories ──────────────────────────────────────────
WAREHOUSE_BASE="${HOME}/warehouse"
HIVE_METASTORE="${WAREHOUSE_BASE}/hive-metastore"
SPARK_WAREHOUSE="${WAREHOUSE_BASE}/spark-warehouse"

echo ""
echo "Creating warehouse directories..."
mkdir -p "${HIVE_METASTORE}"
mkdir -p "${SPARK_WAREHOUSE}"
chmod -R 777 "${WAREHOUSE_BASE}"
echo "  ✅ ${HIVE_METASTORE}"
echo "  ✅ ${SPARK_WAREHOUSE}"

# ── 2. Detect / export HIVE_HOME ─────────────────────────────────────────────
if [ -z "${HIVE_HOME:-}" ]; then
    # Try common install locations
    for candidate in /opt/hive /usr/local/hive /usr/lib/hive "${HOME}/hive"; do
        if [ -d "${candidate}" ]; then
            export HIVE_HOME="${candidate}"
            break
        fi
    done
fi

if [ -n "${HIVE_HOME:-}" ]; then
    echo ""
    echo "HIVE_HOME  = ${HIVE_HOME}"
else
    echo ""
    echo "⚠️  HIVE_HOME not found (set manually if needed)"
fi

# ── 3. Detect / export HADOOP_HOME ───────────────────────────────────────────
if [ -z "${HADOOP_HOME:-}" ]; then
    for candidate in /opt/hadoop /usr/local/hadoop /usr/lib/hadoop "${HOME}/hadoop"; do
        if [ -d "${candidate}" ]; then
            export HADOOP_HOME="${candidate}"
            break
        fi
    done
fi

if [ -n "${HADOOP_HOME:-}" ]; then
    echo "HADOOP_HOME = ${HADOOP_HOME}"
else
    echo "⚠️  HADOOP_HOME not found (set manually if needed)"
fi

# ── 4. Detect / export SPARK_HOME ────────────────────────────────────────────
if [ -z "${SPARK_HOME:-}" ]; then
    for candidate in /opt/spark /usr/local/spark "${HOME}/spark"; do
        if [ -d "${candidate}" ]; then
            export SPARK_HOME="${candidate}"
            break
        fi
    done
fi

if [ -n "${SPARK_HOME:-}" ]; then
    echo "SPARK_HOME  = ${SPARK_HOME}"
else
    echo "⚠️  SPARK_HOME not found (set manually if needed)"
fi

# ── 5. Summary ───────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo "  Setup Complete"
echo "========================================"
echo ""
echo "Next step: Run the Hive DDL"
echo "  hive -f src/create_hive_tables.sql"
echo "  -- OR with Spark SQL --"
echo "  spark-sql -f src/create_hive_tables.sql"
echo ""
