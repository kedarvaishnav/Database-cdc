#!/usr/bin/env bash
# Warehouse and Hive environment setup script
set -e

echo "Setting up Hive and Spark warehouse directories..."
mkdir -p "$HOME/warehouse/hive-metastore"
mkdir -p "$HOME/warehouse/spark-warehouse"
chmod -R 777 "$HOME/warehouse" 2>/dev/null || true

# Java 11 for Hive / Hadoop CLI compatibility
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop}"
export HIVE_HOME="${HIVE_HOME:-$HOME/hive}"

export PATH="$JAVA_HOME/bin:$HIVE_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

echo "✅ Environment configured:"
echo "   JAVA_HOME:   $JAVA_HOME"
echo "   HADOOP_HOME: $HADOOP_HOME"
echo "   HIVE_HOME:   $HIVE_HOME"
echo "   Warehouse:   $HOME/warehouse"