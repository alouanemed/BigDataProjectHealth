#!/bin/bash

#
# This script:
#   1. Builds the project JAR via Gradle (shadowJar).
#   2. Assumes input data is ALREADY in HDFS (no automatic upload).
#   3. Runs Phase 1, Phase 2, and optionally Phase 3.
#
# Usage:
#   ./run_anomaly_pipeline.sh \
#       <HDFS_PHASE1_INPUT> \
#       <PHASE1_OUTPUT> \
#       <HDFS_PHASE2_INPUT> \
#       <PHASE2_OUTPUT> \
#       [PHASE3_OUTPUT] \
#       [PHASE1_BASELINE_FILE]
#
echo "============================================"
echo "1. Check Script Arguments"
echo "============================================"
# ============================
# 1. Check Script Arguments
# ============================
if [ $# -lt 4 ]; then
  echo "ERROR: At least 4 arguments required."
  echo "Usage: $0 <HDFS_PHASE1_INPUT> <PHASE1_OUTPUT> <HDFS_PHASE2_INPUT> <PHASE2_OUTPUT> [PHASE3_OUTPUT] [PHASE1_BASELINE_FILE]"
  exit 1
fi

HDFS_PHASE1_INPUT="$1"
PHASE1_OUTPUT="$2"
HDFS_PHASE2_INPUT="$3"
PHASE2_OUTPUT="$4"
PHASE3_OUTPUT="$5"
PHASE1_BASELINE_FILE="$6"

# ============================
# 2. Gradle Build Step
# ============================
echo "============================================"
echo "BUILDING JAR WITH GRADLE (shadowJar)"
echo "============================================"

# If you have a gradlew wrapper in your project directory, use ./gradlew
# Otherwise, just use gradle if it's installed system-wide.
# Adjust the command as needed (shadowJar, etc.)
if [ -f "./gradlew" ]; then
  ./gradlew clean shadowJar
else
  gradle clean shadowJar
fi

# The fat JAR is typically placed here:
JAR_PATH="build/libs/two-phase-mapreduce-jar.jar"

# If your jar name differs, adjust accordingly:
# e.g., "build/libs/two-phase-mapreduce-1.0-SNAPSHOT-all.jar"
# or rename it:
# cp build/libs/yourname-here.jar two-phase-mapreduce.jar

# ============================
# 3. Check the JAR
# ============================
if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: JAR does not exist or was not built at: $JAR_PATH"
  exit 1
fi

# ============================
# 4. Main Classes
# ============================
PHASE1_CLASS="siia.alouane.Phase1RegionBaselineComputation"
PHASE2_CLASS="siia.alouane.Phase2AnomalyLabeling"
PHASE3_CLASS="siia.alouane.Phase3AnomalyAggregator"

# ======================
# 5. Phase 1
# ======================
echo "=========================================="
echo " PHASE 1: Baseline Computation"
echo " Input : $HDFS_PHASE1_INPUT"
echo " Output: $PHASE1_OUTPUT"
echo "=========================================="

hdfs dfs -rm -r -skipTrash "$PHASE1_OUTPUT" 2>/dev/null

yarn jar "$JAR_PATH" \
  "$PHASE1_CLASS" \
  "$HDFS_PHASE1_INPUT" \
  "$PHASE1_OUTPUT"

# ======================
# 6. Phase 2
# ======================
echo "=========================================="
echo " PHASE 2: Anomaly Labeling"
echo " Input : $HDFS_PHASE2_INPUT"
echo " Output: $PHASE2_OUTPUT"

# If no baseline file is provided, default to part-r-00000 from Phase 1
if [ -z "$PHASE1_BASELINE_FILE" ]; then
  PHASE1_BASELINE_FILE="$PHASE1_OUTPUT/part-r-00000"
fi


echo " Baseline CSV: $PHASE1_BASELINE_FILE"
echo "=========================================="

hdfs dfs -rm -r -skipTrash "$PHASE2_OUTPUT" 2>/dev/null

yarn jar "$JAR_PATH" \
  "$PHASE2_CLASS" \
  "$HDFS_PHASE2_INPUT" \
  "$PHASE2_OUTPUT" \
  "$PHASE1_BASELINE_FILE"

# ======================
# 7. Optional Phase 3
# ======================
if [ -n "$PHASE3_OUTPUT" ]; then
  echo "=========================================="
  echo " PHASE 3: Anomaly Aggregation"
  echo " Phase 2 Output : $PHASE2_OUTPUT"
  echo " Phase 3 Output : $PHASE3_OUTPUT"
  echo "=========================================="

  hdfs dfs -rm -r -skipTrash "$PHASE3_OUTPUT" 2>/dev/null

  yarn jar "$JAR_PATH" \
    "$PHASE3_CLASS" \
    "$PHASE2_OUTPUT" \
    "$PHASE3_OUTPUT"
else
  echo "No Phase 3 argument provided. Skipping aggregator step."
fi

echo "=========================================="
echo " Pipeline COMPLETE"
echo "=========================================="
