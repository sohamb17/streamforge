#!/usr/bin/env bash
# Rebuilds bench/results/faults/SUMMARY.md from the scenario directories.
source "$(dirname "$0")/lib.sh"
$COMPOSE run --rm -T tools probe -summarize /results/faults
cat bench/results/faults/SUMMARY.md
