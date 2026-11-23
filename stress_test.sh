#!/usr/bin/env bash

TEST_NAME="storage_engine::snapshot::tests::concurrent_aggregation_is_consistent"
RUNS=100
PAUSE=2

LOG_DIR="test-stress-logs"
SUMMARY="$LOG_DIR/summary.log"

mkdir -p "$LOG_DIR"
: > "$SUMMARY"

FAILURES=0

echo "Starting $RUNS stress-test runs"
echo "Available CPUs: $(nproc)"
echo "Test: $TEST_NAME"
echo "Pause between runs: ${PAUSE}s"
echo "Logs: $LOG_DIR"
echo

for ((i=1; i<=RUNS; i++)); do
    LOG_FILE="$LOG_DIR/run_$i.log"
    START=$(date '+%Y-%m-%d %H:%M:%S')

    echo "[$i/$RUNS] Running..."

    RUST_BACKTRACE=full cargo test "$TEST_NAME" -- --exact \
        > "$LOG_FILE" 2>&1

    EXIT_CODE=$?

    if [ "$EXIT_CODE" -eq 0 ]; then
        echo "PASS"
        rm "$LOG_FILE"
        echo "[$START] Run $i: PASS" >> "$SUMMARY"
    else
        echo "FAIL (exit code: $EXIT_CODE)"
        FAILURES=$((FAILURES + 1))

        {
            echo "========================================"
            echo "Run: $i"
            echo "Timestamp: $START"
            echo "Exit code: $EXIT_CODE"
            echo "Log: $LOG_FILE"
            echo "========================================"
        } | tee -a "$SUMMARY"
    fi

    echo
    echo "Memory status after run $i:"
    free -h

    echo
    echo "Waiting ${PAUSE}s before next run..."
    sleep "$PAUSE"
    echo "----------------------------------------"
done

echo
echo "========================================"
echo "STRESS TEST RESULTS"
echo "========================================"
echo "Total runs: $RUNS"
echo "Passed: $((RUNS - FAILURES))"
echo "Failed: $FAILURES"
echo "========================================"

if [ "$FAILURES" -gt 0 ]; then
    echo "Investigate failed logs in: $LOG_DIR"
    exit 1
fi

echo "All tests passed!"
exit 0
