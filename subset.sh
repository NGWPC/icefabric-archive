#!/bin/bash

# The command to time
COMMAND="icefabric subset --identifier gages-01010000 --id-type hl_uri --domain conus_hf --catalog sql"
COMMAND2="icefabric subset-v2 --identifier gages-01010000 --id-type hl_uri --domain conus_hf --catalog sql"

echo "Timing command: $COMMAND"
echo "$(printf '=%.0s' {1..60})"

# Start timing
start_time=$(date +%s.%N)

# Execute the command
if eval "$COMMAND"; then
    # End timing
    end_time=$(date +%s.%N)

    # Calculate execution time
    execution_time=$(echo "$end_time - $start_time" | bc -l)
    printf "  ✓ Completed v1 in %.3f seconds\n" "$execution_time"
else
    echo "  ✗ Command v1 failed on run $i"
    exit 1
fi

echo "Timing command: $COMMAND2"
echo "$(printf '=%.0s' {1..60})"

# Start timing
start_time=$(date +%s.%N)

# Execute the command
if eval "$COMMAND2"; then
    # End timing
    end_time=$(date +%s.%N)

    # Calculate execution time
    execution_time=$(echo "$end_time - $start_time" | bc -l)
    printf "  ✓ Completed v2 in %.3f seconds\n" "$execution_time"
else
    echo "  ✗ Command v2 failed on run $i"
    exit 1
fi
