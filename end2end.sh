#!/bin/bash

# Set PYTHONPATH to include project root
export PYTHONPATH="${PYTHONPATH}:$(pwd):$(pwd)/checks"

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Default parallel workers (override with: ./end2end.sh 5)
WORKERS=${1:-1}

echo "=== Data Validation Pipeline ==="
echo "Parallel workers: $WORKERS"
echo ""

# ========== META CHECK ==========
# Part 1: PCDS Meta Check (run on PCDS/Windows machine)
echo "=== Part 1: PCDS Meta Check ==="
python checks/meta_check_pcds.py

# Part 2: AWS Meta Check (run on AWS/Linux machine)
echo "=== Part 2: AWS Meta Check ==="
python checks/meta_check_aws.py

# Part 3: Meta Check Comparison
echo "=== Part 3: Meta Check Comparison ==="
python checks/compare_report.py

echo "✓ Meta Check Complete"
echo ""

# ========== COLUMN CHECK ==========
# Part 4: PCDS Column Statistics (run on PCDS/Windows machine)
echo "=== Part 4: PCDS Column Check ==="
python checks/column_check_pcds.py $WORKERS

# Part 5: AWS Column Statistics (run on AWS/Linux machine)
echo "=== Part 5: AWS Column Check ==="
python checks/column_check_aws.py $WORKERS

# Part 6: Column Check Comparison
echo "=== Part 6: Column Check Comparison ==="
python checks/column_check_compare.py

echo "✓ Column Check Complete"
echo ""

# ========== HASH CHECK ==========
# Part 7: PCDS Hash Check (run on PCDS/Windows machine)
echo "=== Part 7: PCDS Hash Check ==="
python checks/hash_check_pcds.py $WORKERS

# Part 8: AWS Hash Check (run on AWS/Linux machine)
echo "=== Part 8: AWS Hash Check ==="
python checks/hash_check_aws.py $WORKERS

# Part 9: Hash Check Comparison
echo "=== Part 9: Hash Check Comparison ==="
python checks/hash_check_compare.py

echo "✓ Hash Check Complete"
echo ""

echo "=== Pipeline Complete ==="
