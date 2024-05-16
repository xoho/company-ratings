for i in 5 6 7 8 9; do
    python import.py loadppp data/public_up_to_150k_${i}_230630.csv --quick-mode --chunk-size 20
    python3 import.py loadppp data/public_up_to_150k_${i}_230630.csv --chunk-size 20
done;