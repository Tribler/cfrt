#!/bin/bash


cd $OUTPUT_DIR
find -iname '00*.out' -print0 | xargs -0 cat | grep ";" > crdt.csv

echo "Plotting graph_crdt.r"
$EXPERIMENT_DIR/graph_crdt.r

echo "Extracting ipv8 stats"
../gumby/experiments/ipv8/parse_ipv8_statistics.py .
echo "Graph ipv8 stats"
graph_ipv8_stats.sh

echo "Plotting process guard data"
graph_process_guard_data.sh

echo "Convert to svg"
find . -iname '*.svg' | sed -r -e "s/.svg$//g" | xargs -i{} svg2pdf {}.svg {}.pdf
