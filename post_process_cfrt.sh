#!/bin/bash

cd $OUTPUT_DIR
find -iname '00*.out' -print0 | xargs -0 cat | grep ";" > cfrt.csv

$EXPERIMENT_DIR/graph_cfrt.r

../gumby/experiments/ipv8/parse_ipv8_statistics.py .
graph_ipv8_stats.sh

graph_process_guard_data.sh

find . -iname '*.svg' | sed -r -e "s/.svg$//g" | xargs -i{} svg2pdf {}.svg {}.pdf

