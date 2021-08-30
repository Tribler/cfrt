#!/bin/bash

cd $OUTPUT_DIR
$EXPERIMENT_DIR/graph_times.r

../gumby/experiments/ipv8/parse_ipv8_statistics.py .
graph_ipv8_stats.sh

graph_process_guard_data.sh

cp ../saves/cfrt_three/tribler_times_numbered.csv .
find -iname '00*.out' -print0 | xargs -0 cat | grep ";validate" | sort -g | tail -n 1198608 | nl -s ";" > cfrt_times_numbered.csv

$EXPERIMENT_DIR/graph_times.r

find . -iname '*.svg' | sed -r -e "s/.svg$//g" | xargs -i{} svg2pdf {}.svg {}.pdf

