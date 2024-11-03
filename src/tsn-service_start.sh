#!/bin/bash

make

executables=("./coordinator -p 9090" "./tsd -c 1 -s 1 -h localhost -k 9090 -p 10000" "./tsd -c 2 -s 2 -h localhost -k 9090 -p 10001")

for program in "${executables[@]}"; do
    gnome-terminal -- bash -c "$program; exec bash"
    sleep 3
done