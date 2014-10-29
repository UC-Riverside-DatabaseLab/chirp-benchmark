#!/usr/bin/env bash

echo
echo "Folder contents: "
ls
echo
echo "Running command ../benchmark_gen.py -i tweet_extract.txt"
../benchmark_gen.py -i tweet_extract.txt
echo "Benchmark created"
echo
echo "Folder contents: "
ls
echo

