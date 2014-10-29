#!/usr/bin/env bash

echo
echo "Folder contents: "
ls
echo
echo "Running command ../chirp.py -i tweet_sample.txt"
../chirp.py -i tweet_extract.txt
echo "Benchmark created"
echo
echo "Folder contents: "
ls
echo

