#!/usr/bin/env bash

echo
echo "Folder contents: "
ls
echo
echo -e "Running command: \033[32m../run_chirp.py -i tweet_sample.txt\033[0m"
../run_chirp.py -i tweet_sample.txt
echo "Benchmark created"
echo
echo "Folder contents: "
ls
echo

