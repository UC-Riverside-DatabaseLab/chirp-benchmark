#!/usr/bin/env python
#coding: utf-8

import os
from chirp import *

if __name__ == '__main__':

    process_parameters, benchmark_parameters, file_parameters = options_parser.parse_args()
    
    if not file_parameters.pre_sorted:
        external_sort.batch_sort(process_parameters, file_parameters)

    ts_benchmark.generate_benchmark(process_parameters, benchmark_parameters, file_parameters)

    if not file_parameters.pre_sorted and not file_parameters.keep_sorted_file:
        os.remove(file_parameters.sorted_file)

