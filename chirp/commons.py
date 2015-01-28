#!/usr/bin/env python
#coding: utf-8

import collections

try:
	import ujson
except ImportError:
	import json as ujson

__all__ = ['ProcessingParameters', 'BenchmarkParameters', 'FileParameters', 'ujson']


# Define some named tuples to make downstream code more readable
ProcessingParameters = collections.namedtuple('ProcessingParameters', 'buffer_size time_field sort_fields key_fields')
BenchmarkParameters = collections.namedtuple('BenchmarkParameters', 'speedup rw_ratio ps_ratio freshness read_buffer output_limit read_range_width width_strictly_enforced keys_not_strings')
FileParameters = collections.namedtuple('FileParameters', 'input_file pre_sorted sorted_file keep_sorted_file output_file temp_dirs')

