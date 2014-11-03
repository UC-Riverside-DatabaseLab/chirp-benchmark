#!/usr/bin/env python
#coding: utf-8

from commons import ujson


# Helper function 2: reads sorted file and extracts info required for generating benchmark
def extract_info(process_parameters, file_parameters):
    input = (file_parameters.input_file if file_parameters.pre_sorted else file_parameters.sorted_file)

    id = lambda data: tuple([data[field] for field in process_parameters.key_fields])
    timestamp = lambda data: data[process_parameters.time_field]

    ids = []
    max_time = float('-inf')
    min_time = float('inf')

    with open(input,'rb',1) as sorted_file:
        records = (line.strip() for line in sorted_file)
        for record in records:
            try:
                record = ujson.loads(record)
            except:
                continue
            rec_id = id(record)
            if rec_id[0] and rec_id[1]:
                ids.append(rec_id)
            if timestamp(record) > max_time:
                max_time = timestamp(record)
            if timestamp(record) < min_time:
                min_time = timestamp(record)

    return ids, max_time, min_time


