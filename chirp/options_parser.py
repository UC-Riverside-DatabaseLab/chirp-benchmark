#!/usr/bin/env python
#coding: utf-8

import os, argparse, operator
from commons import ProcessingParameters, BenchmarkParameters, FileParameters


# Command line options parser code
def parse_args():
    usage = """./chirp.py -i IN_FILE [-o OUT_FILE] [-bs BUFFER_SIZE]
                          [-su SPEEDUP] [-rw RW_RATIO] [-ps PS_RATIO]"""

    description = 'Chirp benchmark program v3.0'

    parser = argparse.ArgumentParser(usage=usage, description=description)

    help = 'Name and path of input file containing one JSON record per line.'
    parser.add_argument('-i', action='store', dest='in_file', required=True, help=help)

    help = 'Use flag to set to True if the input file is pre-sorted by timestamp field. Default value is False.'
    parser.add_argument('-p', action='store_true', dest='pre_sorted', default=False, help=help)

    help = 'Desired name and path of sorted intermediate file generated from the input file. Default value is \'sorted.dat\' in the current working directory.'
    parser.add_argument('-s', action='store', dest='sorted_file', default='sorted.dat', help=help)

    help = 'Use flag to set to True if you desire to keep the sorted intermediate file. Default value is same as pre-sorted flag.'
    parser.add_argument('-k', action='store_true', dest='keep_sorted_file', default=False, help=help)

    help = 'Desired name and path of the output benchmark file. Default value is \'benchmark.file\' in the current working directory.'
    parser.add_argument('-o', action='store', dest='out_file', default='benchmark.file', help=help)

    help = 'List of temporary directories to store file chunks while sorting. Choosing multiple directories on different physical drives may speed up the sorting. Repeat flag and provide multiple directory names in any order. Default value is obtained from environment variables or available system paths.'
    parser.add_argument('-t', action='append', dest='temp_dirs', default=[], help=help)

    help = 'Desired speedup factor for timestamps in the output benchmark file. Default value is 100.'
    parser.add_argument('-su', action='store', type=float, dest='speedup', default=100, help=help)

    help = 'Desired reads to writes ratio in the output benchmark file. Default value is 30.'
    parser.add_argument('-rw', action='store', type=float, dest='rw_ratio', default=30, help=help)

    help = 'Desired reads on primary key to reads on secondary key ratio in the output benchmark file. Default value is 10.'
    parser.add_argument('-ps', action='store', type=float, dest='ps_ratio', default=10, help=help)

    help = 'Desired freshness of reads. The higher the number the more likely are reads on recent writes. Default value is 0, i.e. all records in read buffer are equally likely to be picked - maximum \'staleness\'.'
    parser.add_argument('-f', action='store', type=float, dest='freshness', default=0, help=help)

    help = 'Read buffer size in terms of number of recently written IDs to remember for reading. Default value is 5,000 IDs.'
    parser.add_argument('-rb', action='store', type=int, dest='read_buffer', default=5000, help=help)

    help = 'Limit total number of commands in the output benchmark file. Default value depends on the number of JSON records in the input file and the read/write ratio.'
    parser.add_argument('-lo', action='store', type=float, dest='output_limit', default=float('inf'), help=help)

    help = 'Read commands query for ranges of key values of this width. Default value is 1, i.e. not a range query.'
    parser.add_argument('-rrw', action='store', type=int, dest='read_range_width', default=1, help=help)

    help = 'Range read commands query for ranges of exactly the specified width or none at all. Use the width-strictly-enforced flag to set to True. Default value is False.'
    parser.add_argument('-wse', action='store_true', dest='width_strictly_enforced', default=False, help=help)

    help = 'Key values are sorted in lexicographic order to calculate range widths. Use the keys-not-strings flag to set to False. Default value is True.'
    parser.add_argument('-kns', action='store_false', dest='keys_not_strings', default=True, help=help)

    help = 'Memory buffer size for sorting in terms of number of lines of input file. Default value is 500,000 lines.'
    parser.add_argument('-bs', action='store', type=int, dest='buffer_size', default=500000, help=help)

    help = 'Timestamp field in the JSON records. Default value is \'CreationTime\'.'
    parser.add_argument('-tf', action='store', dest='time_field', default='CreationTime', help=help)

    help = 'List of fields to be used to sort JSON records in the input file. Should include the timestamp field as the first field. Repeat flag and provide multiple fields in the required order. Default value is [\'CreationTime\', \'ID\'].'
    parser.add_argument('-sf', action='append', dest='sort_fields', default=[], help=help)

    help = 'List of primary and secondary key fields. It should be possible to extract these fields and hold in memory for all records. Repeat flag and provide the primary key followed by the secondary key. Default value is [\'ID\', \'UserID\'].'
    parser.add_argument('-kf', action='append', dest='key_fields', default=[], help=help)

    parser.add_argument('-v', '--version', action='version', version='%(prog)s 3.0')

    args = parser.parse_args()

    if not os.path.exists(args.in_file):
        parser.error('Input file does not exist: %s' % args.in_file)

    if args.temp_dirs and not reduce(operator.and_, map(os.path.exists, args.temp_dirs)):
        for temp_dir in temp_dirs:
            if not os.path.exists(temp_dir):
                parser.error('Temporary directory does not exist: %s' % temp_dir)

    # set parameters
    process_parameters = ProcessingParameters(buffer_size = args.buffer_size,
                                              time_field = args.time_field,
                                                     sort_fields = args.sort_fields if args.sort_fields else ['CreationTime', 'ID'],
                                                     key_fields = args.key_fields if args.key_fields else ['ID', 'UserID'])

    benchmark_parameters = BenchmarkParameters(speedup = args.speedup,
                                                      rw_ratio = args.rw_ratio,
                                                      ps_ratio = args.ps_ratio,
                                                      freshness = args.freshness,
                                                      read_buffer = args.read_buffer,
                                                      output_limit = args.output_limit,
                                                      read_range_width = args.read_range_width,
                                                      width_strictly_enforced = args.width_strictly_enforced,
                                                      keys_not_strings = args.keys_not_strings)

    file_parameters = FileParameters(input_file = args.in_file,
                                            pre_sorted = args.pre_sorted,
                                            sorted_file = args.sorted_file,
                                            keep_sorted_file = args.keep_sorted_file,
                                            output_file = args.out_file,
                                            temp_dirs = args.temp_dirs)

    return process_parameters, benchmark_parameters, file_parameters

