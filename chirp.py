#!/usr/bin/env python
#coding: utf-8

import sys, tempfile, heapq, random, os, argparse, collections, math, itertools, operator

# Try importing faster ujson package
try:
    import ujson
except ImportError:
    import json as ujson


# Define some named tuples to make downstream code more readable
ProcessingParameters = collections.namedtuple('ProcessingParameters', 'buffer_size time_field sort_fields key_fields')
BenchmarkParameters = collections.namedtuple('BenchmarkParameters', 'speedup rw_ratio ps_ratio')
FileParameters = collections.namedtuple('FileParameters', 'input_file pre_sorted sorted_file keep_sorted_file output_file temp_dirs')



# Helper function 1: extracts key for a given record
def sort_key(data):
    try:
        parsed_data = ujson.loads(data)
    except:
        return (int('0xdbe928f86f85143c8282db0da081c05530ea2163', 16),) # kludge: magic key indicates unparsable json
    return tuple([parsed_data[field] for field in process_parameters.sort_fields])


# External sort code
# based on ActiveState Recipe 466302: Sorting big files the Python 2.4 way by Nicolas Lehuen &
# ActiveState Recipe 576755: Sorting big files the Python 2.6 way by Gabriel Genellina
# http://code.activestate.com/recipes/576755-sorting-big-files-the-python-26-way/
def merge(key, *iterables):
    # based on code posted by Scott David Daniels in c.l.p.
    # http://groups.google.com/group/comp.lang.python/msg/484f01f1ea3c832d

    Keyed = collections.namedtuple("Keyed", ["key", "obj"])

    keyed_iterables = [(Keyed(key(obj), obj) for obj in iterable) for iterable in iterables]
    for element in heapq.merge(*keyed_iterables):
        if element.key != (int('0xdbe928f86f85143c8282db0da081c05530ea2163', 16),):
            yield element.obj
        else:
            continue

def batch_sort():
    tempdirs = file_parameters.temp_dirs

    if tempdirs is None:
        tempdirs = []
    if not tempdirs:
        tempdirs.append(tempfile.gettempdir())

    chunks = []
    try:
        with open(file_parameters.input_file,'rb',64*1024) as input_file:
            input_iterator = iter(input_file)
            for tempdir in itertools.cycle(tempdirs):
                current_chunk = list(itertools.islice(input_iterator,process_parameters.buffer_size))
                if not current_chunk:
                    break
                current_chunk.sort(key=sort_key)
                output_chunk = open(os.path.join(tempdir,'%06i'%len(chunks)),'w+b',64*1024)
                chunks.append(output_chunk)
                output_chunk.writelines(current_chunk)
                output_chunk.flush()
                output_chunk.seek(0)
        del current_chunk
        with open(file_parameters.sorted_file,'wb',64*1024) as output_file:
            output_file.writelines(merge(sort_key, *chunks))
    finally:
        for chunk in chunks:
            try:
                chunk.close()
                os.remove(chunk.name)
            except Exception:
                pass


# Helper function 2: reads sorted file and extracts info required for generating benchmark
def extract_info():
    input = (file_parameters.input_file if file_parameters.pre_sorted else file_parameters.sorted_file)

    id = lambda data: tuple([data[field] for field in process_parameters.key_fields])
    timestamp = lambda data: data[process_parameters.time_field]

    global ids
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

    return max_time, min_time


# Benchmark generation code
def generate_benchmark():
    input = open((file_parameters.input_file if file_parameters.pre_sorted else file_parameters.sorted_file),'rb',1)
    sorted_file = (line.strip() for line in input)

    max_time, zero_time = extract_info()
    tweets = ids
    number_of_tweets = len(tweets)

    # calculate parameter lambda for a Poisson distribution of reads
    duration = max_time - zero_time
    number_of_reads = float(benchmark_parameters.rw_ratio) * number_of_tweets
    lambda_for_reads = number_of_reads / duration # in tweets per millisecond

    # create a list of tweet indices to be read with repeats according to Zipf distribution
    first_tweet_frequency = int(number_of_reads / (math.log(number_of_tweets) + 0.5772156649)) # H_n ~ log(n) + Euler–Mascheroni constant
    tweet_indices_to_read = []
    rank_ = None
    for rank_ in xrange(len(tweets)):
        tweet_frequency = int(first_tweet_frequency / (rank_ + 1))
        if tweet_frequency == 0:
            break
        tweet_indices_to_read += [rank_] * tweet_frequency
    tweet_indices_to_read = tuple(tweet_indices_to_read)

    # remove tweets that will not be read to reduce memory footprint and make downstream calculations faster
    random.shuffle(tweets)
    tweets = tuple(tweets[:(rank_ + 1)])

    # generate benchmark
    read_time = 0
    p_threshold = 1/(1 + float(benchmark_parameters.ps_ratio))
    flush_limit = int(process_parameters.buffer_size / benchmark_parameters.rw_ratio)
    write_count = 0
    tweet_to_write = ujson.loads(sorted_file.next())
    tweet_timestamp = tweet_to_write[process_parameters.time_field] - zero_time

    with open(file_parameters.output_file, 'w', 64*1024) as w:
        while read_time < duration:
            read_time += int(random.expovariate(lambda_for_reads))

            # enter sorted writes into benchmark
            while write_count < number_of_tweets and tweet_timestamp <= read_time:
                w.write('\t'.join([str(int(tweet_timestamp/benchmark_parameters.speedup)).zfill(8),'w',ujson.dumps(tweet_to_write)])+'\n')
                try:
                    tweet_to_write = ujson.loads(sorted_file.next())
                    tweet_timestamp = tweet_to_write[process_parameters.time_field] - zero_time
                except:
                    pass
                write_count += 1
                if write_count % flush_limit == 0:
                    w.flush()

            # generate tweet to be read based on Zipf distribution
            tweet_to_read = tweets[random.choice(tweet_indices_to_read)]
            toss = random.random()
            try:
                if toss > p_threshold:
                    w.write('\t'.join([str(int(read_time/benchmark_parameters.speedup)).zfill(8),'rp',str(tweet_to_read[0])])+'\n')
                else:
                    w.write('\t'.join([str(int(read_time/benchmark_parameters.speedup)).zfill(8),'rs',str(tweet_to_read[1])])+'\n')
            except UnicodeEncodeError:
                pass

    input.close()


# Command line options parser code
def parse_args():
    usage = """./chirp.py -i IN_FILE [-o OUT_FILE] [-bs BUFFER_SIZE]
                          [-su SPEEDUP] [-rw RW_RATIO] [-ps PS_RATIO]"""

    description = 'Chirp benchmark program v1.0'

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

    help = 'Memory buffer size for sorting in terms of number of lines of input file. Default value is 4,000,000 lines.'
    parser.add_argument('-bs', action='store', type=int, dest='buffer_size', default=4000000, help=help)

    help = 'Timestamp field in the JSON records. Default value is \'CreationTime\'.'
    parser.add_argument('-tf', action='store', dest='time_field', default='CreationTime', help=help)

    help = 'List of fields to be used to sort JSON records in the input file. Should include the timestamp field as the first field. Repeat flag and provide multiple fields in the required order. Default value is [\'CreationTime\', \'ID\'].'
    parser.add_argument('-sf', action='append', dest='sort_fields', default=[], help=help)

    help = 'List of primary and secondary key fields. It should be possible to extract these fields and hold in memory for all records. Repeat flag and provide the primary key followed by the secondary key. Default value is [\'ID\', \'UserID\'].'
    parser.add_argument('-kf', action='append', dest='key_fields', default=[], help=help)

    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0')

    args = parser.parse_args()

    if not os.path.exists(args.in_file):
        parser.error('Input file does not exist: %s' % args.in_file)

    if args.temp_dirs and not reduce(operator.and_, map(os.path.exists, args.temp_dirs)):
        for temp_dir in temp_dirs:
            if not os.path.exists(temp_dir):
                parser.error('Temporary directory does not exist: %s' % temp_dir)

    # set parameters
    global process_parameters
    process_parameters = ProcessingParameters(buffer_size = args.buffer_size,
                                              time_field = args.time_field,
                                                     sort_fields = args.sort_fields if args.sort_fields else ['CreationTime', 'ID'],
                                                     key_fields = args.key_fields if args.key_fields else ['ID', 'UserID'])

    global benchmark_parameters
    benchmark_parameters = BenchmarkParameters(speedup = args.speedup,
                                                      rw_ratio = args.rw_ratio,
                                                      ps_ratio = args.ps_ratio)

    global file_parameters
    file_parameters = FileParameters(input_file = args.in_file,
                                            pre_sorted = args.pre_sorted,
                                            sorted_file = args.sorted_file,
                                            keep_sorted_file = args.keep_sorted_file,
                                            output_file = args.out_file,
                                            temp_dirs = args.temp_dirs)



if __name__ == '__main__':

    parse_args()

    if not file_parameters.pre_sorted:
        batch_sort()

    generate_benchmark()

    if not file_parameters.pre_sorted and not file_parameters.keep_sorted_file:
        os.remove(file_parameters.sorted_file)



