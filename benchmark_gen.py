#!/usr/bin/env python

import ujson, sys, tempfile, heapq, random, os, argparse, collections



def process_json_file(in_file, process_parameters):

    contents = {}
    sorted_files = []
    tmp_dir = tempfile.mkdtemp()
    key_of = lambda data: ''.join([str(data[field]) for field in process_parameters.key_fields])
    extract_from = lambda data: [data[field] for field in process_parameters.extract_fields]

    # sort input file in chunks
    with open(in_file, 'r') as i:
        for line in i:
            data = ujson.loads(line)
            contents[key_of(data)] = data

            # if memory buffer is exceeded, sort and flush to disk
            if sys.getsizeof(contents) > process_parameters.buffer_size:
                keys = contents.keys()
                keys.sort()

                tmp_file = tempfile.TemporaryFile('w+t', dir=tmp_dir)
                for key in keys:
                    tmp_file.write(ujson.dumps(contents[key])+'\n')

                sorted_files += [tmp_file]
                contents = {}

    # flush any remaining data to disk
    if sys.getsizeof(contents) > 0:
        keys = contents.keys()
        keys.sort()

        tmp_file = tempfile.TemporaryFile('w+t', dir=tmp_dir)
        for key in keys:
            tmp_file.write(ujson.dumps(contents[key])+'\n')

        sorted_files += [tmp_file]
    del contents

    # put first lines of sorted files into a heap
    first_lines = []
    for tmp_file in sorted_files:
        tmp_file.seek(0)
        data = ujson.loads(tmp_file.readline())
        first_lines += [(key_of(data), data, tmp_file)]
    heapq.heapify(first_lines)
    del sorted_files

    # write sorted data into output file, extract information and calculate stats
    sorted_file = tempfile.TemporaryFile('w+t', dir=tmp_dir)
    data_extract = []

    count = 0
    max_val_indices = range(0,len(process_parameters.stats_fields.max))
    max_vals = [-float('inf') for i in max_val_indices]
    min_val_indices = range(0,len(process_parameters.stats_fields.min))
    min_vals = [float('inf') for i in min_val_indices]
    sum_val_indices = range(0,len(process_parameters.stats_fields.sum))
    sum_vals = [0 for i in sum_val_indices]

    while first_lines:
        min_data = heapq.heappop(first_lines)
        sorted_file.write(ujson.dumps(min_data[1])+'\n')

        # might need tweaking - will skip lines without every field
        try:
            data_extract += [extract_from(min_data[1])]
        except KeyError:
            pass

        # calculate stats
        count += 1
        for index in max_val_indices:
            if max_vals[index] < min_data[1][process_parameters.stats_fields.max[index]]:
                max_vals[index] = min_data[1][process_parameters.stats_fields.max[index]]
        for index in min_val_indices:
            if min_vals[index] > min_data[1][process_parameters.stats_fields.min[index]]:
                min_vals[index] = min_data[1][process_parameters.stats_fields.min[index]]
        for index in sum_val_indices:
            sum_vals[index] += min_data[1][process_parameters.stats_fields.sum[index]]

        next = min_data[2].readline()
        if next:
            new_data = ujson.loads(next)
            heapq.heappush(first_lines, (key_of(new_data), new_data, min_data[2]))
        else:
            min_data[2].close()

    file_stats = [count, max_vals, min_vals, sum_vals]

    return sorted_file, tmp_dir, data_extract, file_stats



def generate_benchmark(in_file, out_file, process_parameters, benchmark_parameters):

    sorted_file, tmp_dir, data_extract, file_stats = process_json_file(in_file, process_parameters)

    number_of_tweets, [max_time], [zero_time], x = file_stats

    retweet_counts = {}
    tweets = []
    for item in data_extract:
        tid, uid, rtc = item
        tweets += [(tid, uid)]
        try:
            if rtc:
                retweet_counts[rtc] += [(tid, uid)]
        except KeyError:
            retweet_counts[rtc] = [(tid, uid)]
    del data_extract

    # create a list of (tweet id, user id) tuples to be read (with repeats according to retweet frequency)
    tweets_to_read = []
    for count in retweet_counts:
        tids = retweet_counts[count]
        for tid in tids:
            tweets_to_read += [tid] * count
    while len(tweets_to_read) < benchmark_parameters.min_tweet_diversity * number_of_tweets:
        tweets_to_read += [random.choice(tweets)]
    del retweet_counts, tweets

    # calculate parameter lambda for Poisson distribution of reads
    duration = max_time - zero_time
    number_of_reads = float(benchmark_parameters.rw_ratio) * number_of_tweets
    lambda_for_reads = number_of_reads / duration # in tweets per millisecond

    # generate benchmark
    benchmark = []
    read_time = 0
    p_threshold = 1/(1 + float(benchmark_parameters.ps_ratio))
    [time_variable] = process_parameters.stats_fields.max

    write_count = 0
    sorted_file.seek(0)
    tweet_to_write = ujson.loads(sorted_file.readline())
    tweet_timestamp = tweet_to_write[time_variable] - zero_time

    with open(out_file, 'w') as w:
        while read_time < duration:
            read_time += int(random.expovariate(lambda_for_reads))

            # enter sorted writes into benchmark
            while write_count < number_of_tweets and tweet_timestamp <= read_time:
                w.write('\t'.join([str(tweet_timestamp/benchmark_parameters.speedup).zfill(8),'w',ujson.dumps(tweet_to_write)])+'\n')
                try:
                    tweet_to_write = ujson.loads(sorted_file.readline())
                    tweet_timestamp = tweet_to_write[time_variable] - zero_time
                except:
                    pass
                write_count += 1

            # generate random tweet to be read based on (possibly Zipf) distribution of retweets
            tweet_to_read = random.choice(tweets_to_read)
            toss = random.random()
            if toss > p_threshold:
                w.write('\t'.join([str(read_time/benchmark_parameters.speedup).zfill(8),'rp',str(tweet_to_read[0])])+'\n')
            else:
                w.write('\t'.join([str(read_time/benchmark_parameters.speedup).zfill(8),'rs',str(tweet_to_read[1])])+'\n')

    sorted_file.close()
    os.removedirs(tmp_dir)



def parse_args():
    usage = """benchmark_gen.py -i IN_FILE [-o OUT_FILE] [-bs BUFFER_SIZE]
                        [-su SPEEDUP] [-rw RW_RATIO] [-ps PS_RATIO]
                        [-md MIN_TWEET_DIVERSITY]
                        
"""
    description = 'Chirp benchmark program v0.3'

    parser = argparse.ArgumentParser(usage=usage, description=description)

    help = 'Input file containing one JSON tweet per line.'
    parser.add_argument('-i', action='store', dest='in_file', required=True, help=help)

    help = 'Output benchmark file.'
    parser.add_argument('-o', action='store', dest='out_file', default='benchmark.file', help=help)

    help = 'Speedup factor for tweet timestamps. Default value is 100.'
    parser.add_argument('-su', action='store', type=float, dest='speedup', default=100, help=help)

    help = 'Reads to writes ratio. Default value is 30.'
    parser.add_argument('-rw', action='store', type=float, dest='rw_ratio', default=30, help=help)

    help = 'Reads on primary key to reads on secondary key ratio. Default value is 10.'
    parser.add_argument('-ps', action='store', type=float, dest='ps_ratio', default=10, help=help)

    help = 'Minimum tweet diversity (percentage of area under retweet distribution curve of tweets). Default value is 0.20.'
    parser.add_argument('-md', action='store', type=float, dest='min_tweet_diversity', default=0.20, help=help)

    help = 'Memory buffer size in MBs for sorting input file. Default value is 1000MB.'
    parser.add_argument('-bs', action='store', type=int, dest='buffer_size', default=1000, help=help)

    help = 'List of key fields used to sort tweets in input file. Repeat flag and provide multiple fields in required order. Default value is [\'CreationTime\', \'ID\']. WARNING: Make sure code does not break when modifying the default setting for this option.'
    parser.add_argument('-kf', action='append', dest='key_fields', default=['CreationTime', 'ID'], help=help)

    help = 'List of fields that can be extracted and held in memory and that are required for generating the benchmark. Repeat flag and provide multiple fields in any order. Default value is [\'ID\', \'UserID\', \'RetweetsNum\']. WARNING: Make sure code does not break when modifying the default setting for this option.'
    parser.add_argument('-ef', action='append', dest='extract_fields', default=['ID', 'UserID', 'RetweetsNum'], help=help)

    help = 'List of fields whose maximum value is required for generating the benchamrk. Repeat flag and provide multiple fields in any order. Default value is [\'CreationTime\']. WARNING: Make sure code does not break when modifying the default setting for this option.'
    parser.add_argument('-sf1', action='append', dest='stats_fields_max', default=['CreationTime'], help=help)

    help = 'List of fields whose minimum value is required for generating the benchamrk. Repeat flag and multiple provide fields in any order. Default value is [\'CreationTime\']. WARNING: Make sure code does not break when modifying the default setting for this option.'
    parser.add_argument('-sf2', action='append', dest='stats_fields_min', default=['CreationTime'], help=help)

    help = 'List of fields whose sum is required for generating the benchamrk. Repeat flag and provide multiple fields in any order. Default value is []. WARNING: Make sure code does not break when modifying the default setting for this option.'
    parser.add_argument('-sf3', action='append', dest='stats_fields_sum', default=[], help=help)

    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.3')


    args = parser.parse_args()

    if not os.path.exists(args.in_file):
        parser.error('Input file does not exist: %s' % args.in_file)

    # create some named tuples to make downstream code more readable
    StatsFields = collections.namedtuple('StatsFields', 'max min sum')
    ProcessingParameters = collections.namedtuple('ProcessingParameters', 'buffer_size key_fields extract_fields stats_fields')
    BenchmarkParameters = collections.namedtuple('BenchmarkParameters', 'speedup rw_ratio ps_ratio min_tweet_diversity')

    process_parameters = ProcessingParameters(buffer_size = args.buffer_size * 1048576,
                                           key_fields = args.key_fields,
                                           extract_fields = args.extract_fields,
                                           stats_fields = StatsFields(max = args.stats_fields_max,
                                                                      min = args.stats_fields_min,
                                                                      sum =  args.stats_fields_sum))
    benchmark_parameters = BenchmarkParameters(speedup = args.speedup,
                                               rw_ratio = args.rw_ratio,
                                               ps_ratio = args.ps_ratio,
                                               min_tweet_diversity =  args.min_tweet_diversity)

    return args.in_file, args.out_file, process_parameters, benchmark_parameters



if __name__ == '__main__':

    in_file, out_file, process_parameters, benchmark_parameters = parse_args()
    generate_benchmark(in_file, out_file, process_parameters, benchmark_parameters)



