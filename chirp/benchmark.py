#!/usr/bin/env python
#coding: utf-8

import random, os, math
from commons import ujson


# Helper function 2: reads sorted file and extracts info required for generating benchmark
def extract_info(process_parameters, file_parameters):
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
def generate_benchmark(process_parameters, benchmark_parameters, file_parameters):
    input = open((file_parameters.input_file if file_parameters.pre_sorted else file_parameters.sorted_file),'rb',1)
    sorted_file = (line.strip() for line in input)

    max_time, zero_time = extract_info(process_parameters, file_parameters)
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

