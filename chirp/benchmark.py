#!/usr/bin/env python
#coding: utf-8

import random, os, math
from commons import ujson
from ts_circular_buffer import TSCircularBuffer


# Extracts information about the JSON file that has been sorted by the timestamp field
def extractInfo(process_parameters, file_parameters):
    input_ = (file_parameters.input_file if file_parameters.pre_sorted else file_parameters.sorted_file)
    
    timestamp = lambda data: data[process_parameters.time_field]
    
    first_tweet = ujson.loads(os.popen("head -1 " + input_).readlines()[0].strip())
    last_tweet = ujson.loads(os.popen("tail -1 " + input_).readlines()[0].strip())
    number_of_tweets = int(os.popen("wc -l " + input_).readlines()[0].strip().split()[0])
    
    return number_of_tweets, timestamp(last_tweet), timestamp(first_tweet)


# Benchmark generation code
def generate_benchmark(process_parameters, benchmark_parameters, file_parameters):
    
    lines_written = 0
    with open((file_parameters.input_file if file_parameters.pre_sorted else file_parameters.sorted_file),'rb',1) as input_:
        
        sorted_file = (line.strip() for line in input_)
    
        number_of_tweets, max_time, zero_time = extractInfo(process_parameters, file_parameters)

        # Calculate parameter lambda for a Poisson distribution of reads
        duration = max_time - zero_time
        number_of_reads = float(benchmark_parameters.rw_ratio) * number_of_tweets
        lambda_for_reads = number_of_reads / duration # in tweets per millisecond
        
        # Create a buffer that will store a fixed number of previously written tweets
        tweets_p = TSCircularBuffer(benchmark_parameters.read_buffer)
        tweets_s = TSCircularBuffer(benchmark_parameters.read_buffer)
        id_p = lambda data: data[process_parameters.key_fields[0]]
        id_s = lambda data: data[process_parameters.key_fields[1]]
        
        # Generate benchmark
        read_time = 0
        p_threshold = 1/(1 + float(benchmark_parameters.ps_ratio))
        flush_limit = int(process_parameters.buffer_size / benchmark_parameters.rw_ratio)
        write_count = 0
        tweet_to_write = ujson.loads(sorted_file.next())
        tweet_timestamp = tweet_to_write[process_parameters.time_field] - zero_time

        with open(file_parameters.output_file, 'w', 64*1024) as w:
            while read_time < duration:
                read_time += int(random.expovariate(lambda_for_reads))

                # Enter sorted writes into benchmark
                while write_count < number_of_tweets and tweet_timestamp <= read_time:
                    w.write('\t'.join([str(int(tweet_timestamp/benchmark_parameters.speedup)).zfill(8),
                                       'w',ujson.dumps(tweet_to_write)])+'\n')
                    
                    lines_written += 1
                    if lines_written == benchmark_parameters.output_limit:
                        return
                    
                    # Insert the just-written tweet into the read buffer with a likelihood based on its timestamp and the freshness
                    if id_p(tweet_to_write):
                        tweets_p.insert(id_p(tweet_to_write), benchmark_parameters.freshness*tweet_timestamp/benchmark_parameters.speedup)
                    if id_s(tweet_to_write):
                        tweets_s.insert(id_s(tweet_to_write), benchmark_parameters.freshness*tweet_timestamp/benchmark_parameters.speedup)
                    
                    try:
                        tweet_to_write = ujson.loads(sorted_file.next())
                        tweet_timestamp = tweet_to_write[process_parameters.time_field] - zero_time
                    except:
                        pass
                    write_count += 1
                    if write_count % flush_limit == 0:
                        w.flush()

                # Generate random tweet to be read from the buffer
                toss = random.random()
                try:
                    if toss > p_threshold:
                        p_id = tweets_p.rand()
                        if p_id:
                            w.write('\t'.join([str(int(read_time/benchmark_parameters.speedup)).zfill(8),'rp',str(p_id)])+'\n')
                            lines_written += 1
                            if lines_written == benchmark_parameters.output_limit:
                                return
                    else:
                        s_id = tweets_s.rand()
                        if s_id:
                            w.write('\t'.join([str(int(read_time/benchmark_parameters.speedup)).zfill(8),'rs',str(s_id)])+'\n')
                            lines_written += 1
                            if lines_written == benchmark_parameters.output_limit:
                                return
                    
                except UnicodeEncodeError:
                    pass

