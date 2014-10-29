
import random


def process_tweets(in_file="parsed_tweets.txt", speedup=10, rw_ratio=10, ps_ratio=1):

    fields = []
    tweet_bunch = {}
    retweet_map = {}

    # Get file contents with tweets and parsed out CreationTime, Retweets, UserID and ID into memory
    with open(in_file, "r") as r:
        for line in r:
            fields = line.split('@,@', 5)
            # Store in a hash map using creation time concatenated with tweet id as key
            for i in range(0,4):
                fields[i] = int(fields[i])
            key = fields[0]*1000000000000000000 + fields[3]
            tweet_bunch[key] = fields
            try:
                if not fields[1] == 0:
                    retweet_map[fields[1]] = retweet_map[fields[1]] + [key]
            except KeyError:
                retweet_map[fields[1]] = [key]

    # Create an index for tweets sorted by creation time and then by tweet id (by tweet key)
    keys = tweet_bunch.keys()
    keys.sort()
    number_of_tweets = len(keys)

    # Create a list of tweet keys (with repeats according to frequency) to be read
    tweets_to_read = []
    retweets = retweet_map.keys()
    for retweet_count in retweets:
        tids = retweet_map[retweet_count]
        for tid in tids:
            tweets_to_read += [tid]*retweet_count
    while len(tweets_to_read) < 0.2*number_of_tweets:
        tweets_to_read += [random.choice(keys)]

    # Calculate lambda for Poisson distribution of reads
    duration = keys[-1]/1000000000000000000 - keys[0]/1000000000000000000 # 13 digit Unix time -> milisecond precision
    number_of_reads = rw_ratio * len(tweet_bunch)
    lambda_for_reads = float(number_of_reads) / duration # in tweets per millisecond

    # Generate benchmark
    benchmark = []
    zero_time = keys[0]/1000000000000000000
    read_time = 0
    write_index = 0
    p_threshold = 1/(1 + float(ps_ratio))
    with open("benchmark.file", "w") as w:
        while read_time < duration:
            read_time += int(random.expovariate(lambda_for_reads))
            # Enter writes into benchmark
            while write_index < number_of_tweets and tweet_bunch[keys[write_index]][0] - zero_time <= read_time:
                w.writelines("\t".join([str((tweet_bunch[keys[write_index]][0]-zero_time)/speedup).zfill(6),"w",tweet_bunch[keys[write_index]][4]]))
                write_index += 1
            # generate random tweet (to be read) based on (possibly Zipf) distribution of retweets
            tweet = tweet_bunch[random.choice(tweets_to_read)]
            toss = random.random()
            if toss > p_threshold:
                w.writelines("\t".join([str(read_time/speedup).zfill(6),"rp",str(tweet[3])+'\n']))
            else:
                w.writelines("\t".join([str(read_time/speedup).zfill(6),"rs",str(tweet[2])+'\n']))


if __name__ == '__main__':
    process_tweets()

