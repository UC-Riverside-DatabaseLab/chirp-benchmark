
##Chirp Benchmark 3.0

Use the Chirp Python script to generate a benchmark file that is formatted as shown below:

```
00000000	w	{"UserMentions": "379961664;", "GeoLocation": null, "Hashtags": null, "ReplyToId": null, "UserID": 30363580, "IsFavorited": false, "CreationTime": 1377241159000, "MediaLinks": null, "URLLinks": null, "Source": "<a href=\"http://twitter.com/download/iphone\" rel=\"nofollow\">Twitter for iPhone</a>", "Place": null, "Text": "Tweet text removed as per Twitter policy", "IsRetweet": true, "RetweetsNum": null, "ID": 370802416255787008}
00000000	w	{"UserMentions": null, "GeoLocation": null, "Hashtags": null, "ReplyToId": null, "UserID": 1316308724, "IsFavorited": false, "CreationTime": 1377241159000, "MediaLinks": null, "URLLinks": "http://feedproxy.google.com/~r/reuters/rns_ent/~3/V4ic80GYhxw/story01.htm;", "Source": "<a href=\"http://blackberry.com/twitter\" rel=\"nofollow\">Twitter for BlackBerry\u00ae</a>", "Place": null, "Text": "Tweet text removed as per Twitter policy", "IsRetweet": false, "RetweetsNum": null, "ID": 370802416691982336}
00000000	rp	370802416691982336
00000000	rp	370802416691982336
00000000	rs	130288926
00000000	rp	370802416255787008
```

Each line contains of the tab-separated file:
 - A timestamp, in milliseconds
 - A command:
   - w for writes
   - rp for reads on primary key
   - rs for reads on secondary key
 - A value:
   - a JSON tweet object for writes
   - one or two primary key(s)
   - one or two secondary key(s)

A visual walkthrough is available on the [Chirp website](http://www.cs.ucr.edu/~ameno002/benchmark/).

Check out the Chirp help for detailed information about configuration parameters.
```
./run_chirp.py --help
```


####Minimum requirements

- Python 2.7
- Optional, but highly recommended: UltraJSON 1.33 - https://pypi.python.org/pypi/ujson

