
#include <fstream>
#include <iostream>
#include <string>

#include <rapidjson/document.h>

using namespace std;


// Create a extract containing fewer tweets
int create_extract(int lines_to_read = 50, string in_file = "input_file.txt", string out_file = "tweet_extract.txt") {
ifstream ifile(in_file.c_str());
ofstream ofile(out_file.c_str());

if (!ifile) {
  cerr << "Can't open input file " << endl;
  return -1;
}

if (!ofile) {
  cerr << "Can't open output file " << endl;
  return -2;
}

string line;

while(getline(ifile, line) and lines_to_read > 0) {
  ofile << line << endl;
  lines_to_read--;
}

ifile.close();
ofile.close();

return 0;
}


// Parse out fields required by the benchmark generator
int parse_tweets(string in_file = "tweet_extract.txt", string out_file = "parsed_tweets.txt") {
ifstream ifile(in_file.c_str());
ofstream ofile(out_file.c_str());

if (!ifile) {
  cerr << "Can't open input file " << endl;
  return -1;
}

if (!ofile) {
  cerr << "Can't open output file " << endl;
  return -2;
}

string line;
rapidjson::Document d;

while(getline(ifile, line)) {
  d.Parse<0>(line.c_str());
  ofile << d["CreationTime"].GetUInt64() << "@,@" << (d["RetweetsNum"].IsNull() ? 0: d["RetweetsNum"].GetUInt()) << "@,@" << d["UserID"].GetUInt64() << "@,@" << d["ID"].GetUInt64() << "@,@" << line << endl;
}

ifile.close();
ofile.close();

return 0;
}


int main () {

create_extract(5000000); // Extract 5 million tweets from input file
parse_tweets();

return 0;
}
