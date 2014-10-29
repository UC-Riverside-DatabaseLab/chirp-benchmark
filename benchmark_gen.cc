
#include <fstream>
#include <iostream>
#include <string>

#include <rapidjson/document.h>

using namespace std;


int create_extract(int lines_to_read = 50, string in_file = "/home/abhinand/work/all.csv", string out_file = "tweet_extract.txt") {
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
  ofile << d["CreationTime"].GetInt64() << "@,@" << (d["RetweetsNum"].IsNull() ? 0: d["RetweetsNum"].GetInt()) << "@,@" << d["UserID"].GetInt64() << "@,@" << d["ID"].GetInt64() << "@,@" << line << endl;
}

ifile.close();
ofile.close();

return 0;
}


int generate(string in_file = "tweet_extract.txt", string out_file = "benchmark.txt") {
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

  int64_t sortval[2] = {d["CreationTime"].GetInt64(), d["ID"].GetInt64()};
  ofile << sortval[0] << " " << sortval[1] << " " << line << endl;
}

ifile.close();
ofile.close();

return 0;
}


int main () {

create_extract(5000000);
parse_tweets();

return 0;
}
