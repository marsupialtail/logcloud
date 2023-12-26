#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/*
This code will expose two utility functions to Python API -- inferring the
length of the timestamp field as well as the format of the timestamp field.
*/

inline bool isValidTimestamp(size_t timestamp) {
  // Define minimum and maximum valid epoch times
  size_t minValidTimestamp = 946684800;  // January 1, 2000, 00:00:00 UTC
  size_t maxValidTimestamp = 2524608000; // January 1, 2050, 00:00:00 UTC

  return (timestamp >= minValidTimestamp && timestamp < maxValidTimestamp);
}

int main(int argc, char **argv) {
  struct tm tm;
  char buf[255];

  memset(&tm, 0, sizeof(tm));
  strptime(argv[1], argv[2], &tm);
  // convert tm into epoch time in milliseconds
  tm.tm_isdst = -1;
  long epoch_ts = mktime(&tm);

  std::cout << epoch_ts << std::endl;

  struct tm timeinfo;

  // Convert the epoch time to a struct tm using localtime
  localtime_r(&epoch_ts, &timeinfo);

  // Create a character array to hold the formatted time string
  char timeString[100]; // Adjust the size as needed

  // Use strftime to format the time string
  strftime(timeString, sizeof(timeString), "%Y-%m-%d %H:%M:%S", &timeinfo);

  // Print the formatted time string
  std::cout << "Formatted time string: " << timeString << std::endl;

  exit(EXIT_SUCCESS);
}
