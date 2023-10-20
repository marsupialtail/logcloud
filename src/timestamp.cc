#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <iostream>

int main(int argc, char ** argv)
{
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
