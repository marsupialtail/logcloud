#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>

int main() {
    std::ifstream timestampFile("__timestamp__");
    std::ifstream logFile("__rexed__");
    
    if (!timestampFile.is_open() || !logFile.is_open()) {
        std::cerr << "Failed to open the files." << std::endl;
        return 1;
    }

    std::unordered_map<std::string, std::ofstream> dateFiles;
    
    std::string timestamp, log;
    
    while (getline(timestampFile, timestamp)) {
        if (timestamp.back() == ',') {
            timestamp.pop_back(); // remove the comma at the end
        }

        if (!getline(logFile, log)) {
            std::cerr << "Log file has fewer lines than the timestamp file." << std::endl;
            break;
        }

        std::string date = timestamp.substr(0, 10); // Extracting the date from the timestamp
        
        if (dateFiles.find(date) == dateFiles.end()) {
            dateFiles[date].open("big-logs/windows/" + date + ".log", std::ios::out | std::ios::app);
            if (!dateFiles[date].is_open()) {
                std::cerr << "Failed to open or create the file for date: " << date << std::endl;
                return 2;
            }
        }
        
        dateFiles[date] << timestamp << " " << log << std::endl;
    }
    
    // Close all open files
    for (auto &pair : dateFiles) {
        pair.second.close();
    }
    timestampFile.close();
    logFile.close();

    return 0;
}

