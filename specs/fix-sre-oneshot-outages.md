#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <algorithm>
#include <cctype>

// Helper function to process a single line
void processLine(const std::string& line, std::map<std::string, int>& wordCounts) {
    std::stringstream ss(line);
    std::string word;

    while (ss >> word) {
        // Remove non-alphabetic characters from the start and end
        word.erase(std::remove_if(word.begin(), word.end(), [](char c) {
            return !std::isalpha(c);
        }), word.end());

        // Convert to lowercase
        std::transform(word.begin(), word.end(), word.begin(), ::tolower);

        if (!word.empty()) {
            wordCounts[word]++;
        }
    }
}

// Function to read a file and count word frequencies
void processFile(const std::string& filename) {
    std::ifstream inputFile(filename);
    
    if (!inputFile.is_open()) {
        std::cerr << "Error: Could not open file '" << filename << "'." << std::endl;
        return;
    }

    std::map<std::string, int> wordCounts;
    std::string line;

    while (std::getline(inputFile, line)) {
        processLine(line, wordCounts);
    }

    // Output results
    for (const auto& pair : wordCounts) {
        std::cout << pair.first << ": " << pair.second << std::endl;
    }

    inputFile.close();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <filename>" << std::endl;
        return 1;
    }

    processFile(argv[1]);
    return 0;
}
