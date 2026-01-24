#include <fstream>

class FileLogHandler : public crow::ILogHandler {
public:
    FileLogHandler(const std::string& filename) {
        log_file.open(filename, std::ios::out | std::ios::trunc);
    }

    void log(const std::string& message, crow::LogLevel) override {
        if (log_file.is_open()) {
            log_file << message << "\n";
            log_file.flush(); 
        }
    }

private:
    std::ofstream log_file;
};