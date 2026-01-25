#include "crow.h"

#include "api/routes.hpp"
#include "api/FileLogHandler.hpp"
#include "database/Rocask.hpp"

int main(int argc, char* argv[])
{
    if(argc < 2) {
        std::cerr << "Need Port" << std::endl;
        return 1;
    }

    std::string str_port = argv[1];
    int port = std::stoi(str_port);

    crow::SimpleApp app;
    Rocask db(port);

    std::string logname = "./logs/api_" + str_port + ".log";
    crow::logger::setHandler(new FileLogHandler(logname));

    handle_ping(app);
    handle_insert(app, db);
    handle_get(app, db);

    app.port(port).multithreaded().run();
}