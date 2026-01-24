#pragma once 
#include "crow.h"
#include "../database/Rocask.hpp"

#include <string>

void handle_ping(crow::SimpleApp& app);

void handle_insert(crow::SimpleApp& app, Rocask& db);
void handle_get(crow::SimpleApp& app, Rocask& db);