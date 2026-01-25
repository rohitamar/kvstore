#include "routes.hpp"

// Seems like crow::json::wvalue.dump() 
// considers the additional backslashes used with escaping backslash and quotes
// This function fixes this, but not sure if I missed something here.
// (might not be needed)
std::string fix_formatting(const std::string& in) {
    std::string res;
    res.reserve(in.size());

    for(size_t i = 0; i < in.size(); i++) {
        if(i < in.size() - 1) {
            if(in.substr(i, 2) == "\\\\" || in.substr(i, 2) == "\\\"") {
                i++;
            }
        }
        res += in[i];
    }

    return res;
}

// GET /ping 
void handle_ping(
    crow::SimpleApp& app
) {
    CROW_ROUTE(app, "/ping")
    .methods(crow::HTTPMethod::GET)
    ([](const crow::request& req) {
        crow::json::wvalue res;
        res["message"] = "Pinged!";
        crow::response response(res);
        response.set_header("Content-Type", "application/json");

        return response;
    });
}

// PUT /api/insert
void handle_insert(
    crow::SimpleApp& app,
    Rocask& db
) {
    CROW_ROUTE(app, "/api/insert")
    .methods(crow::HTTPMethod::PUT)
    ([&db](const crow::request& req) {
        auto x = crow::json::load(req.body);

        if(!x) {
            return crow::response(400, "Invalid JSON");
        }

        if(!x.has("key") || !x.has("value")) {
            return crow::response(400, "Missing key or value.");
        }

        std::string key = x["key"].s();
        
        crow::json::wvalue raw_value = x["value"];
        std::string value = raw_value.dump();


        db.write<std::string, std::string>(key, value);
        
        // std::cout << fix_formatting(value) << std::endl;
        crow::json::wvalue res;
        res["message"] = "Successfully created object at key: " + key;

        CROW_LOG_INFO << "SET key=" << key << " | value=" << value;
        
        crow::response response(res);
        response.code = 201;

        std::string location = "/api/get/" + key;
        response.set_header("Location", location);
        response.set_header("Content-Type", "application/json");

        return response;
    });
}

// GET /api/get/<key:string>
void handle_get(
    crow::SimpleApp& app,
    Rocask& db
) {
    CROW_ROUTE(app, "/api/get/<string>")
    .methods(crow::HTTPMethod::GET)
    ([&db](std::string key) {
        
        std::string raw_value;

        try {
            raw_value = db.read<std::string, std::string>(key);
        } catch(const std::out_of_range& _) {
            return crow::response(400, "Key not found.");
        } catch(...) {
            return crow::response(500, "Server error. Try again.");
        }

        auto json_value = crow::json::load(raw_value);

        crow::json::wvalue res;
        res["payload"] = std::move(json_value);

        return crow::response(200, res);
    });
}