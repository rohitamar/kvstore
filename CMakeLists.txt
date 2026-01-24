cmake_minimum_required(VERSION 3.14)
project(Rocask)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_definitions(_WIN32_WINNT=0x0601)

include(FetchContent)

FetchContent_Declare(
    asio
    GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
    GIT_TAG        asio-1-30-2  
)
FetchContent_MakeAvailable(asio)

set(ASIO_INCLUDE_DIR ${asio_SOURCE_DIR}/asio/include CACHE PATH "Path to asio" FORCE)
include_directories(${ASIO_INCLUDE_DIR})

FetchContent_Declare(
    Crow
    GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
    GIT_TAG        master
)
FetchContent_MakeAvailable(Crow)


set(SOURCE_FILES 
    main.cpp 
    database/utils.cpp
    database/Rocask.cpp
    api/routes.cpp
)

add_executable(api ${SOURCE_FILES})
target_link_libraries(api PRIVATE Crow::Crow)