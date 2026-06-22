set(target_name NatsPlugin)

add_library(${target_name} SHARED
    "TransportNats.cpp"
    "NatsClient.cpp"
    "NatsServer.cpp"
)

target_include_directories(${target_name} PUBLIC
    "${CMAKE_SOURCE_DIR}/GraftcodePluginsInterfaces"
)

include(FetchContent)

FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.12.0
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(json)

target_link_libraries(${target_name} PUBLIC nlohmann_json::nlohmann_json)

if(WIN32 OR CMAKE_SYSTEM_NAME STREQUAL "Windows")
    target_link_libraries(${target_name} PUBLIC ws2_32)
endif()
