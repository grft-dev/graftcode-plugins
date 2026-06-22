include(FetchContent)

add_executable(NatsPluginTests
    "NatsConfigTests.cpp"
    "NatsPluginTests.cpp"
    "${CMAKE_SOURCE_DIR}/NatsPlugin/NatsClient.cpp"
    "${CMAKE_SOURCE_DIR}/NatsPlugin/TransportNats.cpp"
    "${CMAKE_SOURCE_DIR}/NatsPlugin/NatsServer.cpp"
)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.17.0
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

target_include_directories(NatsPluginTests PUBLIC
    "${CMAKE_SOURCE_DIR}/NatsPlugin"
    "${CMAKE_SOURCE_DIR}/GraftcodePluginsInterfaces"
)

target_link_libraries(NatsPluginTests PRIVATE gtest_main nlohmann_json::nlohmann_json)

if(WIN32 OR CMAKE_SYSTEM_NAME STREQUAL "Windows")
    target_link_libraries(NatsPluginTests PRIVATE ws2_32)
endif()

if(CMAKE_DL_LIBS)
    target_link_libraries(NatsPluginTests PRIVATE ${CMAKE_DL_LIBS})
endif()

include(GoogleTest)
gtest_discover_tests(NatsPluginTests)
