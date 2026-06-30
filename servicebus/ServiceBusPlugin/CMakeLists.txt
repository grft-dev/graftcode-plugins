# Graftcode Azure Service Bus plugin (AMQP 1.0 via the Azure SDK for C++).
set(target_name ServiceBusPlugin)

add_library(${target_name} SHARED
        "TransportServiceBus.cpp"
        "ServiceBusClient.cpp"
        "ServiceBusServer.cpp"
)

target_include_directories(${target_name} PUBLIC
    "${CMAKE_SOURCE_DIR}/GraftcodePluginsInterfaces"
)

# Both dependencies are provided by vcpkg (see vcpkg.json). Configure with the vcpkg
# toolchain file so find_package can locate them, e.g.:
#   cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=<vcpkg>/scripts/buildsystems/vcpkg.cmake
find_package(azure-core-amqp-cpp CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

# The vcpkg azure-core-amqp-cpp port is built with the uAMQP backend but does not propagate
# the backend selection macro to consumers, so the public uAMQP API (CreateMessageSender,
# MessageSender::Send, etc.) is compiled out unless we define it here.
target_compile_definitions(${target_name} PRIVATE ENABLE_UAMQP=1)

target_link_libraries(${target_name} PUBLIC
    Azure::azure-core-amqp
    nlohmann_json::nlohmann_json
)
