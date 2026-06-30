# Azure Service Bus plugin tests

include(FetchContent)

add_executable(ServiceBusPluginTests
	"ServiceBusTransportTests.cpp"
	"${CMAKE_SOURCE_DIR}/ServiceBusPlugin/TransportServiceBus.cpp"
	"${CMAKE_SOURCE_DIR}/ServiceBusPlugin/ServiceBusClient.cpp"
	"${CMAKE_SOURCE_DIR}/ServiceBusPlugin/ServiceBusServer.cpp"
)

FetchContent_Declare(
	googletest
	GIT_REPOSITORY https://github.com/google/googletest.git
	GIT_TAG v1.17.0
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

find_package(azure-core-amqp-cpp CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

target_include_directories(ServiceBusPluginTests PUBLIC
	"${CMAKE_SOURCE_DIR}/ServiceBusPlugin"
	"${CMAKE_SOURCE_DIR}/GraftcodePluginsInterfaces"
)
target_compile_definitions(ServiceBusPluginTests PRIVATE ENABLE_UAMQP=1)

target_link_libraries(ServiceBusPluginTests PRIVATE
	gtest_main
	Azure::azure-core-amqp
	nlohmann_json::nlohmann_json
)

if(CMAKE_DL_LIBS)
	target_link_libraries(ServiceBusPluginTests PRIVATE ${CMAKE_DL_LIBS})
endif()

include(GoogleTest)
gtest_discover_tests(ServiceBusPluginTests)
