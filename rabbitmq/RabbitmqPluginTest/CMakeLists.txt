# RabbitMQ plugin tests

include(FetchContent)

add_executable(RabbitMqPluginTests
	"RabbitMqTransportTests.cpp"
	"${CMAKE_SOURCE_DIR}/RabbitmqPlugin/TransportRabbitMq.cpp"
	"${CMAKE_SOURCE_DIR}/RabbitmqPlugin/RabbitMqClient.cpp"
)

FetchContent_Declare(
	googletest
	GIT_REPOSITORY https://github.com/google/googletest.git
	GIT_TAG v1.17.0
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

target_include_directories(RabbitMqPluginTests PUBLIC
	"${CMAKE_SOURCE_DIR}/RabbitmqPlugin"
	"${CMAKE_SOURCE_DIR}/GraftcodePluginsInterfaces"
)
target_link_libraries(RabbitMqPluginTests PRIVATE gtest_main amqpcpp nlohmann_json::nlohmann_json)

if(${OSNAME} MATCHES "Windows")
	target_link_libraries(RabbitMqPluginTests PRIVATE ws2_32)
endif()

if(CMAKE_DL_LIBS)
	target_link_libraries(RabbitMqPluginTests PRIVATE ${CMAKE_DL_LIBS})
endif()

include(GoogleTest)
gtest_discover_tests(RabbitMqPluginTests)

