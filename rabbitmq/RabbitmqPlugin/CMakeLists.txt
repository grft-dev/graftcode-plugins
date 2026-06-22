#- HypertubeNative - common code for all Hypertube Native Libraries.
set(target_name RabbitmqPlugin)

add_library(${target_name} SHARED
        "TransportRabbitMq.cpp"
        "RabbitMqClient.cpp"
		"RabbitMqServer.cpp"
)

target_include_directories(${target_name} PUBLIC 
    "${CMAKE_SOURCE_DIR}/HypertubeNativeInterfaces"
)

include(FetchContent)

FetchContent_Declare(
	json
	GIT_REPOSITORY https://github.com/nlohmann/json.git
	GIT_TAG v3.12.0
	GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(json)

set(AMQP-CPP_BUILD_SHARED OFF CACHE BOOL "Build AMQP-CPP static library" FORCE)
set(AMQP-CPP_LINUX_TCP OFF CACHE BOOL "Disable AMQP-CPP Linux TCP backend" FORCE)

FetchContent_Declare(
	amqpcpp
	GIT_REPOSITORY https://github.com/CopernicaMarketingSoftware/AMQP-CPP.git
	GIT_TAG v4.3.27
	GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(amqpcpp)
if(NOT TARGET amqpcpp)
	message(FATAL_ERROR "AMQP-CPP target was not found")
endif()

target_link_libraries(${target_name} PUBLIC amqpcpp nlohmann_json::nlohmann_json)

if(${OSNAME} MATCHES "Windows")
    target_link_libraries(${target_name} PUBLIC ws2_32)
endif()

