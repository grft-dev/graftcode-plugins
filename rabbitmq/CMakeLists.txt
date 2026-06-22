set(CMAKE_MIN 3.22)
cmake_minimum_required (VERSION ${CMAKE_MIN})
set(CMAKE_POLICY_VERSION_MINIMUM ${CMAKE_MIN})
cmake_policy(VERSION ${CMAKE_MIN})

project ("RabbitmqPlugin" VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)  
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_definitions(-DUNICODE)
enable_testing()

# Set build type
set (BUILDTYPE ${CMAKE_BUILD_TYPE})
set (CMAKE_POLICY_DEFAULT_CMP0135 NEW)

if(${OSNAME} MATCHES "Windows")
	add_compile_options(/utf-8)
	if(MSVC)
		set(CMAKE_MSVC_RUNTIME_LIBRARY
			"MultiThreaded$<$<CONFIG:Debug>:Debug>"
			CACHE STRING "MSVC runtime library" FORCE)
	endif()
endif()

add_subdirectory ("RabbitmqPlugin")
add_subdirectory ("RabbitmqPluginTest")

