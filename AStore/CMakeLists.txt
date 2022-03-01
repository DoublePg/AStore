cmake_minimum_required(VERSION 3.2)
project(xxx)
ADD_DEFINITIONS(-std=c++17)
add_definitions(-DBOOST_ERROR_CODE_HEADER_ONLY)
add_definitions(-DBOOST_SYSTEM_NO_DEPRECATED)
include_directories("deps")
include_directories("src/core")
include_directories("deps/boost/include")
include_directories(/deps/jemalloc/include)

set(CMAKE_CXX_FLAGS "-O0 -g -mrtm -march=native")
#add_compile_options( -Wall -Wextra)
#set(CMAKE_CXX_FLAGS "-O3 -march=native -Wall -Wextra")
find_library(boost_system_lib NAMES boost_system PATHS ./deps/boost PATH_SUFFIXES lib
	               NO_DEFAULT_PATH)

	       if(NOT boost_system_lib)

		       	set(boost_system_lib "")

		endif()


		find_library(boost_coroutine_lib NAMES boost_coroutine PATHS ./deps/boost PATH_SUFFIXES lib
			               NO_DEFAULT_PATH)

			       if(NOT boost_coroutine_lib)

				       	set(boost_coroutine_lib "")

				endif()
file(GLOB ycsb_server_SORUCES ""  "./src/bench/server.cc"  "./deps/r2/src/logging.cc"  "./benchs/terminate.cc" )
add_executable(ycsb_server ${ycsb_server_SORUCES} )

find_library(mkl_rt_lib NAMES mkl_rt PATHS /opt/intel/mkl PATH_SUFFIXES lib/intel64
               NO_DEFAULT_PATH)

if(NOT mkl_rt_lib)

	set(mkl_rt_lib "")

endif()

target_link_libraries(ycsb_server ${boost_system_lib} ${boost_coroutine_lib} ibverbs pthread ${mkl_rt_lib} )
file(GLOB ycsb_client_SORUCES ""  "./src/bench/Client.cc"  "./deps/r2/src/logging.cc"  "./deps/r2/src/sshed.cc"  "./src/bench/terminate.cc" )
add_executable(ycsb_client ${ycsb_client_SORUCES} )
target_link_libraries(ycsb_client  ${boost_system_lib} ${boost_coroutine_lib} ibverbs pthread ${mkl_rt_lib} )
