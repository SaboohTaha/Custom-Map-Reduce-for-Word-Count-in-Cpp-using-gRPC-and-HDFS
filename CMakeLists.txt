# Minimum CMake required
cmake_minimum_required(VERSION 3.15)

# Project
project(stringreverse)

# Protobuf
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

# Protobuf-compiler
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)

# gRPC
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
set(_GRPC_GRPCPP gRPC::grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

# Proto file
get_filename_component(hw_proto "masterslave.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)



# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/masterslave.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/masterslave.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/masterslave.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/masterslave.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

set(ENV{CLASSPATH} "$ENV{CLASSPATH}:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*:${HADOOP_HOME}/share/hadoop/yarn/*:${HADOOP_HOME}/share/hadoop/yarn/lib/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/mapreduce/lib/*")
	set(HDFS_LIBRARY "/home/sabooh/hadoop-3.3.5/lib/native")
	find_library(HDFS_LIBRARY NAMES hdfs)
	set(HADOOP_INCLUDE_DIRS "/home/sabooh/hadoop-3.3.5/include")
	set(HADOOP_LIBRARY_DIRS "/home/sabooh/hadoop-3.3.5/lib")
	set(HADOOP_LIBRARIES "hdfs")
	link_directories("/home/sabooh/hadoop-3.3.5/lib/native")
	include_directories(${HADOOP_INCLUDE_DIRS})
	link_directories(${HADOOP_LIBRARY_DIRS})
	message(STATUS "Hadoop 3.3.5 is linked")
# Targets (client|server)
foreach(_target
  master slave)
  add_executable(${_target} "${_target}.cc"
    ${hw_proto_srcs}
    ${hw_grpc_srcs})
  target_link_libraries(${_target}
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    ${HADOOP_LIBRARIES})
endforeach()