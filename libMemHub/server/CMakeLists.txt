
cmake_minimum_required(VERSION 3.8)
project(SERVER_MEMHUB)

add_compile_options(-fPIC)
add_compile_options(-pthread -g  -ggdb -O0 -Wall   -fstack-protector-all -march=native -fnon-call-exceptions)

include(./include/remote_communicator/cmake/common.cmake)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

find_library(libconfig config++)

## Proto file
get_filename_component(hw_proto "./include/remote_communicator/protos/DBMF.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/DBMF.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/DBMF.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/DBMF.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/DBMF.grpc.pb.h")
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

# hw_grpc_proto
add_library(hw_grpc_proto
  ${hw_grpc_srcs}
  ${hw_grpc_hdrs}
  ${hw_proto_srcs}
  ${hw_proto_hdrs})
target_link_libraries(hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

## excute target_link

set(SERVER_MEMHUB_CXX_SOURCES 
    src/utils/server_api.cpp include/utils/server_api.h  
    src/utils/cs_port.cpp include/utils/cs_port.h 
    include/utils/util.h

    src/random_sampler/sampler.cpp include/random_sampler/sampler.h include/random_sampler/i_sampler.h
    src/remote_communicator/communicator_backend.cpp include/remote_communicator/communicator_backend.h include/remote_communicator/i_communicator_backend.h
    src/manager/data_manager.cpp include/manager/data_manager.h include/manager/i_data_manager.h 
    src/manager/memory_backend.cpp include/manager/memory_backend.h 
    src/manager/filesystem_backend.cpp include/manager/filesystem_backend.h 

    ../local_communication/src/thread_queue.cpp ../local_communication/include/thread_queue.h 
    ../local_communication/src/ipc_shm.cpp  ../local_communication/include/ipc_shm.h 
    ../local_communication/src/ipc_queue.cpp  ../local_communication/include/ipc_queue.h 
    ../local_communication/src/semaphore.cpp ../local_communication/include/semaphore.h 
    ../local_communication/include/msg_info.h ../local_communication/include/util.h
    ../configure/src/configuration.cpp ../configure/include/configuration.h
    src/remote_communicator/reply_rpc.cpp include/remote_communicator/reply_rpc.h
    src/remote_communicator/request_rpc.cpp include/remote_communicator/request_rpc.h
     
)

add_library(SERVER_MEMHUB SHARED "${SERVER_MEMHUB_CXX_SOURCES}")
target_link_libraries(SERVER_MEMHUB
    hw_grpc_proto
    absl::flags
    absl::flags_parse
    ${libconfig}
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})
