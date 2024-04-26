cmake_minimum_required(VERSION 3.0)
project(CLIENT_MEMHUB)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

set(CLIENT_MEMHUB_CXX_SOURCES 
    src/client_api.cpp  include/client_api.h  
    src/client.cpp include/client.h 
    include/util.h
    ../local_communication/src/ipc_queue.cpp ../local_communication/include/ipc_queue.h  
    ../local_communication/src/ipc_shm.cpp  ../local_communication/include/ipc_shm.h
    ../local_communication/include/msg_info.h ../local_communication/include/util.h
)

add_library(CLIENT_MEMHUB SHARED "${CLIENT_MEMHUB_CXX_SOURCES}")

