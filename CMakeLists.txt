cmake_minimum_required(VERSION 3.16)

project(task_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---- Task Engine library ----
add_library(task_engine_lib
    src/job.cpp
    src/job_queue.cpp
    src/logger.cpp
    src/scheduler.cpp
    src/thread_pool.cpp
)

target_include_directories(task_engine_lib
    PUBLIC include
)

# ---- Main demo executable ----
add_executable(task_engine
    apps/main.cpp
)

target_link_libraries(task_engine
    PRIVATE task_engine_lib
)
