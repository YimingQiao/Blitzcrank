# Set the minimum version of CMake that can be used
# To find the cmake version run
# $ cmake --version
cmake_minimum_required(VERSION 3.20)

# Set the project name
project(blitzcrank)

IF (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING
            "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
ENDIF ()

set(CMAKE_CXX_STANDARD 17)

option(BLITZCRANK_RUST_ENCODER "Experimental bit-compatible Rust block encoder (C++ decoder retained)" OFF)
set(BLITZCRANK_DELAYED_CODING_DIR "" CACHE PATH "Local pinned delayed-coding Rust source checkout")
set(BLITZCRANK_DELAYED_CODING_REVISION "847df1439f0345d652515f19b486fd9bcf5264bd")
option(BLITZCRANK_ALLOW_UNPINNED_DC "Development only: allow a different/modified Rust dependency" OFF)
if(BLITZCRANK_RUST_ENCODER)
    if(NOT EXISTS "${BLITZCRANK_DELAYED_CODING_DIR}/include/delayed_coding.h")
        message(FATAL_ERROR "Set BLITZCRANK_DELAYED_CODING_DIR to a tested delayed-coding checkout; nothing is downloaded")
    endif()
    find_package(Git REQUIRED)
    execute_process(COMMAND "${GIT_EXECUTABLE}" -C "${BLITZCRANK_DELAYED_CODING_DIR}" rev-parse HEAD
        RESULT_VARIABLE dc_git_status OUTPUT_VARIABLE dc_revision OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND "${GIT_EXECUTABLE}" -C "${BLITZCRANK_DELAYED_CODING_DIR}" diff --quiet HEAD --
        src ffi include Cargo.toml Cargo.lock CMakeLists.txt
        RESULT_VARIABLE dc_dirty_status)
    if(NOT dc_git_status EQUAL 0 OR NOT dc_revision STREQUAL BLITZCRANK_DELAYED_CODING_REVISION
        OR NOT dc_dirty_status EQUAL 0)
        if(BLITZCRANK_ALLOW_UNPINNED_DC)
            message(WARNING "Using untested delayed-coding source; this is not the pinned compatibility checkpoint")
        else()
            message(FATAL_ERROR "Expected unmodified delayed-coding ${BLITZCRANK_DELAYED_CODING_REVISION}; use BLITZCRANK_ALLOW_UNPINNED_DC only for development")
        endif()
    endif()
    add_subdirectory("${BLITZCRANK_DELAYED_CODING_DIR}" "${CMAKE_BINARY_DIR}/rust-delayed-coding" EXCLUDE_FROM_ALL)
endif()

add_subdirectory(rapidjson)

# Change me! Now it is delayed coding version, you can switch to arithmetic coding version by `set(CODING "ARITHMETIC_CODING")`,
# note that this version does not support JSON.
set(CODING "DELAYED_CODING")

IF (CODING STREQUAL "DELAYED_CODING")
    # Delayed Coding Version
    add_subdirectory(delayed_coding)
    if(BLITZCRANK_RUST_ENCODER)
        target_compile_definitions(db_compress PUBLIC BLITZCRANK_RUST_ENCODER=1)
        target_link_libraries(db_compress PUBLIC DelayedCoding::delayed_coding)
    endif()
    add_executable(JSON_blitzcrank ${PROJECT_SOURCE_DIR}/JSON.cpp)
    add_executable(tabular_blitzcrank ${PROJECT_SOURCE_DIR}/tabular.cpp)

    target_link_libraries(JSON_blitzcrank PUBLIC db_compress)
    target_link_libraries(tabular_blitzcrank PUBLIC db_compress)
    if(BLITZCRANK_RUST_ENCODER)
        add_executable(verify_record_seeks tests/verify_record_seeks.cpp)
        target_link_libraries(verify_record_seeks PRIVATE db_compress)
    endif()
ELSEIF (CODING STREQUAL "ARITHMETIC_CODING")
    # Arithmetic Coding Version
    add_subdirectory(arithmetic_coding)
    add_executable(tabular_blitzcrank ${PROJECT_SOURCE_DIR}/tabular.cpp)
    target_link_libraries(tabular_blitzcrank PUBLIC db_compress_arith)
ELSE ()
    MESSAGE(Unsupported Coding Method.)
ENDIF ()


# Random Access Test Program
# Plain
add_executable(plain_ra plain_ra.cpp)
target_link_libraries(plain_ra PUBLIC db_compress)
