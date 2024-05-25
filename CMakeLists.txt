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

add_subdirectory(rapidjson)

# Change me! Now it is delayed coding version, you can switch to arithmetic coding version by `set(CODING "ARITHMETIC_CODING")`,
# note that this version does not support JSON.
set(CODING "DELAYED_CODING")

IF (CODING STREQUAL "DELAYED_CODING")
    # Delayed Coding Version
    add_subdirectory(delayed_coding)
    add_executable(JSON_blitzcrank ${PROJECT_SOURCE_DIR}/JSON.cpp)
    add_executable(tabular_blitzcrank ${PROJECT_SOURCE_DIR}/tabular.cpp)

    target_link_libraries(JSON_blitzcrank PUBLIC db_compress)
    target_link_libraries(tabular_blitzcrank PUBLIC db_compress)
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
