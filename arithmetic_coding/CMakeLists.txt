# Set the project name
project(db_compress_arith)

# Glob all source files, named SRC_FILES
file(GLOB_RECURSE SRC_FILES src/*.cpp)

# Create a sources variable with a link to all cpp files to compile
add_library(
        ${PROJECT_NAME}
        ${SRC_FILES}
)

target_include_directories(${PROJECT_NAME}
        PUBLIC
        ${PROJECT_SOURCE_DIR}/include
        )
