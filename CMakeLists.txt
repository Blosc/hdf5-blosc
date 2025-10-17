cmake_minimum_required(VERSION 3.10.0)
cmake_policy(SET CMP0074 NEW)
project(blosc_hdf5)
include(ExternalProject)
include(GNUInstallDirs)

# options
option(BUILD_TESTS
    "Build test programs form the blosc filter" ON)

option(BUILD_PLUGIN
    "Build dynamically loadable plugin for HDF5 version > 1.8.11" ON)
if(BUILD_PLUGIN)
    set(PLUGIN_INSTALL_PATH "/usr/local/hdf5/lib/plugin" CACHE PATH
      "Where to install the dynamic HDF5-plugin")
endif(BUILD_PLUGIN)

set(BLOSC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/blosc")
set(BLOSC_INSTALL_DIR "${CMAKE_CURRENT_BINARY_DIR}/blosc")
set(BLOSC_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${BLOSC_INSTALL_DIR})

message("BLOSC_PREFIX='${BLOSC_PREFIX}'")
message("BLOSC_INSTALL_DIR='${BLOSC_INSTALL_DIR}'")
message("BLOSC_CMAKE_ARGS='${BLOSC_CMAKE_ARGS}'")
message("GIT_EXECUTABLE='${GIT_EXECUTABLE}'")

ExternalProject_Add(project_blosc
  PREFIX ${BLOSC_PREFIX}
  GIT_REPOSITORY https://github.com/Blosc/c-blosc.git
  GIT_TAG main
  INSTALL_DIR ${BLOSC_INSTALL_DIR}
  CMAKE_ARGS ${BLOSC_CMAKE_ARGS}
)


# sources
set(SOURCES src/blosc_filter.c)
set(PLUGIN_SOURCES src/blosc_filter.c src/blosc_plugin.c )

# dependencies
if(MSVC AND NOT DEFINED ENV{CONDA_PREFIX})
    # FindHDF5.cmake does not find Windows installations other than conda.
    # Try to use an environment variable instead until the official "find"
    # file can be updated for Windows.
    #
    # Note that you have to set this environment variable by hand.
    file(TO_CMAKE_PATH "$ENV{HDF5_DIR}" HDF5_HINT)
    set(HDF5_DIR ${HDF5_HINT} CACHE STRING "Path to HDF5 CMake config directory.")
    find_package(HDF5 REQUIRED HINTS ${HDF5_DIR})
else(MSVC AND NOT DEFINED ENV{CONDA_PREFIX})
    find_package(HDF5 REQUIRED)
endif(MSVC AND NOT DEFINED ENV{CONDA_PREFIX})

include_directories(${HDF5_INCLUDE_DIRS})


# add blosc libraries
add_library(blosc_shared SHARED IMPORTED)
# Needed on Linux
set_property(TARGET blosc_shared PROPERTY IMPORTED_LOCATION ${BLOSC_INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}blosc${CMAKE_SHARED_LIBRARY_SUFFIX})
# Needed on Windows. You will also need to manually copy blosc/bin/blosc.dll
# to the folder of your executable.
set_property(TARGET blosc_shared PROPERTY IMPORTED_IMPLIB ${BLOSC_INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}blosc${CMAKE_STATIC_LIBRARY_SUFFIX})
# Needed on MacOS
set(CMAKE_BUILD_RPATH ${BLOSC_INSTALL_DIR}/lib)

add_dependencies(blosc_shared project_blosc)
include_directories(${BLOSC_INSTALL_DIR}/include)

add_library(blosc_filter_shared SHARED ${SOURCES})
set_target_properties(
  blosc_filter_shared PROPERTIES OUTPUT_NAME blosc_filter)
target_link_libraries(blosc_filter_shared blosc_shared ${HDF5_LIBRARIES})

if(BUILD_PLUGIN)
    add_library(blosc_plugin_shared SHARED ${PLUGIN_SOURCES})
    set_target_properties(
      blosc_plugin_shared PROPERTIES OUTPUT_NAME H5Zblosc)
    target_link_libraries(blosc_plugin_shared blosc_shared ${HDF5_LIBRARIES})

    install(TARGETS blosc_plugin_shared DESTINATION ${PLUGIN_INSTALL_PATH} COMPONENT HDF5_FILTER_DEV)
endif(BUILD_PLUGIN)

# install
install(FILES src/blosc_filter.h DESTINATION include COMPONENT HDF5_FILTER_DEV)
install(TARGETS blosc_filter_shared DESTINATION lib COMPONENT HDF5_FILTER_DEV)


# test
message("LINK LIBRARIES='blosc_filter_shared ${HDF5_LIBRARIES}'")
if(BUILD_TESTS)
    enable_testing()
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    find_package(Threads REQUIRED)
    set(LIBS ${LIBS} ${CMAKE_THREAD_LIBS_INIT})
    add_executable(example src/example.c)
    add_executable(test_array src/test_array.c)
    add_executable(test_compound src/test_compound.c)
    add_executable(test_strings src/test_strings.c)
    add_executable(test_tiny_chunks src/test_tiny_chunks.c)
    target_link_libraries(example blosc_filter_shared ${HDF5_LIBRARIES} ${LIBS})
    target_link_libraries(test_array blosc_filter_shared ${HDF5_LIBRARIES} ${LIBS})
    target_link_libraries(test_compound blosc_filter_shared ${HDF5_LIBRARIES} ${LIBS})
    target_link_libraries(test_strings blosc_filter_shared ${HDF5_LIBRARIES} ${LIBS})
    target_link_libraries(test_tiny_chunks blosc_filter_shared ${HDF5_LIBRARIES} ${LIBS})
    add_test(NAME example[nelmts=0] COMMAND example)
    add_test(NAME example[nelmts=4] COMMAND example 4)
    add_test(NAME example[nelmts=5] COMMAND example 5)
    add_test(NAME example[nelmts=6] COMMAND example 6)
    add_test(NAME example[nelmts=7] COMMAND example 7)
    add_test(NAME test_array COMMAND test_array)
    add_test(NAME test_compound[le_BLOSC_MAX_TYPESIZE] COMMAND test_compound 255)
    add_test(NAME test_compound[gt_BLOSC_MAX_TYPESIZE] COMMAND test_compound 256)
    add_test(NAME test_strings COMMAND test_strings)
    add_test(NAME test_tiny_chunks COMMAND test_tiny_chunks)
endif(BUILD_TESTS)
