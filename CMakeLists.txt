cmake_minimum_required( VERSION 2.8 )

set( CMAKE_INSTALL_PREFIX /usr/local )

project(sodium)
include(CheckCXXCompilerFlag)

check_cxx_compiler_flag("-std=c++11" SUPPORTS_CXX11)
if( SUPPORTS_CXX11 )
    set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    check_cxx_compiler_flag("-std=c++0x" SUPPORTS_CXX0X)
    if( SUPPORTS_CXX0X )
        set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
    else()
        message("C++11 is not supported")
        add_definitions(-DSODIUM_NO_CXX11)
    endif()
endif()

set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wno-deprecated-declarations" )

set( SODIUM_BASE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/sodium )

set( CMAKE_INCLUDE_CURRENT_DIR ON )

find_package( Boost REQUIRED )

set(SODIUM_INCLUDE_FILES
    ${SODIUM_BASE_DIR}/config.h
    ${SODIUM_BASE_DIR}/count_set.h
    ${SODIUM_BASE_DIR}/light_ptr.h
    ${SODIUM_BASE_DIR}/lock_pool.h
    ${SODIUM_BASE_DIR}/promise.h
    ${SODIUM_BASE_DIR}/router.h
    ${SODIUM_BASE_DIR}/sodium.h
    ${SODIUM_BASE_DIR}/time.h
    ${SODIUM_BASE_DIR}/transaction.h
    ${SODIUM_BASE_DIR}/unit.h
    )

set(SODIUM_SOURCE_FILES
    ${SODIUM_BASE_DIR}/light_ptr.cpp
    ${SODIUM_BASE_DIR}/lock_pool.cpp
    ${SODIUM_BASE_DIR}/time.cpp
    ${SODIUM_BASE_DIR}/transaction.cpp
    )

file(
    COPY ${SODIUM_INCLUDE_FILES} 
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/include/sodium
    )

include_directories( ${CMAKE_SOURCE_DIR} ${Boost_INCLUDE_DIR} )

set( ALL_SOURCES ${SODIUM_INCLUDE_FILES} ${SODIUM_SOURCE_FILES} )

add_library( sodium SHARED ${ALL_SOURCES} )
add_library( libsodium STATIC ${ALL_SOURCES} )

set_target_properties( libsodium PROPERTIES OUTPUT_NAME sodium )

# Testing

enable_testing()
add_subdirectory(tests)

# INSTALL
# -------

install (
    TARGETS sodium libsodium
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    )

# Install header files
install (
    FILES ${SODIUM_INCLUDE_FILES}
    DESTINATION include/sodium
    )
