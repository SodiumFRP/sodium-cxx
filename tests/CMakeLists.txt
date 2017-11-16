add_subdirectory(memory)

add_executable( test_sodium test_sodium.cpp )
target_link_libraries( test_sodium cppunit libsodium )
add_test( test_sodium test_sodium )

add_executable( test_time test_time.cpp )
target_link_libraries( test_time cppunit libsodium )
add_test( test_time test_time )

