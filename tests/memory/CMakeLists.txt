add_executable( promise-memory promise-memory.cpp )
target_link_libraries( promise-memory cppunit libsodium )
add_test( promise_memory promise-memory )

add_executable( release-sink-machinery release-sink-machinery.cpp )
target_link_libraries( release-sink-machinery cppunit libsodium )
add_test( release_sink_machinery release-sink-machinery )

add_executable( switch-memory switch-memory.cpp )
target_link_libraries( switch-memory cppunit libsodium )
add_test( switch_memory switch-memory )

