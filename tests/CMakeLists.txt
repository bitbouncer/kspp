add_executable(test1 test1.cpp)
target_link_libraries(test1 ${CSI_LIBS})
add_test(NAME test1 COMMAND $<TARGET_FILE:test1>)
