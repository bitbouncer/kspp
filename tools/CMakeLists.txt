add_executable(avrogencpp0_ex avrogencpp0_ex.cpp)
target_link_libraries(avrogencpp0_ex ${CSI_LIBS})

add_executable(avrogencpp1_ex avrogencpp1_ex.cpp)
target_link_libraries(avrogencpp1_ex ${CSI_LIBS})

add_executable(avrogencpp2_ex avrogencpp2_ex.cpp)
target_link_libraries(avrogencpp2_ex ${CSI_LIBS})

INSTALL(TARGETS avrogencpp0_ex  avrogencpp1_ex avrogencpp2_ex
        RUNTIME DESTINATION bin
        )
