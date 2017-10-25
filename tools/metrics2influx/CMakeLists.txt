add_executable(metrics2influx metrics2influx.cpp influx_sink.h influx_sink.cpp)
target_link_libraries(metrics2influx ${CSI_LIBS})
