#file(GLOB_RECURSE Bosen_SOURCE_FILES *.[ch]pp)
file(GLOB_RECURSE petuum_ps_files "petuum_ps/*.[ch]pp")
file(GLOB_RECURSE petuum_ps_common_files "petuum_ps_common/*.[ch]pp")
set(Bosen_SOURCE_FILES ${petuum_ps_files} ${petuum_ps_common_files} PARENT_SCOPE)

# uncomment to enable HDFS.
set(USE_HDFS 0)

# Required libraries
find_package(Boost 1.54.0 REQUIRED COMPONENTS thread system)
find_package(Eigen3 REQUIRED)
find_package(Gflags REQUIRED)
find_package(Glog 0.3.4 REQUIRED)
find_package(LevelDB REQUIRED)
find_package(Libconfig REQUIRED)
find_package(Snappy REQUIRED)
find_package(sparsehash 2.0.2 REQUIRED)
find_package(Tcmalloc REQUIRED)
find_package(Threads REQUIRED)
find_package(Yaml REQUIRED)
find_package(ZeroMQ 3.2.0 REQUIRED)

set(Bosen_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR})

if(${USE_HDFS})
    find_package(HDFS REQUIRED)
    find_package(JVM REQUIRED)
    add_definitions(-DHAS_HADOOP)
    set(Bosen_INCLUDE_DIRS ${Bosen_INCLUDE_DIRS} ${HDFS_INCLUDE_DIRS})
    set(Bosen_LIBRARIES ${Bosen_LIBRARIES} ${HDFS_LIBRARY} ${JVM_LIBRARIES})
endif(${USE_HDFS})

#MESSAGE(STATUS "Third Party Dir: ${third_INCLUDE_DIRS}")
#MESSAGE(STATUS "Bosen Include Dir: ${Bosen_INCLUDE_DIRS}")
#MESSAGE(STATUS "ZMQ Test Dir: ${cppzmq_INCLUDE_DIRS}")
#MESSAGE(STATUS "HDFS and JVM: ${HDFS_LIBRARIES}, ${JVM_LIBRARIES}")
MESSAGE(STATUS "MY SNAPPY: ${Snappy_LIBRARIES}, ${Snappy_INCLUDE_DIR}")

set(Bosen_INCLUDE_DIRS ${Bosen_INCLUDE_DIRS}
        ${ZeroMQ_INCLUDE_DIRS}
        ${GLOG_INCLUDE_DIRS}
        ${YAML_INCLUDE_DIRS}
        ${LevelDB_INCLUDE_PATH}
        ${Snappy_INCLUDE_DIR}
        ${SPARSEHASH_INCLUDE_DIR}
        ${GFLAGS_INCLUDE_DIRS}
        ${LIBCONFIG_INCLUDE_DIR}
        ${Tcmalloc_INCLUDE_DIR}
        ${EIGEN3_INCLUDE_DIR} PARENT_SCOPE)

if(UNIX AND NOT APPLE)
    list(APPEND UNIX_ONLY_LIBS rt nsl)
else()
    list(APPEND UNIX_ONLY_LIBS "")
endif()

set(Bosen_LIBRARIES ${Bosen_LIBRARIES} ${UNIX_ONLY_LIBS}
        ${ZeroMQ_LIBRARIES}
        ${Boost_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
        ${GLOG_LIBRARIES}
        ${Tcmalloc_LIBRARIES}
        ${LIBCONFIG_LIBRARIES}
        ${LevelDB_LIBRARY}
        ${Snappy_LIBRARIES}
        ${Boost_LIBRARIES}
        ${YAML_LIBRARIES}
        ${GFLAGS_LIBRARIES} PARENT_SCOPE)
