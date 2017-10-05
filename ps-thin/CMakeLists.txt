cmake_minimum_required(VERSION 2.8)
project(Bosen)
add_compile_options(-std=c++11 -fPIC -Wall -Wno-sign-compare)
if(PS_STATS)
    add_compile_options(-DPETUUM_STATS)
endif()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules)
MESSAGE(STATUS "CMAKE_MODULE_PATH:" ${CMAKE_MODULE_PATH})

add_subdirectory(third_party)
add_subdirectory(src)
add_subdirectory(tests)

#MESSAGE(STATUS "PS_SOURCE_FILES:" ${Bosen_SOURCE_FILES})
#MESSAGE(STATUS "Bosen_INCLUDE_DIRS: " ${Bosen_INCLUDE_DIRS})
#MESSAGE(STATUS "PS_LIBRARIES:" ${Bosen_LIBRARIES})

# ps library
add_library(petuum_ps ${Bosen_SOURCE_FILES})
target_include_directories(petuum_ps PUBLIC ${Bosen_INCLUDE_DIRS} ${third_INCLUDE_DIRS})
target_link_libraries(petuum_ps PUBLIC ${Bosen_LIBRARIES})

# ps tests
add_executable(ps_tests ${Bosen_TEST_SOURCE_FILES})
target_include_directories(ps_tests PUBLIC ${Bosen_TEST_INCLUDE_DIRS} ${Bosen_INCLUDE_DIRS})
target_link_libraries(ps_tests ${Bosen_TEST_LIBRARIES} ${Bosen_LIBRARIES})
#add_dependencies(ps_tests petuum_ps)

# clang based format
add_custom_target(ps-clangformat
    COMMAND /usr/bin/clang-format-3.5
    -style=LLVM
    -i
    ${Bosen_SOURCE_FILES})

# set headers for parents
set(PS_INCLUDE_DIRS ${PS_INCLUDE_DIRS} ${Bosen_INCLUDE_DIRS} ${third_INCLUDE_DIRS} PARENT_SCOPE)

#add_subdirectory(app/dml)
#add_subdirectory(app/dnn)
#add_subdirectory(app/dnn_speech)
#add_subdirectory(app/kmeans)
#add_subdirectory(app/mlr)
#add_subdirectory(app/NMF)
#add_subdirectory(app/rand_forest)
#add_subdirectory(app/sparsecoding)
#add_subdirectory(app/demo)
