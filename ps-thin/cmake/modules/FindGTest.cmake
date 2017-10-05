# - Try to find GTEST
#
# The following variables are optionally searched for defaults
#  GTEST_ROOT_DIR:            Base directory where all GTEST components are found
#
# The following are set after configuration is done:
#  GTEST_FOUND
#  GTEST_INCLUDE_DIRS
#  GTEST_LIBRARIES

include(FindPackageHandleStandardArgs)

set(GTEST_ROOT_DIR "" CACHE PATH "Folder contains Gtest")

find_path(GTEST_INCLUDE_DIR gtest/gtest.h PATHS ${GTEST_ROOT_DIR})
find_library(GTEST_LIBRARY gtest)
find_library(GTEST_MAIN_LIBRARY gtest_main)

find_package_handle_standard_args(GTEST DEFAULT_MSG GTEST_INCLUDE_DIR
    GTEST_LIBRARY GTEST_MAIN_LIBRARY)

if (GTEST_FOUND)
    set(GTEST_INCLUDE_DIRS ${GTEST_INCLUDE_DIR})
    set(GTEST_LIBRARIES ${GTEST_LIBRARY} ${GTEST_MAIN_LIBRARY})
endif()
