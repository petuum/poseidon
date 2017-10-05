//
// Created by raajay on 10/3/17.
//

#include <gtest/gtest.h>
#include <petuum_ps/stats/TwoDimCounter.hpp>
namespace petuum {
TEST(TestTwoDimCounter, Test1) {

  TwoDimCounter<int32_t, int32_t, int32_t> counter;
  counter.Increment(0, 1, 2);
  counter.Increment(0, 1, 2);
  counter.Increment(0, 2, 4);
  counter.Increment(1, 1, 3);
  EXPECT_EQ(counter.Get(0), 8);
  EXPECT_EQ(counter.GetAll(), 11);
  EXPECT_EQ(counter.Get(0, 1), 4);
  EXPECT_EQ(counter.Get(22), 0);
  EXPECT_EQ(counter.Get(22, 23), 0);
  EXPECT_EQ(counter.Get(1, 23), 0);
  counter.Reset();
  EXPECT_EQ(counter.Get(0), 0);
  EXPECT_EQ(counter.Get(1), 0);
  EXPECT_EQ(counter.GetAll(), 0);
}
}
