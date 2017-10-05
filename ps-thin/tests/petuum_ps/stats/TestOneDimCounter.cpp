#include <gtest/gtest.h>
#include <petuum_ps/stats/OneDimCounter.hpp>

namespace petuum {
TEST(TestOneDimCounter, Test1) {
    OneDimCounter<int32_t, int32_t> counter;
  counter.Increment(0, 10);
  counter.Increment(1, 2);
  counter.Increment(0, 2);

  EXPECT_EQ(12, counter.Get(0));
  EXPECT_EQ(2, counter.Get(1));
  EXPECT_EQ(14, counter.GetAll());
  EXPECT_EQ(0, counter.Get(3));
  counter.Reset();
  EXPECT_EQ(0, counter.Get(0));
  EXPECT_EQ(0, counter.GetAll());
}
}

