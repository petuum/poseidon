// Author: Jiesi Zhao (jiesizhao0423@gmail.com), Wei Dai (wdai@cs.cmu.edu)
// Date: 2014.11.5

#pragma once

#include <vector>
#include <cstdint>
#include <glog/logging.h>
#include <ml/include/ml.hpp>
#include <utility>

namespace tree {

// TreeNode represents both internal and leaf nodes.
class TreeNode {
public:
  TreeNode() : is_leaf_(false) { }

  // This creates two children. Left child are instances <= split_val on
  // feature_id.
  void Split(int32_t feature_id, float split_val) {
    CHECK(!is_leaf_) << "Leaf node cannot be split";
    left_child_.reset(new TreeNode());
    right_child_.reset(new TreeNode());
    split_val_ = split_val;
    feature_id_ = feature_id;
  }

  TreeNode* GetLeftChild() const {
    return left_child_.get();
  }

  TreeNode* GetRightChild() const {
    return right_child_.get();
  }

  int32_t GetFeatureId() const {
    return feature_id_;
  }

  float GetSplitVal() const {
    return split_val_;
  }

  int32_t GetLeafVal() const {
    return leaf_val_;
  }

  // Declare this node is a leaf node.
  void SetLeafVal(int32_t leaf_val) {
    CHECK(!left_child_ && !right_child_)
      << "I have children, can't be leaf node!";
    is_leaf_ = true;
    leaf_val_ = leaf_val;
    feature_id_ = -1;
    split_val_ = -1;
  }

  // Predict instance recusrively and return leaf node value.
  int32_t Predict(const petuum::ml::AbstractFeature<float>& x) const {
    if (is_leaf_) {
      return leaf_val_;
    }
   
    if (x[feature_id_] <= split_val_) {
      return left_child_->Predict(x);
    }
    return right_child_->Predict(x);
  }

private:
  int32_t feature_id_;  // feature_id associated with this node.
  float split_val_;
  bool is_leaf_;
  int32_t leaf_val_;

  // We take ownership of children_.
  std::unique_ptr<TreeNode> left_child_;
  std::unique_ptr<TreeNode> right_child_;
};

}  // namespace tree
