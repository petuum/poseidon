#include <vector>

#include "caffe/data_layers.hpp"

#ifdef USE_PS_THIN
#include <petuum_ps/include/petuum_ps.hpp>
#else
#include <petuum_ps_common/include/petuum_ps.hpp>
#endif

namespace caffe {

template <typename Dtype>
void BasePrefetchingDataLayer<Dtype>::Forward_gpu(
    const vector<Blob<Dtype>*>& bottom, vector<Blob<Dtype>*>* top) {
  // First, join the thread
  JoinPrefetchThread();
  // Copy the data
  caffe_copy(prefetch_data_.count(), prefetch_data_.cpu_data(),
      (*top)[0]->mutable_gpu_data());
  if (this->output_labels_) {
    caffe_copy(prefetch_label_.count(), prefetch_label_.cpu_data(),
        (*top)[1]->mutable_gpu_data());
  }
  // Start a new prefetch thread
  CreatePrefetchThread();
}

INSTANTIATE_CLASS(BasePrefetchingDataLayer);

}  // namespace caffe
